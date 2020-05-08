package sync

//#cgo CFLAGS: -I /usr/local/include
//#cgo LDFLAGS: -L ../common  -Wl,-rpath=/usr/local/lib -lcommon
//
//#include "libcommon.h"
import "C"

import (
	"container/list"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type MafkaSyncer struct {
	toBeAckCommitTSMu      sync.Mutex
	toBeAckCommitTS *MapList
	shutdown chan struct{}
	maxWaitThreshold int64
	tableInfos *TableInformations
	*baseSyncer
}

func NewMafkaSyncer (
	cfg *DBConfig,
	cfgFile string,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync) (dsyncer Syncer, err error) {
	if cfgFile == "" {
		return nil, errors.New("config file name is empty")
	}

	ret := C.InitProducerOnce(C.CString(cfgFile))
	if len(C.GoString(ret)) > 0 {
		return nil, errors.New("init producer error")
	}

	time.Sleep(5 * time.Second)

	executor := &MafkaSyncer{}
	executor.shutdown = make(chan struct{})
	executor.toBeAckCommitTS = NewMapList()
	executor.baseSyncer = newBaseSyncer(tableInfoGetter)
	executor.maxWaitThreshold = int64(C.GetWaitThreshold())

	is, err := NewTableInformations(cfg.Checkpoint.User, cfg.Checkpoint.Password, cfg.Host, cfg.Port)
	if err != nil {
		return nil, err
	}
	executor.tableInfos = is

	log.Info("New MafkaSyncer success")
	go executor.Run()

	return executor, nil
}

func (ms *MafkaSyncer) Sync(item *Item) error {
	txn, err := translator.TiBinlogToTxn(ms.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue, item.ShouldSkip)
	if err != nil {
		return errors.Trace(err)
	}

	cts := item.Binlog.GetCommitTs()
	if txn.DDL != nil {
		sqls := strings.Split(txn.DDL.SQL, ";")
		for _, sql := range sqls {
			m := NewMessage(txn.DDL.Database, txn.DDL.Table, sql, cts, time.Now().Unix())
			if ms.tableInfos.NeedRefreshTableInfo(sql) {
				ms.tableInfos.RefreshToInfos(txn.DDL.Database, txn.DDL.Table)
			}
			data, err := json.Marshal(m)
			if err != nil {
				return err
			}
			log.Info("send to mafka", zap.String("sql", m.Sql), zap.Int64("commit-ts", m.Cts), zap.Int64("applied-ts", m.Ats))
			C.AsyncMessage(C.CString(string(data)), C.long(m.Cts))
		}
	} else {
		for _, dml := range txn.DMLs {
			log.Info("###", zap.String("dml", fmt.Sprintf("%v", dml)))
			i, e := ms.tableInfos.GetFromInfos(dml.Database, dml.Table)
			if e != nil {
				return err
			}
			log.Info("table info", zap.String("info", fmt.Sprintf("%v", i)))
			dml.SetTableInfo(i)
			normal, args := dml.Sql()
			sql, err := GenSQL(normal, args, true, time.Local)
			if err != nil {
				return err
			}
			m := NewMessage(dml.Database, dml.Table, sql, cts, time.Now().Unix())
			data, err := json.Marshal(m)
			if err != nil {
				return err
			}
			log.Info("send to mafka", zap.String("sql", m.Sql), zap.Int64("commit-ts", m.Cts), zap.Int64("applied-ts", m.Ats))
			C.AsyncMessage(C.CString(string(data)), C.long(m.Cts))
		}
	}
	ms.toBeAckCommitTSMu.Lock()
	ms.toBeAckCommitTS.Push(item)
	ms.toBeAckCommitTSMu.Unlock()

	return nil
}

func (ms *MafkaSyncer) Close() error {
	if ms.shutdown != nil {
		close(ms.shutdown)
		ms.shutdown = nil
	}
	return nil
}

func (ms *MafkaSyncer) SetSafeMode(mode bool) bool {
	return false
}

func (ms *MafkaSyncer) Run () {
	var wg sync.WaitGroup
	log.Info("MafkaSyncer Running now")
	// handle successes from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		checkTick := time.NewTicker(time.Second)
		defer checkTick.Stop()
		for {
			select {
			case <-checkTick.C:
				ts := int64(C.GetLatestApplyTime())
				ms.toBeAckCommitTSMu.Lock()
				var next *list.Element
				for elem := ms.toBeAckCommitTS.GetDataList().Front(); elem != nil; elem = next {
					if elem.Value.(Keyer).GetKey() <= ts {
						next = elem.Next()
						ms.success <- elem.Value.(*Item)
						ms.toBeAckCommitTS.Remove(elem.Value.(Keyer))
					} else {
						break
					}
				}

				tss := int64(C.GetLatestSuccessTime())
				cur := time.Now().Unix()
				if ms.toBeAckCommitTS.Size() > 0 && cur != 0 && cur - tss > ms.maxWaitThreshold {
					err := errors.New(fmt.Sprintf("fail to push msg to mafka after %v, check if kafka is up and working", ms.maxWaitThreshold))
					ms.setErr(err)
					log.Warn("fail to push msg to mafka, MafkaSyncer exit")
					close(ms.shutdown)
				}
				ms.toBeAckCommitTSMu.Unlock()
			}
		}
	}()

	for {
		select {
		case <-ms.shutdown:
			wg.Wait()
			log.Info("MafkaSyncer exited")
			C.CloseProducer()
			ms.setErr(nil)
			return
		}
	}
}

func (it *Item) GetKey() int64 {
	return it.Binlog.CommitTs
}

type Message struct {
	database string `json:"database-name"`
	table    string `json:"table-name"`
	Sql      string `json:"sql"`
	Cts      int64  `json:"committed-timestamp"`
	Ats      int64  `json:"applied-timestamp"`
}

func NewMessage(db, tb, sql string, cts, ats int64) *Message {
	return &Message{
		database: db,
		table:    tb,
		Sql:      sql,
		Cts:      cts,
		Ats:      ats,
	}
}
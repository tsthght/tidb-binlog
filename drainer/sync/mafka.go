package sync

//#cgo CFLAGS: -I /usr/local/include
//#cgo LDFLAGS: -L ../common  -Wl,-rpath=/usr/local/lib -lcommon
//
//#include "libcommon.h"
import "C"

import (
	"container/list"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

type MafkaSyncer struct {
	toBeAckCommitTSMu      sync.Mutex
	toBeAckCommitTS *MapList
	shutdown chan struct{}
	*BaseSyncer
}

func NewMafkaSyncer(
	cfg *DBConfig,
	cfgFile string,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
	enableDispatch bool,
	enableCausility bool) (dsyncer Syncer, err error) {
	if cfgFile == "" {
		return nil, errors.New("config file name is empty")
	}

	ret := C.InitProducerOnce(C.CString(cfgFile))
	if len(C.GoString(ret)) == 0 {
		return nil, errors.New("init producer error")
	}

	time.Sleep(5 * time.Second)
	executor := &MafkaSyncer{}
	executor.shutdown = make(chan struct{})
	executor.toBeAckCommitTS = NewMapList()
	executor.Run()

	return executor, nil
}

func (ms *MafkaSyncer) Sync(item *Item) error {
	slaveBinlog, err := translator.TiBinlogToSlaveBinlog(ms.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(slaveBinlog)
	if err != nil {
		return err
	}
	C.AsyncMessage(C.CString(string(data)), C.long(item.Binlog.CommitTs))
	ms.toBeAckCommitTSMu.Lock()
	ms.toBeAckCommitTS.Push(item)
	ms.toBeAckCommitTSMu.Unlock()
	return nil
}

func (ms *MafkaSyncer) Close() error {
	return nil
}

func (ms *MafkaSyncer) SetSafeMode(mode bool) bool {
	return false
}

func (ms *MafkaSyncer) Run () {
	var wg sync.WaitGroup

	// handle successes from producer
	wg.Add(1)
	go func() {
		defer wg.Done()

		ts := int64(C.GetLatestApplyTime())
		ms.toBeAckCommitTSMu.Lock()
		var next *list.Element
		for elem := ms.toBeAckCommitTS.dataList.Front(); elem != nil; elem = next {
			if elem.Value.(Keyer).GetKey() <= ts {
				next = elem.Next()
				ms.success <- elem.Value.(*Item)
				ms.toBeAckCommitTS.Remove(elem.Value.(Keyer))
			} else {
				break
			}
		}
		ms.toBeAckCommitTSMu.Unlock()

		time.Sleep(1 * time.Second)
	}()

	for {
		select {
		case <-ms.shutdown:
			//ds.asynProducer.Close()
			ms.SetErr(nil)

			wg.Wait()
			return
		}
	}
}

func (it *Item) GetKey() int64 {
	return it.Binlog.CommitTs
}
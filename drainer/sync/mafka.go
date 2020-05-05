package sync

//#cgo CFLAGS: -I /usr/local/include
//#cgo LDFLAGS: -L ../common  -Wl,-rpath=/usr/local/lib -lcommon
//
//#include "libcommon.h"
import "C"

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

type MafkaSyncer struct {
	//toBeAckCommitTS orderlist.MapList
	configFile      string
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

	executor := &MafkaSyncer{}

	ret := C.InitProducerOnce(C.CString(cfgFile))
	if len(C.GoString(ret)) == 0 {
		return nil, errors.New("init producer error")
	}

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
	C.AsyncMessage(C.CString(data), C.long(slaveBinlog.CommitTs))
	return nil
}

func (ms *MafkaSyncer) Close() error {
	return nil
}

func (ms *MafkaSyncer) SetSafeMode(mode bool) bool {
	return false
}
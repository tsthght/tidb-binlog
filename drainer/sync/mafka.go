package sync

import (
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

	return executor, nil
}

func (ms *MafkaSyncer) Sync(item *Item) error {
	return nil
}

func (ms *MafkaSyncer) Close() error {
	return nil
}

func (ms *MafkaSyncer) SetSafeMode(mode bool) bool {
	return false
}
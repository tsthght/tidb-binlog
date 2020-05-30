package sync

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type SyncerDemo struct {
	*baseSyncer
}

func (sd *SyncerDemo) Sync(item *Item) error {
	//demo
	log.Info("item", zap.String("%s", fmt.Sprintf("%v", item)))
	sd.success <- item
	return nil
}

func (sd *SyncerDemo) Close() error {
	return nil
}

func (sd *SyncerDemo) SetSafeMode(mode bool) bool {
	return false
}

func NewSyncerDemo (
	cfg *DBConfig,
	file string,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
) (Syncer, error) {
		log.Info("call NewSyncerDemo()")
		return &SyncerDemo{}, nil
}
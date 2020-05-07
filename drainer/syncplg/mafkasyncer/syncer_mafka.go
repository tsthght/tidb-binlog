package main

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/sync"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

//SyncerMafka is a syncer for Mafka

//NewSyncerPlugin return Asyncer instance which implemented interface of sync.Syncer
func NewSyncerPlugin (
	cfg *sync.DBConfig,
	cfgFile string,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync) (dsyncer sync.Syncer, err error) {
	return sync.NewMafkaSyncer(cfg, cfgFile, tableInfoGetter, worker, batchSize, queryHistogramVec, sqlMode,
		destDBType, relayer, info)
}

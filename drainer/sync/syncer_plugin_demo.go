package sync

import (
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/prometheus/client_golang/prometheus"
)

//DemoSyncer is a syncer demo
type DemoSyncer struct {
	*BaseSyncer
}

func NewPluginSyncer(
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
	return &DemoSyncer{}, nil
}

//Sync should be implemented
func (ds *DemoSyncer) Sync(item *Item) error {
	return nil
}

//Close should be implemented
func (ds *DemoSyncer) Close() error {
	return nil
}

// SetSafeMode should be implement if necessary
func (p *DemoSyncer) SetSafeMode(mode bool) bool {
	return false
}

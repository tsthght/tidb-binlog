package sync

import (
	"encoding/json"
	"flag"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/s3common/s3castleclient"
	"github.com/pingcap/tidb-binlog/drainer/s3common/s3mafkaclient"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type MetaInfo struct {
	size int
	item *Item
}

//DemoSyncer is a syncer demo
type MafkaSyncer struct {
	cfg *MafkaConfig
	asynProducer *s3mafkaclient.MafkaAsynProducer

	callBack MafkaCallBack

	shutdown chan struct{}

	toBeAckCommitTSMu      sync.Mutex
	toBeAckCommitTS        map[int64]*MetaInfo
	toBeAckTotalSize       int
	resumeProduce          chan struct{}
	resumeProduceCloseOnce sync.Once
	lastSuccessTime time.Time

	*baseSyncer
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
	//parse config
	mcfg := NewMafkaConfig()
	err = mcfg.Parse(cfgFile)
	if err != nil {
		return nil, err
	}
	//callback
	callback := MafkaCallBack{}
	//asynproducer
	castleCli, err := NewCastleClient(mcfg)
	if err != nil {
		return nil, err
	}
	producer, err := NewAsynProducer(castleCli, mcfg, callback)
	if err != nil {
		return nil, err
	}
	producer.RetryTimes = mcfg.MaxRetryTimes
	producer.AsyncBufferChanSize = mcfg.MaxAsyncBufferChanSize

	executor := &MafkaSyncer {
		cfg: mcfg,
		asynProducer: producer,
		shutdown:        make(chan struct{}),
		baseSyncer:      newBaseSyncer(tableInfoGetter),
		callBack:callback,
		toBeAckCommitTS: make(map[int64]*MetaInfo),
		toBeAckTotalSize: 0,
	}
	go executor.run()

	return executor, nil
}

//Sync should be implemented
func (ds *MafkaSyncer) Sync(item *Item) error {
	slaveBinlog, err := translator.TiBinlogToSlaveBinlog(ds.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	if err != nil {
		return errors.Trace(err)
	}
	//handle
	msg := NewMafkaMessage()
	msg.CommitTs = slaveBinlog.GetCommitTs()
	msg.data, err = slaveBinlog.Marshal()
	if err != nil {
		return err
	}
	str, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := ds.asynProducer.SendMessageToChan(str); err != nil {
		return errors.Trace(err)
	}

	waitResume := false

	ds.toBeAckCommitTSMu.Lock()
	if len(ds.toBeAckCommitTS) == 0 {
		ds.lastSuccessTime = time.Now()
	}
	ds.toBeAckCommitTS[msg.CommitTs] = &MetaInfo{len(str), item}
	ds.toBeAckTotalSize += len(str)
	if ds.toBeAckTotalSize >= ds.cfg.StallThreshold && len(ds.toBeAckCommitTS) > 1 {
		ds.resumeProduce = make(chan struct{})
		ds.resumeProduceCloseOnce = sync.Once{}
		waitResume = true
	}
	ds.toBeAckCommitTSMu.Unlock()

	if waitResume {
		select {
		case <-ds.resumeProduce:
		case <-ds.errCh:
			return errors.Trace(ds.err)
		}
	}
	return nil
}

//Close should be implemented
func (ds *MafkaSyncer) Close() error {
	close(ds.shutdown)
	err := <-ds.Error()
	return err
}

// SetSafeMode should be implement if necessary
func (ds *MafkaSyncer) SetSafeMode(mode bool) bool {
	return false
}

func (ds *MafkaSyncer) run()  {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			    case msgs := <- ds.callBack.SuccessChan:
					for _, msg := range msgs.([]interface{}) {
						var m MafkaMessage
						json.Unmarshal(msg.([]byte), &m)
						commitTs := m.CommitTs

						ds.toBeAckCommitTSMu.Lock()
						ds.lastSuccessTime = time.Now()
						meta := ds.toBeAckCommitTS[commitTs]
						ds.toBeAckTotalSize -= meta.size
						if ds.toBeAckTotalSize < ds.cfg.StallThreshold && ds.resumeProduce != nil {
							ds.resumeProduceCloseOnce.Do(func() {
								close(ds.resumeProduce)
							})
						}
						ds.success <- meta.item
						delete(ds.toBeAckCommitTS, commitTs)
						ds.toBeAckCommitTSMu.Unlock()
					}
			}
		}
		close(ds.success)
	}()

	// handle errors from producer
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case err := <- ds.callBack.FailureChan:
				log.Fatal("fail to produce message to kafka, please check the state of kafka server", zap.Error(err.(error)))
			}
		}
	}()

	checkTick := time.NewTicker(time.Second)
	defer checkTick.Stop()

	for {
		select {
		case <-checkTick.C:
			ds.toBeAckCommitTSMu.Lock()
			if len(ds.toBeAckCommitTS) > 0 && time.Since(ds.lastSuccessTime) > time.Duration(ds.cfg.WaitThreshold) {
				err := errors.Errorf("fail to push msg to kafka after %v, check if kafka is up and working", ds.cfg.WaitThreshold)
				ds.setErr(err)
				ds.toBeAckCommitTSMu.Unlock()
				return
			}
			ds.toBeAckCommitTSMu.Unlock()
		case <-ds.shutdown:
			ds.asynProducer.Close()
			ds.setErr(nil)

			wg.Wait()
			return
		}
	}
}

// MafkaConfig is the Mafka Syncer's configuration.
type MafkaConfig struct {
	fs                         *flag.FlagSet
	Topic                      string      `toml:"topic" json:"topic"`
	Group                      string      `toml:"group" json:"group"`
	BusinessAppkey             string      `toml:"business-appkey" json:"business-appkey"`
	CastleAppkey               string      `toml:"castle-appkey" json:"castle-appkey"`
	CastleName                 string      `toml:"castle-name" json:"castle-name"`
	StallThreshold             int         `toml:"stall-threshold" json:"stall-threshold"`
	WaitThreshold              int64       `toml:"wait-threshold" json:"wait-threshold"`
	MaxRetryTimes              int         `toml:"max-retry-times" json:"max-retry-times"`
	MaxAsyncBufferChanSize     int64       `toml:"max-chan-size" json:"max-chan-size"`

	LocalHost                  string
}

func NewMafkaConfig() *MafkaConfig {
	cfg := &MafkaConfig{}
	fs := flag.NewFlagSet("mafka-syncer", flag.ContinueOnError)
	fs.StringVar(&cfg.Topic, "topic", "", "mafka 's topic")
	fs.StringVar(&cfg.Group, "group", "", "mafka 's group")
	fs.StringVar(&cfg.BusinessAppkey, "business-appkey", "", "business appkey")
	fs.StringVar(&cfg.CastleAppkey, "castle-appkey", "", "castle appkey")
	fs.StringVar(&cfg.CastleName, "castle-name", "", "castle name")
	fs.IntVar(&cfg.StallThreshold, "stall-threshold", 90 * 1024 * 1024, "stall threshold")
	fs.Int64Var(&cfg.WaitThreshold, "wait-threshold", 30000, "wait threshold (ms)")
	fs.IntVar(&cfg.MaxRetryTimes, "max-retry-times", 10000, "max retry times")
	fs.Int64Var(&cfg.MaxAsyncBufferChanSize, "max-chan-size", 1 << 30, "max async buffer chan size")

	cfg.fs = fs
	return cfg
}

func (cfg *MafkaConfig) configFromFile(path string) error {
	return util.StrictDecodeFile(path, "mafka", cfg)
}

// Parse parses all config from configuration file
func (cfg *MafkaConfig) Parse(filename string) error {
	// load config file if specified
	if filename == "" {
		return errors.New("config file name is nil")
	}
	if err := cfg.configFromFile(filename); err != nil {
		return err
	}
	return nil
}

func GetLocalIP() (error, string) {
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		return err, ""
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return nil, ipnet.IP.String()
			}
		}
	}
	return errors.New("can not get local ip"), ""
}

func GetLocalHost() (error, string) {
	host, err := os.Hostname()
	if err != nil {
		return err, ""
	}
	return nil, host
}

func NewCastleClient(cfg *MafkaConfig) (castleManager *s3castleclient.CastleClientManager, err error) {
	var ip, host string
	err, ip = GetLocalIP()
	if err != nil {
		return nil, err
	}
	err, host = GetLocalHost()
	if err != nil {
		return nil, err
	}
	if castleManager, err = s3castleclient.NewCastleClientManager(host, ip,
		cfg.CastleAppkey, cfg.CastleName, cfg.BusinessAppkey); err != nil {
		return nil, err
	}
	return
}

func NewAsynProducer(castleManager *s3castleclient.CastleClientManager, cfg *MafkaConfig,
	callback s3mafkaclient.SenderCallback) (asynProducer *s3mafkaclient.MafkaAsynProducer, err error) {
	if castleManager == nil {
		return nil, s3mafkaclient.ErrProducerInvalidConfig
	}

	if asynProducer, err = s3mafkaclient.NewMafkaAsynProducer(cfg.Topic, castleManager, nil, callback); err != nil {
		return nil, err
	} else {
		return asynProducer, nil
	}
}

type MafkaCallBack struct{
	SuccessChan chan interface{}
	FailureChan chan interface{}
}

func (m MafkaCallBack) OnSuccess(msgs []interface{}) {
	m.SuccessChan <- msgs
}

func (m MafkaCallBack) OnFailure(msgs []interface{}, err error) {
	m.FailureChan <- err
}

type MafkaMessage struct {
	currentTime int64
	CommitTs int64
	data []byte
}

func NewMafkaMessage() *MafkaMessage {
	msg := &MafkaMessage{}
	msg.currentTime = time.Now().UnixNano()
	return msg
}
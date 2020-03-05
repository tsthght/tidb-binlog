package main

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

type PluginDemo struct{}

func (pd PluginDemo) ExtendTxn(tx *loader.Tx, dmls []*loader.DML, info *loopbacksync.LoopBackSync) (*loader.Tx, []*loader.DML) {
	//do sth
	log.Info("i am ExtendTxn")
	return nil, nil
}

func (pd PluginDemo) FilterTxn(txn *loader.Txn, info *loopbacksync.LoopBackSync) (bool, error) {
	//do sth
	log.Info("i am FilterTxn")
	return true, nil
}

func NewPlugin() interface{} {
	return PluginDemo{}
}

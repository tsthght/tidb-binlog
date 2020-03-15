package main

import (
	gosql "database/sql"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

//PluginDemo is a demo struct
type PluginDemo struct{}

//ExtendTxn is one of the Hook
func (pd PluginDemo) ExtendTxn(tx *loader.Tx) error {
	//do sth
	log.Info("i am ExtendTxn")
	return nil
}

//FilterTxn is one of the Hook
func (pd PluginDemo) FilterTxn(tx *loader.Txn, info *loopbacksync.LoopBackSync) *loader.Txn {
	log.Info("i am FilterTxn")
	if tx.DDL != nil {
		return nil
	}
	for _, v := range tx.DMLs {
		v.Database = ""
	}
	return tx
}

func (pd PluginDemo) LoaderInit(db *gosql.DB, info *loopbacksync.LoopBackSync) error {
	log.Info("i am LoaderInit")
	return nil
}

func (pd PluginDemo) DestroyInit(db *gosql.DB, info *loopbacksync.LoopBackSync) error {
	log.Info("i am LoaderInit")
	return nil
}

//NewPlugin is the Factory function of plugin
func NewPlugin() interface{} {
	return PluginDemo{}
}

var _ PluginDemo
var _ = NewPlugin()

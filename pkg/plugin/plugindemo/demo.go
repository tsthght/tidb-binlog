package main

import (
	"fmt"
	gosql "database/sql"
	"strings"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"go.uber.org/zap"
)

const (
	// ID field in mark table
	ID = "id"
	// Val field in mark table
	Val = "val"
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
	err := createMarkTable(db, info.MarkTableName)
	if err != nil{
		return err
	}
	return initMarkTableData(db, info)
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

func createMarkTable(db *sql.DB, markTableName string) error {
	sql := fmt.Sprintf(
		"CREATE TABLE If Not Exists %s (" +
			"%s bigint not null PRIMARY KEY," +
			"%s bigint not null DEFAULT 0);",
		markTableName, ID,Val)
	_, err := db.Exec(sql)
	if err != nil {
		return errors.Annotate(err, "failed to create mark table")
	}

	return nil
}

func initMarkTableData(db *sql.DB, info *loopbacksync.LoopBackSync) error {
	var builder strings.Builder
	holder := "(?,?,?,?)"
	columns := fmt.Sprintf("(%s,%s) ", ID, Val)
	builder.WriteString("REPLACE INTO " + info.MarkTableName + columns + " VALUES ")
	for i := 0; i < 512; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(holder)
	}

	var args []interface{}
	for id := 0; id < 512; id++ {
		args = append(args, id, info.ChannelID, 1 /* value */, "" /*channel_info*/)
	}

	query := builder.String()
	if _, err := db.Exec(query, args...); err != nil {
		log.Error("Exec fail", zap.String("query", query), zap.Reflect("args", args), zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

var _ PluginDemo
var _ = NewPlugin()

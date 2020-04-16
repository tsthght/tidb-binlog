// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"strings"
	"sync"

	"github.com/pingcap/tidb-binlog/drainer/loopbacksync"
	dsync "github.com/pingcap/tidb-binlog/drainer/sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/drainer/relay"
	"github.com/pingcap/tidb-binlog/drainer/translator"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	"github.com/prometheus/client_golang/prometheus"
)

// MysqlSyncer sync binlog to Mysql
type MysqlSyncer struct {
	db      *sql.DB
	loader  loader.Loader
	relayer relay.Relayer

	*dsync.BaseSyncer
}

// should only be used for unit test to create mock db
var createDB = loader.CreateDBWithSQLMode

// NewMysqlSyncer returns a instance of MysqlSyncer
func NewSyncerPlugin(
	cfg *dsync.DBConfig,
	tableInfoGetter translator.TableInfoGetter,
	worker int,
	batchSize int,
	queryHistogramVec *prometheus.HistogramVec,
	sqlMode *string,
	destDBType string,
	relayer relay.Relayer,
	info *loopbacksync.LoopBackSync,
) (*MysqlSyncer, error) {
	db, err := createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	syncMode := loader.SyncMode(cfg.SyncMode)
	if syncMode == loader.SyncPartialColumn {
		var oldMode, newMode string
		oldMode, newMode, err = relaxSQLMode(db)
		if err != nil {
			db.Close()
			return nil, errors.Trace(err)
		}

		if newMode != oldMode {
			db.Close()
			db, err = createDB(cfg.User, cfg.Password, cfg.Host, cfg.Port, &newMode)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	loader, err := dsync.CreateLoader(db, cfg, worker, batchSize, queryHistogramVec, sqlMode, destDBType, info)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &MysqlSyncer{
		db:         db,
		loader:     loader,
		relayer:    relayer,
		BaseSyncer: dsync.NewBaseSyncer(tableInfoGetter),
	}

	go s.run()

	return s, nil
}

// set newMode as the oldMode query from db by removing "STRICT_TRANS_TABLES".
func relaxSQLMode(db *sql.DB) (oldMode string, newMode string, err error) {
	row := db.QueryRow("SELECT @@SESSION.sql_mode;")
	err = row.Scan(&oldMode)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	toRemove := "STRICT_TRANS_TABLES"
	newMode = oldMode

	if !strings.Contains(oldMode, toRemove) {
		return
	}

	// concatenated by "," like: mode1,mode2
	newMode = strings.Replace(newMode, toRemove+",", "", -1)
	newMode = strings.Replace(newMode, ","+toRemove, "", -1)
	newMode = strings.Replace(newMode, toRemove, "", -1)

	return
}

// SetSafeMode make the MysqlSyncer to use safe mode or not
func (m *MysqlSyncer) SetSafeMode(mode bool) {
	m.loader.SetSafeMode(mode)
}

// Sync implements Syncer interface
func (m *MysqlSyncer) Sync(item *dsync.Item) error {
	// `relayer` is nil if relay log is disabled.
	if m.relayer != nil {
		pos, err := m.relayer.WriteBinlog(item.Schema, item.Table, item.Binlog, item.PrewriteValue)
		if err != nil {
			return err
		}
		item.RelayLogPos = pos
	}

	txn, err := translator.TiBinlogToTxn(m.TableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue, item.ShouldSkip)
	if err != nil {
		return errors.Trace(err)
	}
	txn.Metadata = item

	select {
	case <-m.ErrCh:
		return m.Err
	case m.loader.Input() <- txn:
		return nil
	}
}

// Close implements Syncer interface
func (m *MysqlSyncer) Close() error {
	m.loader.Close()

	err := <-m.Error()

	if m.relayer != nil {
		closeRelayerErr := m.relayer.Close()
		if err != nil {
			err = closeRelayerErr
		}
	}

	return err
}

func (m *MysqlSyncer) run() {
	var wg sync.WaitGroup

	// handle success
	wg.Add(1)
	go func() {
		defer wg.Done()

		for txn := range m.loader.Successes() {
			item := txn.Metadata.(*dsync.Item)
			item.AppliedTS = txn.AppliedTS
			if m.relayer != nil {
				m.relayer.GCBinlog(item.RelayLogPos)
			}
			m.Success <- item
		}
		close(m.Success)
		log.Info("Successes chan quit")
	}()

	// run loader
	err := m.loader.Run()

	wg.Wait()
	m.db.Close()
	m.SetErr(err)
}

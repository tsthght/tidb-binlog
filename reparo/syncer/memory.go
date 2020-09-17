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

package syncer

// execute sql to mysql/tidb

import (
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// MemSyncer just save the pb.Binlog in memory, for test only
type MemSyncer struct {
	binlogs []*pb.Binlog
	success chan *item
}

var _ Syncer = &MemSyncer{nil, make(chan *item, 8)}

func newMemSyncer() (*MemSyncer, error) {
	return &MemSyncer{nil, make(chan *item, 8)}, nil
}

// Sync implement interface of Syncer
func (m *MemSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	m.binlogs = append(m.binlogs, pbBinlog)
	cb(pbBinlog)

	return nil
}

// Close implement interface of Syncer
func (m *MemSyncer) Close() error {
	return nil
}

// GetBinlogs return binlogs receive
func (m *MemSyncer) GetBinlogs() []*pb.Binlog {
	return m.binlogs
}

func (m *MemSyncer) Successes() <-chan *item {
	return m.success
}
package main

import (
	"fmt"
	"os"

	"github.com/pingcap/tidb-binlog/drainer/sync"
)

func main() {
	cfg := sync.NewMafkaConfig()
	e := cfg.Parse("./test.toml")
	if e != nil {
		fmt.Printf("%s", e.Error())
		os.Exit(1)
	}

	_, e = sync.NewCastleClient(cfg)
	if e != nil {
		fmt.Printf("%s\n", e.Error())
		return
	}
}

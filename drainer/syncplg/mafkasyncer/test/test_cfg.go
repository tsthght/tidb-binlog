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
	fmt.Printf("%v\n", cfg.WaitThreshold)
	fmt.Printf("%v\n", cfg.StallThreshold)
	fmt.Printf("%v\n", cfg.Topic)
	fmt.Printf("%v\n", cfg.Group)
	fmt.Printf("%v\n", cfg.BusinessAppkey)
	fmt.Printf("%v\n", cfg.CastleAppkey)
	fmt.Printf("%v\n", cfg.CastleName)
}

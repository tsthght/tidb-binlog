package main

import (
	"fmt"
	"os"

	"github.com/pingcap/tidb-binlog/drainer/sync"
)

func main() {
	err, ip := sync.GetLocalIP()
	if err != nil {
		fmt.Printf("%s", err.Error())
		os.Exit(1)
	}
	fmt.Printf("%v\n", ip)
}

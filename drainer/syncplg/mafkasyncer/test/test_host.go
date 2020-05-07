package main

import (
	"fmt"

	"github.com/pingcap/tidb-binlog/drainer/sync"
)

func main() {
	err, host := sync.GetLocalHost()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	}
	fmt.Printf("%s\n",host)
}

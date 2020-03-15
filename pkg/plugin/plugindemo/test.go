package main

import (
	"fmt"

	"github.com/pingcap/tidb-binlog/pkg/plugin"
)

func main() {
	_, err := plugin.LoadPlugin("./", "demo.so")
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}
	fmt.Printf("load success\n")
}

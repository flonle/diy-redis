package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/diyredis"
)

func main() {
	server := diyredis.MakeServer()
	flag.StringVar(&server.RdbDir, "dir", "", "the directory in which the rdb file resides")
	flag.StringVar(&server.RdbFilename, "dbfilename", "", "the name of the RDB file")
	flag.Parse()
	err := server.LoadRdb()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	server.Start()
}

// TODO list
// - intialize a pool of goroutine workers that consume connections from a channel
// - use recover() to catch all panics that happen inside a connection and not crash the
//   server. This way I can also just do check(err) on all errors that can not be recovered
//   from and should close the connection (and maybe send an error string to the client, who knows)

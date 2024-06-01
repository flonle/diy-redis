package main

import (
	"flag"

	"github.com/codecrafters-io/redis-starter-go/app/diyredis"
)

func main() {
	server := diyredis.MakeServer()
	flag.StringVar(&server.RdbDir, "dir", "", "the directory in which the rdb file resides")
	flag.StringVar(&server.RdbFilename, "dbfilename", "", "the name of the RDB file")
	flag.Parse()
	server.Start()
}

package diyredis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Session struct {
	server   *Server
	conn     net.Conn
	valueDB  *sync.Map
	expiryDB *sync.Map
	log      *log.Logger
}

func (s *Session) SwitchDB(id int) error {
	if id > len(s.server.dbs) {
		return errors.New("database does not exist")
	}

	s.valueDB = s.server.dbs[id].valueDB
	s.valueDB = s.server.dbs[id].expiryDB
	return nil
}

func (s *Session) HandleCommands() {
	reader := bufio.NewReader(s.conn)
	for {
		cmd, err := ParseCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			s.log.Println("Error parsing RESP command: ", err.Error())
			s.conn.Write([]byte("-ERR Cannot parse RESP command"))
		}

		mainCmd := strings.ToLower(cmd[0])
		switch mainCmd {
		case "ping":
			s.conn.Write([]byte("+PONG\r\n"))

		case "echo":
			payload := cmd[1]
			payloadLen := len(payload)
			s.conn.Write([]byte(fmt.Sprintf(
				"$%v\r\n%v\r\n", payloadLen, payload,
			)))

		case "set":
			if len(cmd) < 3 {
				s.conn.Write([]byte("-ERR SET needs at least 2 arguments\r\n"))
			}

			if len(cmd) > 3 && strings.ToLower(cmd[3]) == "px" {
				if len(cmd) < 4 {
					s.conn.Write([]byte("-ERR PX argument found without expiry\r\n"))
				}
				expiryInMs, err := strconv.Atoi(cmd[4])
				if err != nil {
					s.conn.Write([]byte("-ERR Cannot parse given expiry\r\n"))
					break
				}
				expiryTime := time.Now().Add(time.Duration(expiryInMs * 1000000)) // ns -> ms
				s.expiryDB.Store(cmd[1], expiryTime)
			}

			s.valueDB.Store(cmd[1], cmd[2])
			s.conn.Write([]byte("+OK\r\n"))

		case "get":
			value, ok := s.valueDB.Load(cmd[1])
			if ok {
				expiry, ok := s.expiryDB.Load(cmd[1])
				if !ok || expiry.(time.Time).After(time.Now()) {
					s.conn.Write(MakeBulkStr(value.(string)))
					break
				}
			}
			s.conn.Write([]byte("$-1\r\n"))

		case "config":
			// only supports "config get" right now
			if cmd[2] == "dir" {
				fmt.Println(s.server.RdbDir)
				s.conn.Write(MakeArray([]any{"dir", s.server.RdbDir}))
			} else if cmd[2] == "dbfilename" {
				s.conn.Write(MakeArray([]any{"dbfilename", s.server.RdbFilename}))
			}

		case "keys":
			// only support * right now
			keys := make([]any, 0)
			s.valueDB.Range(func(key any, value any) bool {
				keys = append(keys, key)
				return true
			})
			s.conn.Write(MakeArray(keys))

		case "type":
			_, ok := s.valueDB.Load(cmd[1])
			if ok {
				expiry, ok := s.expiryDB.Load(cmd[1])
				if !ok || expiry.(time.Time).After(time.Now()) {
					s.conn.Write([]byte("+string\r\n"))
					break
				}
			}
			s.conn.Write([]byte("+none\r\n"))
		}
	}
}

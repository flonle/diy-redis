package diyredis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	streams "github.com/codecrafters-io/redis-starter-go/app/diyredis/streams"
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
	s.expiryDB = s.server.dbs[id].expiryDB
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
			continue
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
				s.conn.Write([]byte("-ERR Wrong number of arguments for SET command\r\n"))
				continue
			}

			// Technically there's a race condition here because the expiry map and
			// the value map are not synchronized in any way. A reader could read
			// a new value with an old expiry value or vice versa ¯\_(ツ)_/¯
			if len(cmd) > 3 && strings.ToLower(cmd[3]) == "px" {
				if len(cmd) < 4 {
					s.conn.Write([]byte("-ERR PX argument found without expiry\r\n"))
					continue
				}
				expiryInMs, err := strconv.Atoi(cmd[4])
				if err != nil {
					s.conn.Write([]byte("-ERR Cannot parse given expiry\r\n"))
					continue
				}
				expiryTime := time.Now().Add(time.Duration(expiryInMs * 1000000)) // ns -> ms
				s.expiryDB.Store(cmd[1], expiryTime)
			}

			s.valueDB.Store(cmd[1], cmd[2])
			s.conn.Write([]byte("+OK\r\n"))

		case "get":
			value, ok := s.valueDB.Load(cmd[1])
			fmt.Println("ok")
			if ok {
				expiry, ok := s.expiryDB.Load(cmd[1])
				fmt.Println("ok")
				if !ok || expiry.(time.Time).After(time.Now()) {
					strVal, ok := value.(string) // while the map implementation can, and does, hold arbitrary types, get GET command is only for string
					fmt.Println("ikd")
					if !ok {
						fmt.Println("ikd")
						s.conn.Write([]byte(
							"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
						))
						continue
					}
					s.conn.Write(MakeBulkStr(strVal))
					continue
				}
			}
			s.conn.Write([]byte("$-1\r\n")) // key not found

		case "config":
			// only supports "config get" right now
			if cmd[2] == "dir" {
				fmt.Println(s.server.RdbDir)
				s.conn.Write(MakeArray([]any{"dir", s.server.RdbDir}))
			} else if cmd[2] == "dbfilename" {
				s.conn.Write(MakeArray([]any{"dbfilename", s.server.RdbFilename}))
			}

		case "keys":
			// only supports * right now
			keys := make([]any, 0)
			s.valueDB.Range(func(key any, value any) bool {
				keys = append(keys, key)
				return true
			})
			s.conn.Write(MakeArray(keys))

		case "type":
			value, ok := s.valueDB.Load(cmd[1])
			if ok {
				expiry, ok := s.expiryDB.Load(cmd[1])
				if !ok || expiry.(time.Time).After(time.Now()) {
					_, ok := value.(*streams.Stream)
					if ok {
						s.conn.Write([]byte("+stream\r\n"))
					} else {
						s.conn.Write([]byte(
							"+" + strings.ToLower(reflect.TypeOf(value).Name()) + "\r\n"),
						)
					}
					break
				}
			}
			s.conn.Write([]byte("+none\r\n"))

		case "xadd":
			if len(cmd) < 5 {
				s.conn.Write([]byte("-ERR Wrong number of arguments for XADD command\r\n"))
				continue
			}

			streamKey := cmd[1]
			fmt.Println(streamKey)
			value, ok := s.valueDB.Load(streamKey)
			var stream *streams.Stream
			if ok {
				stream, ok = value.(*streams.Stream)
				if !ok {
					s.conn.Write([]byte(
						"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
					))
					continue
				}
			} else {
				stream = &streams.Stream{}
				s.valueDB.Store(streamKey, stream)
				// Technically this causes (empty) streams to be created even if the adding of the first entry failed
			}

			streamEntryKey, err := streams.NewKey(cmd[2])
			if err != nil {
				s.conn.Write([]byte(
					"-ERR Could not parse given entry key\r\n",
				))
				continue
			}

			if streamEntryKey.LeftNr == 0 && streamEntryKey.RightNr == 0 {
				s.conn.Write([]byte(
					"-ERR The ID specified in XADD must be greater than 0-0\r\n",
				))
				continue
			}

			if !streamEntryKey.GreaterThan(stream.LastKey) {
				s.conn.Write([]byte(
					"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
				))
				continue
			}

			keyVals := cmd[3:len(cmd)]
			if len(keyVals) < 2 {
				s.conn.Write([]byte(
					"-ERR A stream entry needs at least one key value pair\r\n",
				))
				continue
			} else if len(keyVals)%2 != 0 {
				s.conn.Write([]byte(
					"-ERR Got an uneven amount of key value pairs -- that can't be right\r\n",
				))
				continue
			}

			streamEntryVal := make(map[string]string, len(keyVals)/2)
			for i := 0; i < len(keyVals); i += 2 {
				streamEntryVal[keyVals[i]] = keyVals[i+1] // this will never be out of bounds because of the modulo check above
			}
			stream.InsertKey(streamEntryKey, streamEntryVal)

			// entryID := strings.Split(cmd[2], "-")
			// if len(entryID) != 2 {
			// 	s.conn.Write([]byte("-ERR Failed to parse command"))
			// }
			// timestampMs, err := strconv.ParseUint(entryID[0], 10, 64)
			// subID, err2 := strconv.ParseUint(entryID[1], 10, 64)
			// if err != nil || err2 != nil {
			// 	s.conn.Write([]byte("-ERR Failed to parse command"))
			// }

			// streamEntry := StreamEntry{
			// 	TimestampMs: timestampMs, // time.Now().UnixMilli() if we had to automate
			// 	SubID:       subID,
			// 	Vals:        cmd[3:len(cmd)],
			// }
			// s.valueDB.Store(streamKey)
			s.conn.Write(MakeBulkStr(streamEntryKey.String()))

		// case "xrange":
		// if len(cmd) < 4 {
		// 	s.conn.Write([]byte("-ERR Wrong number of arguments for XADD command\r\n"))
		// 	continue
		// }

		// value, ok := s.valueDB.Load(cmd[1])
		// if !ok {
		// 	s.conn.Write(MakeArray(nil)) // empty array
		// 	continue
		// }
		// stream, ok := value.(Stream)
		// if !ok {
		// 	s.conn.Write([]byte(
		// 		"-ERR WRONGTYPE Operation against a key holding the wrong kind of value",
		// 	))
		// 	continue
		// }

		// buf := []

		// start, end := cmd[2], cmd[3]

		// s.conn.Write(MakeBulkStr(""))

		default:
			s.conn.Write([]byte("-ERR Command not known\r\n"))
			continue
		}
	}
}

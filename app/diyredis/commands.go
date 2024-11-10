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
			s.doPING(cmd)
		case "echo":
			s.doECHO(cmd)
		case "set":
			s.doSET(cmd)
		case "get":
			s.doGET(cmd)
		case "config":
			s.doCONFIG(cmd)
		case "keys":
			s.doKEYS(cmd)
		case "type":
			s.doTYPE(cmd)
		case "xadd":
			s.doXADD(cmd)
		default:
			s.conn.Write([]byte("-ERR Command not known\r\n"))
		}
	}
}

func (s *Session) writeError(e error) {
	s.conn.Write([]byte("-ERR " + e.Error() + "\r\n"))
}

func (s *Session) doXADD(cmds []string) {
	if len(cmds) < 5 {
		s.conn.Write([]byte("-ERR Wrong number of arguments for XADD command\r\n"))
		return
	}

	streamKey := cmds[1]
	fmt.Println(streamKey)
	value, ok := s.valueDB.Load(streamKey)
	var stream *streams.Stream
	if ok {
		stream, ok = value.(*streams.Stream)
		if !ok {
			s.conn.Write([]byte(
				"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			))
			return
		}
	} else {
		stream = &streams.Stream{}
		s.valueDB.Store(streamKey, stream)
		// Technically this causes empty streams to be created, if adding the first entry fails
	}

	streamEntryKey, err := stream.NewKey(cmds[2])
	if err != nil {
		s.conn.Write([]byte(fmt.Sprintf(
			"-ERR Could not parse given entry key: %s\r\n", err.Error(),
		)))
		return
	}

	if streamEntryKey.LeftNr == 0 && streamEntryKey.RightNr == 0 {
		s.conn.Write([]byte(
			"-ERR The ID specified in XADD must be greater than 0-0\r\n",
		))
		return
	}

	if !streamEntryKey.GreaterThan(stream.LastKey) {
		s.conn.Write([]byte(
			"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
		))
		return
	}

	keyVals := cmds[3:len(cmds)]
	if len(keyVals) < 2 {
		s.conn.Write([]byte(
			"-ERR A stream entry needs at least one key value pair\r\n",
		))
		return
	} else if len(keyVals)%2 != 0 {
		s.conn.Write([]byte(
			"-ERR Received a key without a value\r\n",
		))
		return
	}

	streamEntryVal := make(map[string]string, len(keyVals)/2)
	for i := 0; i < len(keyVals); i += 2 {
		streamEntryVal[keyVals[i]] = keyVals[i+1] // this will never be out of bounds because of the modulo check above
	}
	stream.InsertKey(streamEntryKey, streamEntryVal)
	s.conn.Write(MakeBulkStr(streamEntryKey.String()))
}

func (s *Session) doTYPE(cmds []string) {
	value, ok := s.valueDB.Load(cmds[1])
	if ok {
		expiry, ok := s.expiryDB.Load(cmds[1])
		if !ok || expiry.(time.Time).After(time.Now()) {
			_, ok := value.(*streams.Stream)
			if ok {
				s.conn.Write([]byte("+stream\r\n"))
			} else {
				s.conn.Write([]byte(
					"+" + strings.ToLower(reflect.TypeOf(value).Name()) + "\r\n"),
				)
			}
			return
		}
	}
	s.conn.Write([]byte("+none\r\n"))
}

func (s *Session) doKEYS(cmds []string) {
	// only supports * right now
	keys := make([]any, 0)
	s.valueDB.Range(func(key any, value any) bool {
		keys = append(keys, key)
		return true
	})
	s.conn.Write(MakeArray(keys))
}

func (s *Session) doCONFIG(cmds []string) {
	// only supports "config get" right now
	if cmds[2] == "dir" {
		fmt.Println(s.server.RdbDir)
		s.conn.Write(MakeArray([]any{"dir", s.server.RdbDir}))
	} else if cmds[2] == "dbfilename" {
		s.conn.Write(MakeArray([]any{"dbfilename", s.server.RdbFilename}))
	}
}

func (s *Session) doGET(cmds []string) {
	value, ok := s.valueDB.Load(cmds[1])
	if ok {
		expiry, ok := s.expiryDB.Load(cmds[1])
		if !ok || expiry.(time.Time).After(time.Now()) {
			strVal, ok := value.(string) // while the map implementation can, and does, hold arbitrary types, get GET command is only for string
			if !ok {
				s.conn.Write([]byte(
					"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
				))
				return
			}
			s.conn.Write(MakeBulkStr(strVal))
			return
		}
	}
	s.conn.Write([]byte("$-1\r\n")) // key not found
}

func (s *Session) doSET(cmds []string) {
	if len(cmds) < 3 {
		s.conn.Write([]byte("-ERR Wrong number of arguments for SET command\r\n"))
		return
	}

	// There's a race condition here because the expiry map and
	// the value map are not synchronized in any way. A reader could read
	// a new value with an old expiry value and vice versa ¯\_(ツ)_/¯
	if len(cmds) > 3 && strings.ToLower(cmds[3]) == "px" {
		if len(cmds) < 4 {
			s.conn.Write([]byte("-ERR PX argument found without expiry\r\n"))
			return
		}
		expiryInMs, err := strconv.Atoi(cmds[4])
		if err != nil {
			s.conn.Write([]byte("-ERR Cannot parse given expiry\r\n"))
			return
		}
		expiryTime := time.Now().Add(time.Duration(expiryInMs * 1000000)) // ns -> ms
		s.expiryDB.Store(cmds[1], expiryTime)
	}

	s.valueDB.Store(cmds[1], cmds[2])
	s.conn.Write([]byte("+OK\r\n"))
}

func (s *Session) doECHO(cmds []string) {
	payload := cmds[1]
	payloadLen := len(payload)
	s.conn.Write([]byte(fmt.Sprintf(
		"$%v\r\n%v\r\n", payloadLen, payload,
	)))
}

func (s *Session) doPING(cmds []string) {
	s.conn.Write([]byte("+PONG\r\n"))
}

func (s *Session) doXRANGE(cmds []string) {
	return
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
}

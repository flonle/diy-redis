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

	resp3 "github.com/codecrafters-io/redis-starter-go/app/diyredis/resp3"
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
		var uerr *UserError
		switch mainCmd {
		case "ping":
			uerr = s.doPING(cmd)
		case "echo":
			uerr = s.doECHO(cmd)
		case "set":
			uerr = s.doSET(cmd)
		case "get":
			uerr = s.doGET(cmd)
		case "config":
			uerr = s.doCONFIG(cmd)
		case "keys":
			uerr = s.doKEYS(cmd)
		case "type":
			uerr = s.doTYPE(cmd)
		case "xadd":
			uerr = s.doXADD(cmd)
		case "xrange":
			uerr = s.doXRANGE(cmd)
		case "xread":
			uerr = s.doXREAD(cmd)
		default:
			uerr = &UserError{"Command not known"}
		}

		if uerr != nil {
			s.conn.Write(uerr.RESP())
		}
	}
}

// RESP array of bulk strings -> Go array of strings
func ParseCommand(reader *bufio.Reader) ([]string, error) {
	unit, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if unit[0] != '*' {
		return nil, fmt.Errorf("expected RESP array (*), got: %v", unit[0])
	}
	arrayLength, err := strconv.Atoi(unit[1 : len(unit)-2])
	if err != nil {
		return nil, err
	}

	command := make([]string, arrayLength)
	for i := range arrayLength {
		bulkStrHeader, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if bulkStrHeader[0] != '$' {
			return nil, fmt.Errorf("expected RESP bulk string ($), got: %v", bulkStrHeader[0])
		}
		bulkStrLen, err := strconv.Atoi(bulkStrHeader[1 : len(bulkStrHeader)-2])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, bulkStrLen+2) // +2 is for the \r\n at the end of the bulk string
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}
		command[i] = string(buf[:len(buf)-2])
	}
	return command, nil

}

func (s *Session) doXADD(cmds []string) *UserError {
	if len(cmds) < 5 {
		// s.conn.Write([]byte("-ERR Wrong number of arguments for XADD command\r\n"))
		// return
		return &UserError{"wrong number of arguments for XADD command"}
	}

	streamKey := cmds[1]
	value, ok := s.valueDB.Load(streamKey)
	var stream *streams.Stream
	if ok {
		stream, ok = value.(*streams.Stream)
		if !ok {
			// s.conn.Write([]byte(
			// 	"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			// ))
			// return
			return &UserError{"WRONGTYPE Operation against a key holding the wrong kind of value"}
		}
	} else {
		stream = streams.NewStream()
		s.valueDB.Store(streamKey, stream)
		// Technically this causes empty streams to be created, if adding the first entry fails
	}

	streamEntryKey, err := streams.NewKey(cmds[2], stream)
	if err != nil {
		// s.conn.Write([]byte(fmt.Sprintf(
		// 	"could not parse given entry key: %s\r\n", err.Error(),
		// )))
		// return
		return &UserError{fmt.Sprintf(
			"could not parse given entry key: %s", err.Error(),
		)}
	}

	if streamEntryKey.LeftNr == 0 && streamEntryKey.RightNr == 0 {
		// s.conn.Write([]byte(
		// 	"-ERR The ID specified in XADD must be greater than 0-0\r\n",
		// ))
		// return
		return &UserError{"the ID specified in XADD must be greater than 0-0"}
	}

	if !streamEntryKey.GreaterThan(stream.LastEntry.Key) {
		// s.conn.Write([]byte(
		// 	"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
		// ))
		// return
		return &UserError{
			"the ID specified in XADD is equal or smaller than the target stream top item",
		}
	}

	keyVals := cmds[3:]
	if len(keyVals) < 2 {
		// s.conn.Write([]byte(
		// 	"-ERR A stream entry needs at least one key value pair\r\n",
		// ))
		// return
		return &UserError{"a stream entry needs at least one key value pair"}
	} else if len(keyVals)%2 != 0 {
		// s.conn.Write([]byte(
		// 	"-ERR Received a key without a value\r\n",
		// ))
		// return
		return &UserError{"received a key without a value"}
	}

	streamEntryVal := make(map[string]string, len(keyVals)/2)
	for i := 0; i < len(keyVals); i += 2 {
		streamEntryVal[keyVals[i]] = keyVals[i+1] // this will never be out of bounds because of the modulo check above
	}
	stream.Put(streamEntryKey, streamEntryVal)

	encoder := resp3.Encoder{}
	encoder.WriteBulkStr(streamEntryKey.String())
	s.conn.Write(encoder.Buf)
	return nil
}

func (s *Session) doTYPE(cmds []string) *UserError {
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
			return nil
		}
	}
	s.conn.Write([]byte("+none\r\n"))
	return nil
}

func (s *Session) doKEYS(cmds []string) *UserError {
	// only supports * right now
	keys := make([]string, 0)
	s.valueDB.Range(func(key any, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	s.conn.Write(makeRESPArr(keys))
	return nil
}

func (s *Session) doCONFIG(cmds []string) *UserError {
	// only supports "config get" right now
	if cmds[2] == "dir" {
		s.conn.Write(makeRESPArr([]string{"dir", s.server.RdbDir}))
	} else if cmds[2] == "dbfilename" {
		s.conn.Write(makeRESPArr([]string{"dbfilename", s.server.RdbFilename}))
	}
	return nil
}

func (s *Session) doGET(cmds []string) *UserError {
	value, ok := s.valueDB.Load(cmds[1])
	if ok {
		expiry, ok := s.expiryDB.Load(cmds[1])
		if !ok || expiry.(time.Time).After(time.Now()) {
			strVal, ok := value.(string) // while the map implementation can, and does, hold arbitrary types, get GET command is only for string
			if !ok {
				// s.conn.Write([]byte(
				// 	"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
				// ))
				// return
				return &UserError{"WRONGTYPE Operation against a key holding the wrong kind of value"}
			}

			encoder := resp3.Encoder{}
			encoder.WriteBulkStr(strVal)
			s.conn.Write(encoder.Buf)
			return nil
		}
	}

	s.conn.Write([]byte("$-1\r\n")) // key not found
	return nil
}

func (s *Session) doSET(cmds []string) *UserError {
	if len(cmds) < 3 {
		// s.conn.Write([]byte("-ERR Wrong number of arguments for SET command\r\n"))
		// return
		return &UserError{"wrong number of arguments for SET command"}
	}

	// There's a race condition here because the expiry map and
	// the value map are not synchronized in any way. A reader could read
	// a new value with an old expiry value and vice versa ¯\_(ツ)_/¯
	if len(cmds) > 3 && strings.ToLower(cmds[3]) == "px" {
		if len(cmds) < 4 {
			// s.conn.Write([]byte("-ERR PX argument found without expiry\r\n"))
			// return
			return &UserError{"PX argument found without expiry"}
		}
		expiryInMs, err := strconv.Atoi(cmds[4])
		if err != nil {
			// s.conn.Write([]byte("-ERR Cannot parse given expiry\r\n"))
			// return
			return &UserError{"cannot parse given expiry"}
		}
		expiryTime := time.Now().Add(time.Duration(expiryInMs * 1000000)) // ns -> ms
		s.expiryDB.Store(cmds[1], expiryTime)
	}

	s.valueDB.Store(cmds[1], cmds[2])
	s.conn.Write([]byte("+OK\r\n"))
	return nil
}

func (s *Session) doECHO(cmds []string) *UserError {
	payload := cmds[1]
	payloadLen := len(payload)
	s.conn.Write([]byte(fmt.Sprintf(
		"$%v\r\n%v\r\n", payloadLen, payload,
	)))
	return nil
}

func (s *Session) doPING(cmds []string) *UserError {
	s.conn.Write([]byte("+PONG\r\n"))
	return nil
}

func (s *Session) doXRANGE(cmds []string) *UserError {
	if len(cmds) < 4 {
		// s.conn.Write([]byte("-ERR Wrong number of arguments for XRANGE command\r\n"))
		// return
		return &UserError{"wrong number of arguments for XRANGE command"}
	}

	value, ok := s.valueDB.Load(cmds[1])
	if !ok {
		s.conn.Write(EmptyRespArr)
		return nil
	}
	stream, ok := value.(*streams.Stream)
	if !ok {
		// 	s.conn.Write([]byte(
		// 		"-ERR WRONGTYPE Operation against a key holding the wrong kind of value",
		// 	))
		// 	return
		return &UserError{"WRONTYPE operation against a key holding the wrong kind of value"}
	}

	fromKey, err := streams.NewKey(cmds[2], stream)
	if err != nil {
		// s.conn.Write([]byte("-ERR Bad \"from\" key"))
		// return
		return &UserError{"bad \"from\" key"}
	}
	toKey, err := streams.NewKey(cmds[3], stream)
	if err != nil {
		// s.conn.Write([]byte("-ERR Bad \"to\" key"))
		// return
		return &UserError{"bad \"to\" key"}
	}

	encoder := &resp3.Encoder{}
	err = entriesToRESP(encoder, stream.Range(fromKey, toKey))
	if err != nil {
		s.conn.Write([]byte("-ERR Something went wrong"))
	}
	s.conn.Write(encoder.Buf)
	return nil
}

func (s *Session) doXREAD(cmds []string) *UserError {
	if len(cmds) < 4 {
		// s.conn.Write([]byte("-ERR Wrong number of arguments for XREAD command\r\n"))
		// return
		return &UserError{"wrong number of arguments for XREAD command"}
	}

	// Parse commands, find stream name(s) and their respective keys.
	var streamNames []string
	var keys []string
	var i int
	var blockArg string
	for i = 0; i < len(cmds)-1; i++ {
		cmd := strings.ToLower(cmds[i])
		if cmd == "block" {
			blockArg = cmds[i+1]
			i++
		} else if cmd == "streams" {
			streamsStartIdx := i + 1
			remaining := len(cmds) - streamsStartIdx
			streamsEndIdx := streamsStartIdx + remaining/2
			streamNames = cmds[i+1 : streamsEndIdx]
			keys = cmds[streamsEndIdx:]
			break
		}
	}

	// // Collect stream pointers & correct "from" keys
	results := make(map[*streams.Stream][]streams.Entry, len(streamNames))
	// streamObjs := make([]*streams.Stream, len(streamNames))
	// keyObjs := make([]streams.Key, len(keys))
	emptyResult := true
	// collectCh := make(chan streams.NewEntryMsg)
	for i, streamName := range streamNames {
		value, ok := s.valueDB.Load(streamName)
		if !ok {
			return &UserError{"stream does not exist: " + streamName}
		}
		stream, ok := value.(*streams.Stream)
		if !ok {
			return &UserError{"WRONGTYPE operation against a key holding the wrong kind of value"}
		}

		var fromKey streams.Key
		if keys[i] == "$" {
			fromKey = stream.LastEntry.Key
		} else {
			var err error
			fromKey, err = streams.NewKey(keys[i], stream)
			if err != nil {
				return &UserError{"bad key: " + keys[i]}
			}
		}

		if stream.LastEntry.Key.GreaterThan(fromKey) {
			emptyResult = false
			fromKey, overflow := fromKey.Next()
			if overflow {
				continue
			}
			results[stream] = stream.Range(fromKey, streams.MaxKey)
		} else {
			results[stream] = []streams.Entry{}
		}
		// fromKey, overflow := fromKey.Next()
		// if overflow {
		// 	continue
		// 	// this causes the largest valid key to block forever with BLOCK = 0.
		// 	// Redis does the same, and I think it makes sense. The supplied key is valid,
		// 	// it will just never have a valid resultset.
		// }
		// results[i] = stream.Range(fromKey, streams.MaxKey)
	}

	// Check & handle the BLOCK subcommand
	if emptyResult && len(blockArg) > 0 {
		blockMs, err := strconv.Atoi(blockArg)
		if err != nil {
			return &UserError{"syntax error: invalid BLOCK value"}
		} else if blockMs < 0 {
			return &UserError{"BLOCK must be a positive value"}
		}

		//todo for each stream i need to subscribe
		// and then we put the entry in a slice in result[i]
		ch := make(chan streams.NewEntryMsg)
		for stream, _ := range results {
			stream.Subscribe(ch, stream)
		}
		var entryMsg streams.NewEntryMsg
		if blockMs == 0 {
			entryMsg = <-ch
		} else {
			select {
			case entryMsg = <-ch:
			case <-time.After(time.Duration(blockMs) * time.Millisecond):
				s.conn.Write([]byte("$-1\r\n"))
				return nil
			}
		}
		results[entryMsg.SubscriptionID.(*streams.Stream)] = []streams.Entry{entryMsg.Entry}
	}

	// time.Sleep(time.Duration(blockMs) * time.Millisecond)

	// TODO
	// just doing sleep is not strictly correct. Only sleep if one of the resultsets
	// is empty and block is set. Then, wait indefinetly if block == 0 otherwait wait for block ms
	//

	// Encode to RESP
	respEncoder := &resp3.Encoder{}
	respEncoder.WriteArrHeader(len(results))
	for i, streamName := range streamNames {
		if len(results[i]) == 0 {
			continue
		}
		respEncoder.WriteArrHeader(2)
		respEncoder.WriteBulkStr(streamName)
		err := entriesToRESP(respEncoder, results[i])
		if err != nil {
			return &UserError{"something went wrong"}
		}
	}

	return nil
}

func (s *Session) collectXREAD(streamNames []string, keys []string) *UserError {
	respEncoder := &resp3.Encoder{}
	respEncoder.WriteArrHeader(len(streamNames))

	for i, streamName := range streamNames {
		value, ok := s.valueDB.Load(streamName)
		if !ok {
			continue
		}
		stream, ok := value.(*streams.Stream)
		if !ok {
			// s.conn.Write([]byte(
			// 	"-ERR WRONGTYPE Operation against a key holding the wrong kind of value",
			// ))
			// return true
			return &UserError{"WRONGTYPE operation against a key holding the wrong kind of value"}
		}

		var fromKey streams.Key
		if keys[i] == "$" {
			fromKey = stream.LastEntry.Key
		} else {
			var err error
			fromKey, err = streams.NewKey(keys[i], stream)
			if err != nil {
				// s.conn.Write([]byte("-ERR Bad key: " + keys[i]))
				// return true
				return &UserError{"bad key: " + keys[i]}
			}
		}

		respEncoder.WriteArrHeader(2)
		respEncoder.WriteBulkStr(streamName)

		fromKey, overflow := fromKey.Next()
		if overflow {
			respEncoder.Buf = append(respEncoder.Buf, EmptyRespArr...)
			continue
		}
		err := entriesToRESP(respEncoder, stream.Range(fromKey, streams.MaxKey))
		if err != nil {
			// s.conn.Write([]byte("-ERR something went wrong"))
			// return true
			return &UserError{"something went wrong"}
		}
	}

	s.conn.Write(respEncoder.Buf)
	return nil
}

func (s *Session) collectBlockingXREAD(ms int, streamNames []string, keys []string) *UserError {
	// TODO search for every stream, go func() a closure with waitgroup to call WaitForEntry
	// after above loop, wait for all streams via wg
	// Then, send Entry from spawned goroutine to this one

	respEncoder := &resp3.Encoder{}
	respEncoder.WriteArrHeader(len(streamNames))

	for i, streamName := range streamNames {
		value, ok := s.valueDB.Load(streamName)
		if !ok {
			continue
		}
		stream, ok := value.(*streams.Stream)
		if !ok {
			return &UserError{"WRONGTYPE operation against a key holding the wrong kind of value"}
		}
	}
}

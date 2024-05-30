package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	server := MakeServer()
	server.Start()
}

type Server struct {
	listener net.Listener
	quitch   chan os.Signal
	wg       *sync.WaitGroup
	db       *sync.Map
	expirydb *sync.Map
}

func MakeServer() *Server {
	var wg sync.WaitGroup
	return &Server{
		quitch:   make(chan os.Signal, 1),
		db:       &sync.Map{},
		expirydb: &sync.Map{},
		wg:       &wg,
	}
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()
	s.listener = listener

	go s.serve()
	signal.Notify(s.quitch, syscall.SIGINT, syscall.SIGTERM)

	<-s.quitch // this is blocking until it receives any message on the channel...
	fmt.Println("Shutting Down...")
	s.wg.Wait()
	fmt.Println("Shutdown Complete")
}

func (s *Server) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	connLog := log.New(os.Stderr, conn.RemoteAddr().String(), log.LstdFlags)
	s.wg.Add(1)
	defer s.wg.Done()

	reader := bufio.NewReader(conn)
	for {
		cmd, err := parseCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			connLog.Println("Error parsing RESP command: ", err.Error())
			conn.Write(makeRespError("ERR", "Error parsing RESP command: ", err.Error()))
		}
		mainCmd := strings.ToLower(cmd[0])
		switch mainCmd {
		case "ping":
			conn.Write([]byte("+PONG\r\n"))
		case "echo":
			payload := cmd[1]
			payloadLen := len(payload)
			conn.Write([]byte(fmt.Sprintf(
				"$%v\r\n%v\r\n", payloadLen, payload,
			)))
		case "set":
			if len(cmd) < 3 {
				conn.Write([]byte("-ERR SET needs at least 2 arguments\r\n"))
			}
			s.db.Store(cmd[1], cmd[2])
			if len(cmd) > 3 && strings.ToLower(cmd[3]) == "px" {
				if len(cmd) < 4 {
					conn.Write([]byte("-ERR PX argument found without expiry\r\n"))
				}
				expiryInMs, err := strconv.Atoi(cmd[4])
				if err != nil {
					conn.Write([]byte("-ERR Cannot parse given expiry\r\n"))
					break
				}
				expiryTime := time.Now().Add(time.Duration(expiryInMs * 1000000)) // ns -> ms
				s.expirydb.Store(cmd[1], expiryTime)
			}
			conn.Write([]byte("+OK\r\n"))
		case "get":
			value, ok := s.db.Load(cmd[1])
			if ok {
				expiry, ok := s.expirydb.Load(cmd[1])
				if ok {
				}
				if !ok || expiry.(time.Time).After(time.Now()) {
					conn.Write(makeBulkStr(value.(string)))
					break
				}

			}
			conn.Write([]byte("$-1\r\n"))
		}
	}
}

// RESP array of bulk strings -> Go array of strings
func parseCommand(reader *bufio.Reader) ([]string, error) {
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

func makeRespError(errType string, errFmt string, a ...any) []byte {
	// TODO if contains \r or \n, automatically make bulk string
	errMsg := fmt.Sprintf(errFmt, a...)
	return []byte(fmt.Sprintf("-%s %s\r\n", errType, errMsg))
}

// Go string -> RESP bulk string
func makeBulkStr(input string) []byte {
	return []byte(fmt.Sprintf(
		"$%s\r\n%s\r\n",
		strconv.Itoa(len(input)),
		input,
	))
}

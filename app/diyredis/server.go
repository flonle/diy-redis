package diyredis

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

type Server struct {
	Listener    net.Listener
	Quitch      chan os.Signal
	wg          *sync.WaitGroup
	db          *sync.Map
	expirydb    *sync.Map
	RdbDir      string
	RdbFilename string
}

func MakeServer() *Server {
	var wg sync.WaitGroup
	return &Server{
		Quitch:   make(chan os.Signal, 1),
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
	s.Listener = listener

	go s.serve()
	signal.Notify(s.Quitch, syscall.SIGINT, syscall.SIGTERM)

	<-s.Quitch // this is blocking until it receives any message on the channel...
	fmt.Println("Shutting Down...")
	s.wg.Wait()
	fmt.Println("Shutdown Complete")
}

func (s *Server) serve() {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
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
		cmd, err := ParseCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			connLog.Println("Error parsing RESP command: ", err.Error())
			conn.Write([]byte("-ERR Cannot parse RESP command"))
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
				if !ok || expiry.(time.Time).After(time.Now()) {
					conn.Write(MakeBulkStr(value.(string)))
					break
				}
			}
			conn.Write([]byte("$-1\r\n"))
		case "config":
			// only supports "config get" right now
			if cmd[2] == "dir" {
				fmt.Println(s.RdbDir)
				conn.Write(MakeArray([]any{"dir", s.RdbDir}))
			} else if cmd[2] == "dbfilename" {
				conn.Write(MakeArray([]any{"dbfilename", s.RdbFilename}))
			}
		}
	}
}

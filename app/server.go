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
	"sync"
	"syscall"
)

func main() {
	server := MakeServer()
	server.Start()
}

type Server struct {
	listener net.Listener
	quitch   chan os.Signal
	wg       *sync.WaitGroup
}

func MakeServer() *Server {
	return &Server{
		quitch: make(chan os.Signal, 1),
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

	var wg sync.WaitGroup
	s.wg = &wg
	go s.serve()

	signal.Notify(s.quitch, syscall.SIGINT, syscall.SIGTERM)
	<-s.quitch // this is blocking until it receives any message on the channel...
	fmt.Println("Shutting Down...")
	wg.Wait()
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

// type Conn struct {
// 	conn   net.Conn
// 	log    *log.Logger
// 	reader *bufio.Reader
// }

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	connLog := log.New(os.Stderr, conn.RemoteAddr().String(), log.LstdFlags)
	s.wg.Add(1)
	defer s.wg.Done()

	reader := bufio.NewReader(conn)
	// buf := make([]byte, 1024)
	// io.ReadFull(reader, buf)
	// thisConn := &Conn{
	// 	conn:   conn,
	// 	log:    connLog,
	// 	reader: reader,
	// }

	for {
		cmd, err := parseCommand(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			connLog.Println("Error parsing RESP command: ", err.Error())
			conn.Write(makeRespError("ERR", "Error parsing RESP command: ", err.Error()))
		}
		fmt.Println(cmd)
		conn.Write([]byte("+PONG\r\n"))

	}

	// conn.Write([]byte("+PONG\r\n"))
	// for {
	// 	line, err := reader.ReadString('\n')
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			return
	// 		}
	// 		return
	// 	}

	// 	fmt.Println(line)

	// 	var dataType byte = line[0]
	// 	if dataType != '*' {
	// 		conn.Write(makeRespError("ERR", "Expected RESP array (*), got: ", line))
	// 	}
	// 	arrayLength, err := strconv.Atoi(line[1 : len(line)-2])
	// 	if err != nil {
	// 		conn.Write(makeRespError("ERR", "Error processing RESP array"))
	// 	}
	// 	for i := range arrayLength {

	// 	}
	// }
}

// type Command struct {
// 	cmd  string
// 	args []string
// }

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
		command[i] = string(buf)
	}
	return command, nil

}

func makeRespError(errType string, errFmt string, a ...any) []byte {
	// TODO if contains \r or \n, automatically make bulk string
	errMsg := fmt.Sprintf(errFmt, a...)
	return []byte(fmt.Sprintf("-%s %s\r\n", errType, errMsg))
}

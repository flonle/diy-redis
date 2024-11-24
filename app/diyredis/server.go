package diyredis

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Server struct {
	Listener    net.Listener
	Quitch      chan os.Signal
	wg          *sync.WaitGroup
	dbs         []RedisDB
	RdbDir      string
	RdbFilename string
}

type RedisDB struct {
	id       uint
	valueDB  *sync.Map
	expiryDB *sync.Map
}

func MakeServer() *Server {
	var wg sync.WaitGroup
	dbCount := 16 // 16 databases by default, just like Redis
	server := Server{
		Quitch: make(chan os.Signal, 1),
		dbs:    make([]RedisDB, dbCount),
		wg:     &wg,
	}
	for i := range dbCount {
		server.dbs[i].id = uint(i)
		server.dbs[i].valueDB = &sync.Map{}
		server.dbs[i].expiryDB = &sync.Map{}
	}
	return &server
}

func (s *Server) Start() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Printf("Failed to bind to port 6379: %s", err)
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
		go s.startSession(conn)
	}
}

func (s *Server) startSession(conn net.Conn) {
	defer conn.Close()
	connLog := log.New(os.Stderr, conn.RemoteAddr().String(), log.LstdFlags)
	s.wg.Add(1)
	defer s.wg.Done()

	session := &Session{
		server:   s,
		conn:     conn,
		valueDB:  s.dbs[0].valueDB, // db 0 as default
		expiryDB: s.dbs[0].expiryDB,
		log:      connLog,
	}
	session.HandleCommands()
}

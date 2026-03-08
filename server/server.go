package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/smoss/postgres-mem-go/engine"
)

// Server is a TCP server that accepts PostgreSQL wire protocol connections.
type Server struct {
	addr     string
	listener net.Listener
	wg       sync.WaitGroup
	closed   atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	engine   *engine.Engine
}

// New creates a new Server with the given address.
// If addr is empty, it defaults to ":0" (random available port).
func New(addr string) *Server {
	if addr == "" {
		addr = ":0"
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		ctx:    ctx,
		cancel: cancel,
		engine: engine.New(),
	}
}

// Addr returns the server's listener address.
// Returns nil if the server hasn't been started yet.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Start begins listening for incoming connections.
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = listener

	// Start the query engine
	s.engine.Start()

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	if s.closed.CompareAndSwap(false, true) {
		s.cancel()
		if s.listener != nil {
			_ = s.listener.Close()
		}
		s.engine.Stop()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			handleConnection(conn, s.engine)
		}()
	}
}

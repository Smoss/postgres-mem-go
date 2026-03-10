package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/smoss/postgres-mem-go/server"
)

func main() {
	var addr string
	flag.StringVar(
		&addr,
		"addr",
		":5432",
		"Address to listen on (e.g., :5432 or localhost:5432)",
	)
	flag.Parse()

	srv := server.New(addr)
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	actualAddr := srv.Addr()
	fmt.Printf("PostgreSQL-compatible server listening on %s\n", actualAddr)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
	fmt.Println("Server stopped.")
}

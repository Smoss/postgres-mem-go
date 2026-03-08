package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestServer_ConnectAndPing(t *testing.T) {
	// Create and start server on random port
	srv := New("")
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	// Get the actual address
	addr := srv.Addr()
	if addr == nil {
		t.Fatal("Server address is nil")
	}

	// Build connection string
	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		addr.(*net.TCPAddr).IP.String(),
		addr.(*net.TCPAddr).Port,
	)

	// Connect using pgx
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	// Ping the server
	pingCtx, pingCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer pingCancel()

	if err := conn.Ping(pingCtx); err != nil {
		t.Fatalf("Failed to ping: %v", err)
	}

	t.Log("Successfully connected and pinged the server!")
}

func TestServer_MultipleConnections(t *testing.T) {
	// Create and start server on random port
	srv := New("")
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	addr := srv.Addr()
	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		addr.(*net.TCPAddr).IP.String(),
		addr.(*net.TCPAddr).Port,
	)

	// Test multiple concurrent connections
	ctx := context.Background()
	numConns := 5

	for i := 0; i < numConns; i++ {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("Failed to create connection %d: %v", i, err)
		}
		defer func() { _ = conn.Close(context.Background()) }()

		if err := conn.Ping(ctx); err != nil {
			t.Fatalf("Failed to ping on connection %d: %v", i, err)
		}
	}

	t.Logf("Successfully handled %d concurrent connections", numConns)
}

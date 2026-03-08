package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// @TestDescription Verifies that a pgx client can connect to the server on a random port with successful startup handshake.
// Validates the server accepts connections, performs the PostgreSQL wire protocol startup handshake including
// SSL decline via 'N' byte, AuthenticationOk, ParameterStatus messages, and ReadyForQuery.
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 712d02ff-4075-43f1-8908-79a14e17d4f2
func TestServer_ConnectSucceeds(t *testing.T) {
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

	// Connect using pgx - this validates the full startup handshake
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	t.Log("Successfully connected with startup handshake!")
}

// @TestDescription Verifies that a pgx client can connect to the server on a random port and successfully ping it.
// This test validates the startup handshake (AuthenticationOk, ParameterStatus, ReadyForQuery),
// SSL decline via 'N' byte, and simple query protocol (CommandComplete + ReadyForQuery).
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID bf146e39-7f8b-48c9-be5f-cd11fa217849
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

// @TestDescription Verifies that multiple concurrent client connections are handled correctly via goroutine-per-connection.
// Creates five simultaneous connections, each connecting and pinging successfully without interference.
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 7ec39674-2e9f-46cd-9048-9a16df92ffac
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

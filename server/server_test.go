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

// @test Server returns PostgreSQL error for malformed SQL
func TestServer_MalformedSQL(t *testing.T) {
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

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	// Execute malformed SQL
	_, err = conn.Exec(ctx, "SELECT * FROM") // Incomplete statement
	if err == nil {
		t.Fatal("Expected error for malformed SQL, got nil")
	}

	t.Logf("Got expected error for malformed SQL: %v", err)
}

// @test Server executes CREATE TABLE and DROP TABLE statements
func TestServer_CreateAndDropTable(t *testing.T) {
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

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	// Execute CREATE TABLE - should now succeed
	_, err = conn.Exec(ctx, "CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE should succeed, got error: %v", err)
	}

	t.Log("Successfully created table")

	// Execute DROP TABLE - should succeed
	_, err = conn.Exec(ctx, "DROP TABLE test")
	if err != nil {
		t.Fatalf("DROP TABLE should succeed, got error: %v", err)
	}

	t.Log("Successfully dropped table")

	// Test IF NOT EXISTS - should not error on duplicate
	_, err = conn.Exec(ctx, "CREATE TABLE test2 (id INT)")
	if err != nil {
		t.Fatalf("First CREATE TABLE should succeed: %v", err)
	}

	_, err = conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS test2 (id INT)")
	if err != nil {
		t.Fatalf(
			"CREATE TABLE IF NOT EXISTS should not error on duplicate: %v",
			err,
		)
	}

	t.Log("CREATE TABLE IF NOT EXISTS handled correctly")

	// Test IF EXISTS - should not error on missing table
	_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS nonexistent_table")
	if err != nil {
		t.Fatalf(
			"DROP TABLE IF EXISTS should not error on missing table: %v",
			err,
		)
	}

	t.Log("DROP TABLE IF EXISTS handled correctly")

	// Clean up
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test2")
}

// @TestDescription INSERT rows then SELECT them back with correct values
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 5919cc1e-bc72-4a39-afd8-8a8560dee1c8
func TestServer_InsertThenSelectBack(t *testing.T) {
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

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	// Create a table with INT and TEXT columns (FLOAT8 support is limited)
	_, err = conn.Exec(ctx, "CREATE TABLE test_roundtrip (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert several rows
	_, err = conn.Exec(
		ctx,
		"INSERT INTO test_roundtrip (id, name) VALUES (1, 'Alice')",
	)
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}
	_, err = conn.Exec(
		ctx,
		"INSERT INTO test_roundtrip (id, name) VALUES (2, 'Bob')",
	)
	if err != nil {
		t.Fatalf("Second INSERT failed: %v", err)
	}
	_, err = conn.Exec(
		ctx,
		"INSERT INTO test_roundtrip (id, name) VALUES (3, 'Charlie')",
	)
	if err != nil {
		t.Fatalf("Third INSERT failed: %v", err)
	}

	// SELECT the rows back using simple protocol (server doesn't support extended protocol)
	rows, err := conn.Query(
		ctx,
		"SELECT id, name FROM test_roundtrip ORDER BY id",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	expectedData := []struct {
		id   int32
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	rowCount := 0
	for rows.Next() {
		var id int32
		var name string

		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan row %d: %v", rowCount, err)
		}

		if rowCount >= len(expectedData) {
			t.Fatalf("Unexpected extra row at index %d", rowCount)
		}

		expected := expectedData[rowCount]
		if id != expected.id {
			t.Fatalf(
				"Row %d: expected id=%d, got %d",
				rowCount,
				expected.id,
				id,
			)
		}
		if name != expected.name {
			t.Fatalf(
				"Row %d: expected name='%s', got '%s'",
				rowCount,
				expected.name,
				name,
			)
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}

	if rowCount != 3 {
		t.Fatalf("Expected 3 rows, got %d", rowCount)
	}

	t.Log("Successfully inserted and retrieved rows with correct values")

	// Clean up
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_roundtrip")
}

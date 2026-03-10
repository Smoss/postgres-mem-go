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

// @TestDescription Empty SELECT returns RowDescription without DataRow
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID ad0fad93-c961-4db6-b59d-44fa6817316a
func TestServer_EmptySelectReturnsRowDescription(t *testing.T) {
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

	_, err = conn.Exec(ctx, "CREATE TABLE empty_test (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS empty_test") }()

	// SELECT with no matching rows - should return RowDescription + CommandComplete, no DataRow
	rows, err := conn.Query(
		ctx,
		"SELECT id, name FROM empty_test WHERE 1=0",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	// Verify we received column metadata (RowDescription)
	fds := rows.FieldDescriptions()
	if len(fds) != 2 {
		t.Fatalf("Expected 2 column descriptions, got %d", len(fds))
	}
	if string(fds[0].Name) != "id" || string(fds[1].Name) != "name" {
		t.Fatalf(
			"Expected columns id, name; got %s, %s",
			fds[0].Name,
			fds[1].Name,
		)
	}

	// Verify 0 rows
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if rowCount != 0 {
		t.Fatalf("Expected 0 rows, got %d", rowCount)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
}

// @TestDescription Start transaction, insert data, rollback, then query to verify the inserted data was not persisted.
// @TestType Integration
// @SystemName postgres-mem-go
// @TestID 4cb93ceb-14f0-4822-8772-95b0a1898ce4
func TestServer_BeginInsertRollbackDiscardsData(t *testing.T) {
	srv := New("")
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		srv.Addr().(*net.TCPAddr).IP.String(),
		srv.Addr().(*net.TCPAddr).Port,
	)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "CREATE TABLE rollback_test (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS rollback_test") }()

	_, err = conn.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO rollback_test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = conn.Exec(ctx, "ROLLBACK")
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	rows, err := conn.Query(
		ctx,
		"SELECT id FROM rollback_test",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if rowCount != 0 {
		t.Fatalf("Expected 0 rows after ROLLBACK, got %d", rowCount)
	}
}

// @TestDescription Start transaction, insert data, commit, then query in a new connection/transaction to verify data was persisted.
// @TestType Integration
// @SystemName postgres-mem-go
// @TestID ab15088f-1aa4-4963-8a56-6ff158d838ec
func TestServer_BeginInsertCommitPersistsData(t *testing.T) {
	srv := New("")
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		srv.Addr().(*net.TCPAddr).IP.String(),
		srv.Addr().(*net.TCPAddr).Port,
	)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "CREATE TABLE commit_test (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS commit_test") }()

	_, err = conn.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = conn.Exec(ctx, "INSERT INTO commit_test (id) VALUES (42)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = conn.Exec(ctx, "COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// New connection to verify data was persisted
	conn2, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn2.Close(ctx) }()

	rows, err := conn2.Query(
		ctx,
		"SELECT id FROM commit_test",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	var id int32
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if id != 42 {
			t.Fatalf("Expected id=42, got %d", id)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Fatalf("Expected 1 row after COMMIT, got %d", rowCount)
	}
}

// @TestDescription Execute INSERT without explicit transaction, then query in a separate connection to verify data was auto-committed.
// @TestType Integration
// @SystemName postgres-mem-go
// @TestID d557510c-edb8-419f-b6a1-20d202891605
func TestServer_StatementWithoutBeginAutoCommits(t *testing.T) {
	srv := New("")
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		srv.Addr().(*net.TCPAddr).IP.String(),
		srv.Addr().(*net.TCPAddr).Port,
	)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "CREATE TABLE autocommit_test (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS autocommit_test") }()

	// No BEGIN - INSERT should auto-commit
	_, err = conn.Exec(ctx, "INSERT INTO autocommit_test (id) VALUES (99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Separate connection should see the data
	conn2, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn2.Close(ctx) }()

	rows, err := conn2.Query(
		ctx,
		"SELECT id FROM autocommit_test",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if rowCount != 1 {
		t.Fatalf("Expected 1 row (autocommit), got %d", rowCount)
	}
}

// @TestDescription Connection A starts transaction and inserts data. Connection B queries the same table and should not see the uncommitted data. Connection A commits. Connection B (new transaction) should now see the data.
// @TestType Integration
// @SystemName postgres-mem-go
// @TestID 11054f07-c33d-4e6a-8017-5504ecf18261
func TestServer_TwoConcurrentConnectionsWithTransactionsDoNotSeeUncommittedData(
	t *testing.T,
) {
	srv := New("")
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	connStr := fmt.Sprintf(
		"host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		srv.Addr().(*net.TCPAddr).IP.String(),
		srv.Addr().(*net.TCPAddr).Port,
	)

	ctx := context.Background()

	connA, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Connection A failed: %v", err)
	}
	defer func() { _ = connA.Close(ctx) }()

	connB, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Connection B failed: %v", err)
	}
	defer func() { _ = connB.Close(ctx) }()

	_, err = connA.Exec(ctx, "CREATE TABLE isolation_test (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	defer func() { _, _ = connA.Exec(ctx, "DROP TABLE IF EXISTS isolation_test") }()

	// Connection A: BEGIN, INSERT (uncommitted)
	_, err = connA.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = connA.Exec(ctx, "INSERT INTO isolation_test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Connection B: SELECT - should NOT see uncommitted data
	rows, err := connB.Query(
		ctx,
		"SELECT id FROM isolation_test",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	if rowCount != 0 {
		t.Fatalf(
			"Connection B should not see uncommitted data, got %d rows",
			rowCount,
		)
	}

	// Connection A: COMMIT
	_, err = connA.Exec(ctx, "COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Connection B: SELECT again - should now see the data
	rows, err = connB.Query(
		ctx,
		"SELECT id FROM isolation_test",
		pgx.QueryExecModeSimpleProtocol,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	rowCount = 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	if rowCount != 1 {
		t.Fatalf("Connection B should see 1 row after commit, got %d", rowCount)
	}
}

package engine

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
	"github.com/smoss/postgres-mem-go/parser"
)

// @TestDescription Engine executes SELECT statements and returns results with proper column metadata
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 86038a1d-948d-4472-ad70-f5390203fce8
func TestEngineDispatchSelect(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// First create a table
	createStmt, err := parser.Parse("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse CREATE: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       createStmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("Failed to create table: %v", resp.Error)
	}

	// Insert a row
	insertStmt, err := parser.Parse("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	respCh = make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       insertStmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp = <-respCh
	if resp.Error != nil {
		t.Fatalf("Failed to insert row: %v", resp.Error)
	}

	// Now test SELECT
	stmt, err := parser.Parse("SELECT id FROM test")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh = make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp = <-respCh
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	if len(resp.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(resp.Rows))
	}

	if len(resp.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(resp.Columns))
	}

	if resp.Columns[0].Name != "id" {
		t.Fatalf("Expected column name 'id', got '%s'", resp.Columns[0].Name)
	}

	if resp.Columns[0].TypeOID != uint32(oid.T_int8) {
		t.Fatalf(
			"Expected column type OID %d, got %d",
			oid.T_int4,
			resp.Columns[0].TypeOID,
		)
	}
	// type assert to int64
	rowValue, ok := resp.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected row value to be int64, got %T", resp.Rows[0][0])
	}
	if rowValue != 1 {
		t.Fatalf("Expected row value %d, got %d", int32(1), resp.Rows[0][0])
	}
}

// @TestDescription Engine executes INSERT statements and stores rows in the catalog
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID e501dfb5-1d58-44fd-af6e-76eb6d68d948
func TestEngineDispatchInsert(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// First create a table
	createStmt, err := parser.Parse("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse CREATE: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       createStmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("Failed to create table: %v", resp.Error)
	}

	// Test INSERT
	stmt, err := parser.Parse("INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh = make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp = <-respCh
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	if resp.RowsAffected != 1 {
		t.Fatalf("Expected 1 row affected, got %d", resp.RowsAffected)
	}

	// Verify the row was inserted
	table, _ := eng.catalog.GetTable("test")
	if len(table.Rows) != 1 {
		t.Fatalf("Expected 1 row in table, got %d", len(table.Rows))
	}
}

// @TestDescription Engine executes UPDATE statements and modifies rows, returning affected row count
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 42f56844-00dc-4cf1-83e4-bada38f91575
func TestEngineDispatchUpdate(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// First create a table and insert a row
	createStmt, _ := parser.Parse("CREATE TABLE test (id INT, name TEXT)")
	respCh := make(chan Response, 1)
	eng.Submit(Request{Stmt: createStmt, ConnID: 1, ResponseCh: respCh})
	<-respCh

	insertStmt, _ := parser.Parse(
		"INSERT INTO test (id, name) VALUES (1, 'Alice')",
	)
	respCh = make(chan Response, 1)
	eng.Submit(Request{Stmt: insertStmt, ConnID: 1, ResponseCh: respCh})
	<-respCh

	// Test UPDATE
	stmt, err := parser.Parse("UPDATE test SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh = make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	if resp.RowsAffected != 1 {
		t.Fatalf("Expected 1 row affected, got %d", resp.RowsAffected)
	}
}

// @TestDescription Engine executes DELETE statements and removes rows, returning affected row count
// @TestType integration
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 058cc984-6831-43d8-936b-bca89a7a1171
func TestEngineDispatchDelete(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// First create a table and insert a row
	createStmt, _ := parser.Parse("CREATE TABLE test (id INT)")
	respCh := make(chan Response, 1)
	eng.Submit(Request{Stmt: createStmt, ConnID: 1, ResponseCh: respCh})
	<-respCh

	insertStmt, _ := parser.Parse("INSERT INTO test (id) VALUES (1)")
	respCh = make(chan Response, 1)
	eng.Submit(Request{Stmt: insertStmt, ConnID: 1, ResponseCh: respCh})
	<-respCh

	// Test DELETE
	stmt, err := parser.Parse("DELETE FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh = make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}

	if resp.RowsAffected != 1 {
		t.Fatalf("Expected 1 row affected, got %d", resp.RowsAffected)
	}
}

// @test Engine dispatches CREATE TABLE statements to the DDL executor
func TestEngineDispatchCreateTable(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	// CREATE TABLE is now implemented - should succeed
	if resp.Error != nil {
		t.Fatalf("Expected success for CREATE TABLE, got error: %v", resp.Error)
	}

	// Verify table was created in catalog
	if !eng.catalog.TableExists("test") {
		t.Fatal("Expected table to be created in catalog")
	}
}

// @test Engine dispatches DROP TABLE statements to the DDL executor
func TestEngineDispatchDropTable(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// First create a table
	createStmt, err := parser.Parse("CREATE TABLE test (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse CREATE: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       createStmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("Failed to create table: %v", resp.Error)
	}

	// Verify table exists
	if !eng.catalog.TableExists("test") {
		t.Fatal("Expected table to be created")
	}

	// Now drop the table
	stmt, err := parser.Parse("DROP TABLE test")
	if err != nil {
		t.Fatalf("Failed to parse DROP: %v", err)
	}

	respCh = make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp = <-respCh
	// DROP TABLE is now implemented - should succeed
	if resp.Error != nil {
		t.Fatalf("Expected success for DROP TABLE, got error: %v", resp.Error)
	}

	// Verify table was dropped
	if eng.catalog.TableExists("test") {
		t.Fatal("Expected table to be dropped from catalog")
	}
}

// @test Engine dispatches BEGIN statements to the transaction executor
func TestEngineDispatchBegin(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("BEGIN")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("Expected success for BEGIN, got error: %v", resp.Error)
	}
	if resp.TxStatus != 'T' {
		t.Fatalf("Expected TxStatus 'T' after BEGIN, got %c", resp.TxStatus)
	}
}

// @test Engine dispatches COMMIT statements to the transaction executor
func TestEngineDispatchCommit(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("COMMIT")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf(
			"Expected success for COMMIT (when not in transaction), got error: %v",
			resp.Error,
		)
	}
	if resp.TxStatus != 'I' {
		t.Fatalf("Expected TxStatus 'I' after COMMIT, got %c", resp.TxStatus)
	}
}

// @test Engine dispatches ROLLBACK statements to the transaction executor
func TestEngineDispatchRollback(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("ROLLBACK")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf(
			"Expected success for ROLLBACK (when not in transaction), got error: %v",
			resp.Error,
		)
	}
	if resp.TxStatus != 'I' {
		t.Fatalf("Expected TxStatus 'I' after ROLLBACK, got %c", resp.TxStatus)
	}
}

// @TestDescription Unit test: Send multiple requests to engine goroutine and verify they are processed sequentially with correct responses returned on their respective channels.
// @TestType Unit
// @SystemName postgres-mem-go
// @TestID 2a39125d-1f48-4a60-ad78-68fe55c6a4a0
func TestEngine_ProcessesRequestsSerially(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// Create table first
	createStmt, _ := parser.Parse("CREATE TABLE ser (id INT)")
	respCh := make(chan Response, 1)
	eng.Submit(Request{Stmt: createStmt, ConnID: 1, ResponseCh: respCh})
	if (<-respCh).Error != nil {
		t.Fatal("CREATE TABLE failed")
	}

	// Send multiple requests from different goroutines, verify sequential processing
	done := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func(connID uint64) {
			insertStmt, _ := parser.Parse("INSERT INTO ser (id) VALUES (1)")
			respCh := make(chan Response, 1)
			eng.Submit(
				Request{Stmt: insertStmt, ConnID: connID, ResponseCh: respCh},
			)
			resp := <-respCh
			if resp.Error != nil {
				t.Errorf("Connection %d: got error %v", connID, resp.Error)
			}
			if resp.RowsAffected != 1 {
				t.Errorf(
					"Connection %d: expected RowsAffected 1, got %d",
					connID,
					resp.RowsAffected,
				)
			}
			done <- true
		}(uint64(i + 1))
	}

	for i := 0; i < 3; i++ {
		<-done
	}

	// Verify all 3 rows were inserted
	selectStmt, _ := parser.Parse("SELECT id FROM ser")
	respCh = make(chan Response, 1)
	eng.Submit(Request{Stmt: selectStmt, ConnID: 1, ResponseCh: respCh})
	resp := <-respCh
	if resp.Error != nil {
		t.Fatalf("SELECT failed: %v", resp.Error)
	}
	if len(resp.Rows) != 3 {
		t.Fatalf("Expected 3 rows (serial processing), got %d", len(resp.Rows))
	}
}

// @test Engine returns error for unsupported statement types
func TestEngineUnsupportedStatement(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	// Use a statement type we don't handle (e.g., Grant)
	stmt := &tree.Grant{}

	respCh := make(chan Response, 1)
	eng.Submit(Request{
		Stmt:       stmt,
		ConnID:     1,
		ResponseCh: respCh,
	})

	resp := <-respCh
	if resp.Error == nil {
		t.Fatal("Expected error for unsupported statement")
	}
	expected := "unsupported statement type: *tree.Grant"
	if resp.Error.Error() != expected {
		t.Fatalf("Expected error %q, got %q", expected, resp.Error.Error())
	}
}

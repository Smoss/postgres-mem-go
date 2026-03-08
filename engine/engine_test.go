package engine

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/smoss/postgres-mem-go/parser"
)

// @test Engine dispatches SELECT statements to the SELECT executor
func TestEngineDispatchSelect(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("SELECT 1")
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
	// For Phase 2, SELECT returns "not yet implemented" error
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented SELECT")
	}
	if resp.Error.Error() != "SELECT not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}
}

// @test Engine dispatches INSERT statements to the INSERT executor
func TestEngineDispatchInsert(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("INSERT INTO test (id) VALUES (1)")
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented INSERT")
	}
	if resp.Error.Error() != "INSERT not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}
}

// @test Engine dispatches UPDATE statements to the UPDATE executor
func TestEngineDispatchUpdate(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("UPDATE test SET id = 2")
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented UPDATE")
	}
	if resp.Error.Error() != "UPDATE not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}
}

// @test Engine dispatches DELETE statements to the DELETE executor
func TestEngineDispatchDelete(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("DELETE FROM test")
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented DELETE")
	}
	if resp.Error.Error() != "DELETE not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented BEGIN")
	}
	if resp.Error.Error() != "BEGIN TRANSACTION not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented COMMIT")
	}
	if resp.Error.Error() != "COMMIT not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented ROLLBACK")
	}
	if resp.Error.Error() != "ROLLBACK not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
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

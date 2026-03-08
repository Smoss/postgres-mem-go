package engine

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/smoss/postgres-mem-go/parser"
)

// @TestDescription Verifies engine type-switch correctly dispatches each AST type to its designated
// executor function (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, BEGIN, COMMIT, ROLLBACK).
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID b732a74c-5c52-4f15-a9a8-a76579a991ca
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

// @TestDescription Verifies the Engine correctly dispatches INSERT statements via the Request channel
// to the executeInsert handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 463c902a-e6a3-4394-adb7-95d2252cdad2
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

// @TestDescription Verifies the Engine correctly dispatches UPDATE statements via the Request channel
// to the executeUpdate handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID f3a3dcbf-f644-4936-8cef-97f008fa4cb7
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

// @TestDescription Verifies the Engine correctly dispatches DELETE statements via the Request channel
// to the executeDelete handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 3c379244-0006-4866-9574-302b5a9a8170
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

// @TestDescription Verifies the Engine correctly dispatches CREATE TABLE statements via the Request channel
// to the executeCreateTable handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 0960f164-2c45-415f-8247-a5423508b250
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
	if resp.Error == nil {
		t.Fatal("Expected error for unimplemented CREATE TABLE")
	}
	if resp.Error.Error() != "CREATE TABLE not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}
}

// @TestDescription Verifies the Engine correctly dispatches DROP TABLE statements via the Request channel
// to the executeDropTable handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID e304ec2f-0236-471b-af42-b518dcd8de8c
func TestEngineDispatchDropTable(t *testing.T) {
	eng := New()
	eng.Start()
	defer eng.Stop()

	stmt, err := parser.Parse("DROP TABLE test")
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
		t.Fatal("Expected error for unimplemented DROP TABLE")
	}
	if resp.Error.Error() != "DROP TABLE not yet implemented" {
		t.Fatalf("Unexpected error: %v", resp.Error)
	}
}

// @TestDescription Verifies the Engine correctly dispatches BEGIN TRANSACTION statements via the Request channel
// to the executeBegin handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 0f29becf-b058-4bb0-bcc4-583071ea4558
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

// @TestDescription Verifies the Engine correctly dispatches COMMIT statements via the Request channel
// to the executeCommit handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 33217f30-0708-429e-b2c7-14a84c82e5bf
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

// @TestDescription Verifies the Engine correctly dispatches ROLLBACK statements via the Request channel
// to the executeRollback handler function and returns the expected response through the Response channel.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID c9f1e5ef-e667-4bbf-8ca6-1cdf07f6ee45
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

// @TestDescription Verifies the Engine dispatch function returns a clear error message indicating the
// statement type is not supported when receiving an unhandled AST node type like tree.Grant.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID a930058c-560e-4f5f-b36b-eada6a34d065
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

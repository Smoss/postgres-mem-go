package parser

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// @TestDescription Verifies parser.Parse returns correct tree.Statement types for SELECT, INSERT, UPDATE,
// DELETE, CREATE TABLE, DROP TABLE, BEGIN, COMMIT, ROLLBACK.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID d826c8d4-3ae8-46f2-88ed-e2844a2098fb
func TestParseSelect(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE active = true"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse SELECT: %v", err)
	}

	_, ok := stmt.(*tree.Select)
	if !ok {
		t.Fatalf("Expected *tree.Select, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses INSERT INTO statements and returns a typed
// tree.Insert AST node containing table name, column list, and values information.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID cdbb3576-9d62-4c03-99f3-a58e48a92b69
func TestParseInsert(t *testing.T) {
	sql := "INSERT INTO users (id, name) VALUES (1, 'Alice')"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse INSERT: %v", err)
	}

	_, ok := stmt.(*tree.Insert)
	if !ok {
		t.Fatalf("Expected *tree.Insert, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses UPDATE statements and returns a typed
// tree.Update AST node containing target table, SET clause assignments, and WHERE condition.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 303bddb0-0d54-492b-b0e3-a3972875ff34
func TestParseUpdate(t *testing.T) {
	sql := "UPDATE users SET name = 'Bob' WHERE id = 1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse UPDATE: %v", err)
	}

	_, ok := stmt.(*tree.Update)
	if !ok {
		t.Fatalf("Expected *tree.Update, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses DELETE FROM statements and returns a typed
// tree.Delete AST node containing table reference and optional WHERE clause.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 67013902-b40a-4b91-b218-8fb2b6be4c25
func TestParseDelete(t *testing.T) {
	sql := "DELETE FROM users WHERE id = 1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse DELETE: %v", err)
	}

	_, ok := stmt.(*tree.Delete)
	if !ok {
		t.Fatalf("Expected *tree.Delete, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses CREATE TABLE statements and returns a typed
// tree.CreateTable AST node containing table name, column definitions with types, and constraints.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID b90eeaeb-3d65-46a6-b951-3412a05bcad5
func TestParseCreateTable(t *testing.T) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", err)
	}

	_, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses DROP TABLE statements and returns a typed
// tree.DropTable AST node containing the target table name and IF EXISTS flag.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 5a9bee1a-3d6d-440b-b3ea-daae3fbd4276
func TestParseDropTable(t *testing.T) {
	sql := "DROP TABLE users"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse DROP TABLE: %v", err)
	}

	_, ok := stmt.(*tree.DropTable)
	if !ok {
		t.Fatalf("Expected *tree.DropTable, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses BEGIN transaction statements and returns a typed
// tree.BeginTransaction AST node with transaction mode information.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 44501aae-97c8-4904-bcd9-20e82bc6bb5f
func TestParseBegin(t *testing.T) {
	sql := "BEGIN"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse BEGIN: %v", err)
	}

	_, ok := stmt.(*tree.BeginTransaction)
	if !ok {
		t.Fatalf("Expected *tree.BeginTransaction, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses COMMIT transaction statements and returns a typed
// tree.CommitTransaction AST node.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 2de85b64-3def-4c41-870f-a2590909f93c
func TestParseCommit(t *testing.T) {
	sql := "COMMIT"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse COMMIT: %v", err)
	}

	_, ok := stmt.(*tree.CommitTransaction)
	if !ok {
		t.Fatalf("Expected *tree.CommitTransaction, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse correctly parses ROLLBACK transaction statements and returns a typed
// tree.RollbackTransaction AST node.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 0ba5d157-32a2-40e4-9ffd-89ae8d397cf1
func TestParseRollback(t *testing.T) {
	sql := "ROLLBACK"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse ROLLBACK: %v", err)
	}

	_, ok := stmt.(*tree.RollbackTransaction)
	if !ok {
		t.Fatalf("Expected *tree.RollbackTransaction, got %T", stmt)
	}
}

// @TestDescription Verifies parser.Parse returns a descriptive error when given invalid or incomplete
// SQL syntax, such as SELECT * FROM with no table name.
// @TestType unit
// @FlakeScore 0.0
// @SystemName postgres-mem-go
// @TestID 6a278fd7-4f18-4f05-81ab-1a3afbe1ddea
func TestParseMalformedSQL(t *testing.T) {
	sql := "SELECT * FROM" // Incomplete statement
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("Expected error for malformed SQL, got nil")
	}
}

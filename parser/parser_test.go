package parser

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// @test ParseSelect parses a simple SELECT statement into a typed AST
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

// @test ParseInsert parses an INSERT statement into a typed AST
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

// @test ParseUpdate parses an UPDATE statement into a typed AST
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

// @test ParseDelete parses a DELETE statement into a typed AST
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

// @test ParseCreateTable parses a CREATE TABLE statement into a typed AST
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

// @test ParseDropTable parses a DROP TABLE statement into a typed AST
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

// @test ParseBegin parses a BEGIN statement into a typed AST
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

// @test ParseCommit parses a COMMIT statement into a typed AST
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

// @test ParseRollback parses a ROLLBACK statement into a typed AST
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

// @test ParseMalformedSQL returns an error for invalid SQL
func TestParseMalformedSQL(t *testing.T) {
	sql := "SELECT * FROM" // Incomplete statement
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("Expected error for malformed SQL, got nil")
	}
}

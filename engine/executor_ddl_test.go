package engine

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/smoss/postgres-mem-go/parser"
)

// @test ExecuteCreateTable creates a table in the catalog
func TestExecuteCreateTable(t *testing.T) {
	catalog := NewCatalog()

	stmt, err := parser.Parse(
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
	)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	if !catalog.TableExists("users") {
		t.Fatal("Expected table to exist in catalog")
	}

	table, exists := catalog.GetTable("users")
	if !exists {
		t.Fatal("Expected to retrieve table from catalog")
	}

	if len(table.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(table.Columns))
	}
}

// @test ExecuteCreateTableIfNotExists does not error on duplicate
func TestExecuteCreateTableIfNotExists(t *testing.T) {
	catalog := NewCatalog()

	// Create table first
	stmt, err := parser.Parse("CREATE TABLE users (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error on first create, got %v", resp.Error)
	}

	// Create with IF NOT EXISTS - should not error
	stmt2, err := parser.Parse("CREATE TABLE IF NOT EXISTS users (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt2, ok := stmt2.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt2)
	}

	resp = executeCreateTable(createStmt2, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error with IF NOT EXISTS, got %v", resp.Error)
	}
}

// @test ExecuteCreateTableDuplicateError returns error on duplicate
func TestExecuteCreateTableDuplicateError(t *testing.T) {
	catalog := NewCatalog()

	// Create table first
	stmt, err := parser.Parse("CREATE TABLE users (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error on first create, got %v", resp.Error)
	}

	// Create again without IF NOT EXISTS - should error
	stmt2, err := parser.Parse("CREATE TABLE users (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt2, ok := stmt2.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt2)
	}

	resp = executeCreateTable(createStmt2, catalog)
	if resp.Error == nil {
		t.Fatal("Expected error for duplicate table")
	}
}

// @test ExecuteCreateTableWithConstraints records NOT NULL and DEFAULT
func TestExecuteCreateTableWithConstraints(t *testing.T) {
	catalog := NewCatalog()

	stmt, err := parser.Parse(
		"CREATE TABLE users (id INT NOT NULL, name TEXT DEFAULT 'unknown')",
	)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	table, exists := catalog.GetTable("users")
	if !exists {
		t.Fatal("Expected table to exist")
	}

	if len(table.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(table.Columns))
	}

	// Check NOT NULL on id column
	idCol := table.Columns[0]
	if idCol.Name != "id" {
		t.Fatalf("Expected first column to be 'id', got %s", idCol.Name)
	}
	if !idCol.NotNull {
		t.Fatal("Expected id column to have NOT NULL constraint")
	}

	// Check DEFAULT on name column
	nameCol := table.Columns[1]
	if nameCol.Name != "name" {
		t.Fatalf("Expected second column to be 'name', got %s", nameCol.Name)
	}
	if nameCol.DefaultExpr != "'unknown'" {
		t.Fatalf(
			"Expected default expression ''unknown'', got %s",
			nameCol.DefaultExpr,
		)
	}
}

// @test ExecuteCreateTableWithPrimaryKey records PRIMARY KEY constraint
func TestExecuteCreateTableWithPrimaryKey(t *testing.T) {
	catalog := NewCatalog()

	// Test inline PRIMARY KEY
	stmt, err := parser.Parse(
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
	)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	table, exists := catalog.GetTable("users")
	if !exists {
		t.Fatal("Expected table to exist")
	}

	if len(table.PrimaryKey) != 1 || table.PrimaryKey[0] != "id" {
		t.Fatalf("Expected primary key ['id'], got %v", table.PrimaryKey)
	}

	idCol := table.Columns[0]
	if !idCol.IsPrimaryKey {
		t.Fatal("Expected id column to be marked as primary key")
	}
}

// @test ExecuteCreateTableWithSeparatePrimaryKey records PRIMARY KEY table constraint
func TestExecuteCreateTableWithSeparatePrimaryKey(t *testing.T) {
	catalog := NewCatalog()

	stmt, err := parser.Parse(
		"CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))",
	)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	table, exists := catalog.GetTable("users")
	if !exists {
		t.Fatal("Expected table to exist")
	}

	if len(table.PrimaryKey) != 1 || table.PrimaryKey[0] != "id" {
		t.Fatalf("Expected primary key ['id'], got %v", table.PrimaryKey)
	}

	idCol := table.Columns[0]
	if !idCol.IsPrimaryKey {
		t.Fatal("Expected id column to be marked as primary key")
	}
}

// @test ExecuteCreateTableWithMultipleColumns creates table with multiple columns
func TestExecuteCreateTableWithMultipleColumns(t *testing.T) {
	catalog := NewCatalog()

	stmt, err := parser.Parse(
		"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, active BOOL DEFAULT true)",
	)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := stmt.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	table, exists := catalog.GetTable("users")
	if !exists {
		t.Fatal("Expected table to exist")
	}

	if len(table.Columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(table.Columns))
	}

	// Note: CockroachDB parser normalizes INT to INT8
	expectedColumns := []struct {
		name    string
		goType  string
		notNull bool
	}{
		{"id", "int64", false}, // CockroachDB normalizes INT to INT8
		{"name", "string", true},
		{"email", "string", false},
		{"active", "bool", false},
	}

	for i, expected := range expectedColumns {
		col := table.Columns[i]
		if col.Name != expected.name {
			t.Fatalf(
				"Expected column %d to be '%s', got '%s'",
				i,
				expected.name,
				col.Name,
			)
		}
		if col.GoType != expected.goType {
			t.Fatalf(
				"Expected column %d Go type to be '%s', got '%s'",
				i,
				expected.goType,
				col.GoType,
			)
		}
		if col.NotNull != expected.notNull {
			t.Fatalf(
				"Expected column %d NotNull to be %v, got %v",
				i,
				expected.notNull,
				col.NotNull,
			)
		}
	}
}

// @test ExecuteCreateTableWithTypeAliases resolves type aliases correctly
func TestExecuteCreateTableWithTypeAliases(t *testing.T) {
	// Note: CockroachDB parser normalizes INT/INTEGER to INT8
	testCases := []struct {
		sqlType string
		goType  string
	}{
		{"INTEGER", "int64"}, // CockroachDB normalizes INTEGER to INT8
		{"BIGINT", "int64"},
		{"VARCHAR", "string"},
		{"BOOLEAN", "bool"},
		{"DOUBLE PRECISION", "float64"},
		{"DECIMAL", "string"},
	}

	for _, tc := range testCases {
		catalog := NewCatalog()
		stmt, err := parser.Parse("CREATE TABLE test (col " + tc.sqlType + ")")
		if err != nil {
			t.Fatalf("Failed to parse %s: %v", tc.sqlType, err)
		}

		createStmt, ok := stmt.(*tree.CreateTable)
		if !ok {
			t.Fatalf("Expected *tree.CreateTable, got %T", stmt)
		}

		resp := executeCreateTable(createStmt, catalog)
		if resp.Error != nil {
			t.Fatalf("Expected no error for %s, got %v", tc.sqlType, resp.Error)
		}

		table, exists := catalog.GetTable("test")
		if !exists {
			t.Fatalf("Expected table to exist for %s", tc.sqlType)
		}

		if table.Columns[0].GoType != tc.goType {
			t.Fatalf(
				"Expected %s to map to %s, got %s",
				tc.sqlType,
				tc.goType,
				table.Columns[0].GoType,
			)
		}
	}
}

// @test ExecuteDropTable removes a table from the catalog
func TestExecuteDropTable(t *testing.T) {
	catalog := NewCatalog()

	// Create table first
	createStmtParsed, err := parser.Parse("CREATE TABLE users (id INT)")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	createStmt, ok := createStmtParsed.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", createStmtParsed)
	}

	resp := executeCreateTable(createStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Failed to create table: %v", resp.Error)
	}

	// Drop table
	dropStmtParsed, err := parser.Parse("DROP TABLE users")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	dropStmt, ok := dropStmtParsed.(*tree.DropTable)
	if !ok {
		t.Fatalf("Expected *tree.DropTable, got %T", dropStmtParsed)
	}

	resp = executeDropTable(dropStmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	if catalog.TableExists("users") {
		t.Fatal("Expected table to not exist after drop")
	}
}

// @test ExecuteDropTableIfExists does not error on missing table
func TestExecuteDropTableIfExists(t *testing.T) {
	catalog := NewCatalog()

	// Drop non-existent table with IF EXISTS
	stmtParsed, err := parser.Parse("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	stmt, ok := stmtParsed.(*tree.DropTable)
	if !ok {
		t.Fatalf("Expected *tree.DropTable, got %T", stmtParsed)
	}

	resp := executeDropTable(stmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error with IF EXISTS, got %v", resp.Error)
	}
}

// @test ExecuteDropTableNotFoundError returns error for missing table
func TestExecuteDropTableNotFoundError(t *testing.T) {
	catalog := NewCatalog()

	stmtParsed, err := parser.Parse("DROP TABLE users")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	stmt, ok := stmtParsed.(*tree.DropTable)
	if !ok {
		t.Fatalf("Expected *tree.DropTable, got %T", stmtParsed)
	}

	resp := executeDropTable(stmt, catalog)
	if resp.Error == nil {
		t.Fatal("Expected error for missing table")
	}
}

// @test ExecuteDropTableMultiple drops multiple tables
func TestExecuteDropTableMultiple(t *testing.T) {
	catalog := NewCatalog()

	// Create tables
	createStmt1Parsed, _ := parser.Parse("CREATE TABLE users (id INT)")
	createStmt1, _ := createStmt1Parsed.(*tree.CreateTable)
	executeCreateTable(createStmt1, catalog)

	createStmt2Parsed, _ := parser.Parse("CREATE TABLE products (id INT)")
	createStmt2, _ := createStmt2Parsed.(*tree.CreateTable)
	executeCreateTable(createStmt2, catalog)

	// Verify both exist
	if !catalog.TableExists("users") || !catalog.TableExists("products") {
		t.Fatal("Expected both tables to exist")
	}

	// Drop both
	stmtParsed, err := parser.Parse("DROP TABLE users, products")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	stmt, ok := stmtParsed.(*tree.DropTable)
	if !ok {
		t.Fatalf("Expected *tree.DropTable, got %T", stmtParsed)
	}

	resp := executeDropTable(stmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	if catalog.TableExists("users") || catalog.TableExists("products") {
		t.Fatal("Expected both tables to be dropped")
	}
}

// @test ExecuteCreateTableWithAllPostgreSQLTypes creates table with all supported types
func TestExecuteCreateTableWithAllPostgreSQLTypes(t *testing.T) {
	catalog := NewCatalog()

	// Note: CockroachDB parser normalizes INT -> INT8 and TEXT -> STRING
	// We test the type mappings based on what the parser actually produces
	stmtParsed, err := parser.Parse(`CREATE TABLE all_types (
		col_int INT,
		col_int8 BIGINT,
		col_string TEXT,
		col_bool BOOLEAN,
		col_float8 DOUBLE PRECISION,
		col_numeric NUMERIC,
		col_timestamp TIMESTAMP,
		col_timestamptz TIMESTAMPTZ,
		col_uuid UUID,
		col_bytes BYTEA,
		col_jsonb JSONB
	)`)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	stmt, ok := stmtParsed.(*tree.CreateTable)
	if !ok {
		t.Fatalf("Expected *tree.CreateTable, got %T", stmtParsed)
	}

	resp := executeCreateTable(stmt, catalog)
	if resp.Error != nil {
		t.Fatalf("Expected no error, got %v", resp.Error)
	}

	table, exists := catalog.GetTable("all_types")
	if !exists {
		t.Fatal("Expected table to exist")
	}

	if len(table.Columns) != 11 {
		t.Fatalf("Expected 11 columns, got %d", len(table.Columns))
	}

	// Expected OIDs based on CockroachDB parser normalization
	expectedTypes := map[string]uint32{
		"col_int":         20,   // CockroachDB: INT -> INT8
		"col_int8":        20,   // int8
		"col_string":      25,   // CockroachDB: TEXT -> STRING -> text OID
		"col_bool":        16,   // bool
		"col_float8":      701,  // float8
		"col_numeric":     1700, // numeric
		"col_timestamp":   1114, // timestamp
		"col_timestamptz": 1184, // timestamptz
		"col_uuid":        2950, // uuid
		"col_bytes":       17,   // CockroachDB: BYTEA -> BYTES -> bytea OID
		"col_jsonb":       3802, // jsonb
	}

	for _, col := range table.Columns {
		expectedOID, ok := expectedTypes[col.Name]
		if !ok {
			t.Fatalf("Unexpected column: %s", col.Name)
		}
		if col.TypeOID != expectedOID {
			t.Fatalf(
				"Expected %s to have OID %d, got %d",
				col.Name,
				expectedOID,
				col.TypeOID,
			)
		}
	}
}

package engine

import (
	"testing"
)

// @test Catalog_CreateTable adds a table to the catalog
func TestCatalogCreateTable(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
			{Name: "name", TypeOID: 25, GoType: "string"},
		},
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !catalog.TableExists("users") {
		t.Fatal("Expected table to exist")
	}
}

// @test Catalog_CreateTableDuplicate returns error for duplicate table
func TestCatalogCreateTableDuplicate(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
		},
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("Expected no error on first create, got %v", err)
	}

	err = catalog.CreateTable(table)
	if err == nil {
		t.Fatal("Expected error for duplicate table, got nil")
	}

	if err.Error() != "table already exists: users" {
		t.Fatalf("Expected 'table already exists: users', got %v", err.Error())
	}
}

// @test Catalog_CreateTableIfNotExists creates table only if it doesn't exist
func TestCatalogCreateTableIfNotExists(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
		},
	}

	// First create should succeed
	created, err := catalog.CreateTableIfNotExists(table)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !created {
		t.Fatal("Expected created to be true on first call")
	}

	// Second create should not error but return false
	created, err = catalog.CreateTableIfNotExists(table)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if created {
		t.Fatal("Expected created to be false on second call")
	}
}

// @test Catalog_DropTable removes a table from the catalog
func TestCatalogDropTable(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
		},
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = catalog.DropTable("users")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if catalog.TableExists("users") {
		t.Fatal("Expected table to not exist after drop")
	}
}

// @test Catalog_DropTableNotFound returns error for missing table
func TestCatalogDropTableNotFound(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.DropTable("nonexistent")
	if err == nil {
		t.Fatal("Expected error for missing table, got nil")
	}

	if err.Error() != "table does not exist: nonexistent" {
		t.Fatalf(
			"Expected 'table does not exist: nonexistent', got %v",
			err.Error(),
		)
	}
}

// @test Catalog_DropTableIfExists drops table only if it exists
func TestCatalogDropTableIfExists(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
		},
	}

	// Create table
	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Drop should succeed
	dropped, err := catalog.DropTableIfExists("users")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !dropped {
		t.Fatal("Expected dropped to be true")
	}

	// Second drop should not error but return false
	dropped, err = catalog.DropTableIfExists("users")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if dropped {
		t.Fatal("Expected dropped to be false for nonexistent table")
	}
}

// @test Catalog_GetTable retrieves a table by name
func TestCatalogGetTable(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
			{Name: "name", TypeOID: 25, GoType: "string"},
		},
		PrimaryKey: []string{"id"},
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	retrieved, exists := catalog.GetTable("users")
	if !exists {
		t.Fatal("Expected table to exist")
	}

	if retrieved.Name != "users" {
		t.Fatalf("Expected table name 'users', got %s", retrieved.Name)
	}

	if len(retrieved.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(retrieved.Columns))
	}

	if len(retrieved.PrimaryKey) != 1 || retrieved.PrimaryKey[0] != "id" {
		t.Fatalf("Expected primary key ['id'], got %v", retrieved.PrimaryKey)
	}
}

// @test Catalog_GetTableMissing returns false for missing table
func TestCatalogGetTableMissing(t *testing.T) {
	catalog := NewCatalog()

	_, exists := catalog.GetTable("nonexistent")
	if exists {
		t.Fatal("Expected exists to be false for missing table")
	}
}

// @test Catalog_TableExists checks if a table exists
func TestCatalogTableExists(t *testing.T) {
	catalog := NewCatalog()

	table := &Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", TypeOID: 23, GoType: "int32"},
		},
	}

	if catalog.TableExists("users") {
		t.Fatal("Expected table to not exist initially")
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if !catalog.TableExists("users") {
		t.Fatal("Expected table to exist after creation")
	}
}

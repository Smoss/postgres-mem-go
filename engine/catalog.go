// Package engine provides the SQL execution engine with request/response dispatch.
package engine

import (
	"fmt"
	"sync"
)

// Table represents a table in the catalog.
type Table struct {
	Name       string
	Columns    []TableColumn
	PrimaryKey []string
	Rows       [][]interface{} // In-memory row storage
}

// TableColumn represents a column in a table.
type TableColumn struct {
	Name         string
	TypeOID      uint32
	GoType       string
	NotNull      bool
	DefaultExpr  string
	IsPrimaryKey bool
}

// Catalog tracks all tables and their metadata in-memory.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// NewCatalog creates a new empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		tables: make(map[string]*Table),
	}
}

// CreateTable adds a table to the catalog. Returns error if table exists.
func (c *Catalog) CreateTable(table *Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[table.Name]; exists {
		return fmt.Errorf("table already exists: %s", table.Name)
	}

	c.tables[table.Name] = table
	return nil
}

// CreateTableIfNotExists adds a table only if it doesn't exist.
// Returns true if the table was created, false if it already existed.
func (c *Catalog) CreateTableIfNotExists(table *Table) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[table.Name]; exists {
		return false, nil
	}

	c.tables[table.Name] = table
	return true, nil
}

// DropTable removes a table from the catalog. Returns error if not found.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return fmt.Errorf("table does not exist: %s", name)
	}

	delete(c.tables, name)
	return nil
}

// DropTableIfExists removes a table if it exists.
// Returns true if the table was dropped, false if it didn't exist.
func (c *Catalog) DropTableIfExists(name string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return false, nil
	}

	delete(c.tables, name)
	return true, nil
}

// GetTable retrieves a table by name.
// Returns the table and a boolean indicating if it was found.
func (c *Catalog) GetTable(name string) (*Table, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	table, exists := c.tables[name]
	return table, exists
}

// TableExists checks if a table exists.
func (c *Catalog) TableExists(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.tables[name]
	return exists
}

// InsertRow inserts a row into a table.
// The values slice must match the number of columns in the table.
func (c *Catalog) InsertRow(tableName string, values []interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[tableName]
	if !exists {
		return fmt.Errorf("table does not exist: %s", tableName)
	}

	if len(values) != len(table.Columns) {
		return fmt.Errorf(
			"value count mismatch: expected %d, got %d",
			len(table.Columns),
			len(values),
		)
	}

	table.Rows = append(table.Rows, values)
	return nil
}

// GetAllRows returns all rows from a table.
func (c *Catalog) GetAllRows(tableName string) ([][]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	table, exists := c.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table does not exist: %s", tableName)
	}

	// Return a copy to prevent external modification
	result := make([][]interface{}, len(table.Rows))
	for i, row := range table.Rows {
		result[i] = make([]interface{}, len(row))
		copy(result[i], row)
	}
	return result, nil
}

// DeleteRows removes rows that match the predicate.
// Returns the number of rows deleted.
func (c *Catalog) DeleteRows(
	tableName string,
	predicate func([]interface{}) bool,
) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table does not exist: %s", tableName)
	}

	var newRows [][]interface{}
	deletedCount := 0

	for _, row := range table.Rows {
		if predicate(row) {
			deletedCount++
		} else {
			newRows = append(newRows, row)
		}
	}

	table.Rows = newRows
	return deletedCount, nil
}

// UpdateRows modifies rows that match the predicate using the updater function.
// Returns the number of rows updated.
func (c *Catalog) UpdateRows(
	tableName string,
	predicate func([]interface{}) bool,
	updater func([]interface{}) []interface{},
) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, exists := c.tables[tableName]
	if !exists {
		return 0, fmt.Errorf("table does not exist: %s", tableName)
	}

	updatedCount := 0

	for i, row := range table.Rows {
		if predicate(row) {
			table.Rows[i] = updater(row)
			updatedCount++
		}
	}

	return updatedCount, nil
}

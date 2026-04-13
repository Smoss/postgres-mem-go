package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeCreateTable handles CREATE TABLE statements.
func executeCreateTable(stmt *tree.CreateTable, catalog *Catalog) Response {
	// Extract table name - Table is tree.TableName with a String() method
	tableName := stmt.Table.String()

	// Build the table metadata
	table := &Table{
		Name:    tableName,
		Columns: make([]TableColumn, 0),
	}

	// Track primary key columns found in column definitions
	var primaryKeyCols []string

	// Process column definitions and constraints
	for _, def := range stmt.Defs {
		switch d := def.(type) {
		case *tree.ColumnTableDef:
			column, err := processColumnDef(d)
			if err != nil {
				return Response{Error: err}
			}
			table.Columns = append(table.Columns, column)

			// Track inline primary key
			if column.IsPrimaryKey {
				primaryKeyCols = append(primaryKeyCols, column.Name)
			}

		case *tree.UniqueConstraintTableDef:
			// Handle PRIMARY KEY table constraint
			if d.PrimaryKey {
				for _, col := range d.Columns {
					primaryKeyCols = append(primaryKeyCols, string(col.Column))
				}
			}

		case *tree.IndexTableDef:
			// Handle PRIMARY KEY table constraint (alternative form)
			// IndexTableDef has a PrimaryKey field in some versions
			// Let's check via reflection or just try to access it
			// For now, we'll skip this and handle it differently if needed
		}
	}

	// Apply primary key information
	if len(primaryKeyCols) > 0 {
		table.PrimaryKey = primaryKeyCols

		// Mark columns as primary key
		for i := range table.Columns {
			for _, pkCol := range primaryKeyCols {
				if table.Columns[i].Name == pkCol {
					table.Columns[i].IsPrimaryKey = true
					break
				}
			}
		}
	}

	// Handle IF NOT EXISTS
	if stmt.IfNotExists {
		created, err := catalog.CreateTableIfNotExists(table)
		if err != nil {
			return Response{Error: err}
		}
		if !created {
			// Table already exists, but IF NOT EXISTS was specified - no error
			return Response{RowsAffected: 0}
		}
		return Response{RowsAffected: 0}
	}

	// Normal CREATE TABLE (without IF NOT EXISTS)
	err := catalog.CreateTable(table)
	if err != nil {
		return Response{Error: fmt.Errorf("ERROR: %s", err.Error())}
	}

	return Response{RowsAffected: 0}
}

// processColumnDef extracts column information from a column definition.
func processColumnDef(def *tree.ColumnTableDef) (TableColumn, error) {
	column := TableColumn{
		Name: string(def.Name),
	}

	// Resolve the type
	typeName := def.Type.SQLString()
	typeMapping, err := ResolveType(typeName)
	if err != nil {
		return TableColumn{}, fmt.Errorf(
			"unsupported column type: %s",
			typeName,
		)
	}

	column.TypeOID = typeMapping.OID
	column.GoType = typeMapping.GoType

	// Check for NOT NULL constraint
	// Nullable.Nullability can be tree.NullabilityNotNull, tree.NullabilityNull, or tree.SilentNull
	if def.Nullable.Nullability == tree.NotNull {
		column.NotNull = true
	}

	// Check for DEFAULT expression
	if def.DefaultExpr.Expr != nil {
		column.DefaultExpr = def.DefaultExpr.Expr.String()
	}

	// Check for PRIMARY KEY constraint
	if def.PrimaryKey.IsPrimaryKey {
		column.IsPrimaryKey = true
	}

	return column, nil
}

// executeDropTable handles DROP TABLE statements.
func executeDropTable(stmt *tree.DropTable, catalog *Catalog) Response {
	// Handle multiple table names
	for _, tableName := range stmt.Names {
		name := tableName.String()

		if stmt.IfExists {
			_, err := catalog.DropTableIfExists(name)
			if err != nil {
				return Response{Error: err}
			}
			// If table doesn't exist with IF EXISTS, no error
			continue
		}

		// Normal DROP TABLE (without IF EXISTS)
		err := catalog.DropTable(name)
		if err != nil {
			return Response{Error: fmt.Errorf("ERROR: %s", err.Error())}
		}
	}

	return Response{RowsAffected: 0}
}

package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeDelete handles DELETE statements.
// Supports DELETE FROM table WHERE condition
func executeDelete(stmt *tree.Delete, catalog *Catalog) Response {
	// Get the table name from AliasedTableExpr
	aliasedTable, ok := stmt.Table.(*tree.AliasedTableExpr)
	if !ok {
		return Response{
			Error: fmt.Errorf("unsupported table expression: %T", stmt.Table),
		}
	}

	tableNameExpr, ok := aliasedTable.Expr.(*tree.TableName)
	if !ok {
		return Response{
			Error: fmt.Errorf("unsupported table type: %T", aliasedTable.Expr),
		}
	}

	tableName := tableNameExpr.String()

	// Check if table exists
	table, exists := catalog.GetTable(tableName)
	if !exists {
		return Response{
			Error: fmt.Errorf(
				"ERROR: relation \"%s\" does not exist",
				tableName,
			),
		}
	}

	// Build the column list for WHERE evaluation
	columns := make([]Column, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = Column{Name: col.Name, TypeOID: col.TypeOID}
	}

	// Perform the delete
	var rowsAffected int64

	if stmt.Where == nil {
		// Delete all rows
		count, err := catalog.DeleteRows(tableName,
			func(row []interface{}) bool {
				return true // Match all rows
			},
		)
		if err != nil {
			return Response{Error: err}
		}
		rowsAffected = int64(count)
	} else {
		// Delete rows matching WHERE clause
		count, err := catalog.DeleteRows(tableName,
			func(row []interface{}) bool {
				match, err := evaluateWhereExpr(
					stmt.Where.Expr,
					row,
					columns,
					catalog,
				)
				if err != nil {
					return false
				}
				return match
			},
		)
		if err != nil {
			return Response{Error: err}
		}
		rowsAffected = int64(count)
	}

	return Response{RowsAffected: rowsAffected}
}

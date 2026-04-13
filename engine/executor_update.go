package engine

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeUpdate handles UPDATE statements.
// Supports UPDATE table SET col = val, ... WHERE condition
func executeUpdate(stmt *tree.Update, catalog *Catalog) Response {
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

	// Build column index map for quick lookup
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[strings.ToLower(col.Name)] = i
	}

	// Build the column list for response (needed for WHERE evaluation)
	columns := make([]Column, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = Column{Name: col.Name, TypeOID: col.TypeOID}
	}

	// Parse SET clauses to determine what columns to update
	updates := make([]struct {
		colIdx int
		expr   tree.Expr
	}, 0, len(stmt.Exprs))

	for _, expr := range stmt.Exprs {
		// Get column name - the tuple contains the column names
		if len(expr.Names) == 0 {
			return Response{
				Error: fmt.Errorf("ERROR: UPDATE has no column names"),
			}
		}
		colName := expr.Names[0].String()
		colIdx, ok := colMap[strings.ToLower(colName)]
		if !ok {
			return Response{
				Error: fmt.Errorf(
					"ERROR: column \"%s\" of relation \"%s\" does not exist",
					colName,
					tableName,
				),
			}
		}

		updates = append(updates, struct {
			colIdx int
			expr   tree.Expr
		}{
			colIdx: colIdx,
			expr:   expr.Expr,
		})
	}

	// Perform the update
	var rowsAffected int64

	if stmt.Where == nil {
		// Update all rows
		_, err := catalog.UpdateRows(tableName,
			func(row []interface{}) bool {
				return true // Match all rows
			},
			func(row []interface{}) []interface{} {
				newRow := make([]interface{}, len(row))
				copy(newRow, row)

				// Apply updates
				for _, upd := range updates {
					val, err := evaluateExpr(
						upd.expr,
						table.Columns[upd.colIdx].GoType,
					)
					if err == nil {
						newRow[upd.colIdx] = val
					}
				}
				return newRow
			},
		)
		if err != nil {
			return Response{Error: err}
		}
		// Get row count
		allRows, _ := catalog.GetAllRows(tableName)
		rowsAffected = int64(len(allRows))
	} else {
		// Update rows matching WHERE clause
		_, err := catalog.UpdateRows(tableName,
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
			func(row []interface{}) []interface{} {
				newRow := make([]interface{}, len(row))
				copy(newRow, row)

				// Apply updates
				for _, upd := range updates {
					val, err := evaluateExpr(
						upd.expr,
						table.Columns[upd.colIdx].GoType,
					)
					if err == nil {
						newRow[upd.colIdx] = val
					}
				}
				return newRow
			},
		)
		if err != nil {
			return Response{Error: err}
		}
		// Count affected rows by evaluating the predicate
		allRows, _ := catalog.GetAllRows(tableName)
		for _, row := range allRows {
			match, _ := evaluateWhereExpr(
				stmt.Where.Expr,
				row,
				columns,
				catalog,
			)
			if match {
				rowsAffected++
			}
		}
	}

	return Response{RowsAffected: rowsAffected}
}

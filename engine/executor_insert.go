package engine

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeInsert handles INSERT statements.
// Supports INSERT INTO ... VALUES (...) and INSERT ... RETURNING ...
func executeInsert(stmt *tree.Insert, catalog *Catalog) Response {
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

	// Parse the VALUES clause from the Select statement
	valuesClause, ok := stmt.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return Response{
			Error: fmt.Errorf(
				"unsupported INSERT source: %T",
				stmt.Rows.Select,
			),
		}
	}

	// Insert each row
	insertedRows := make([][]interface{}, 0, len(valuesClause.Rows))
	var rowsAffected int64

	for _, rowExpr := range valuesClause.Rows {
		values := make([]interface{}, len(table.Columns))

		// Process each column value in the row
		for i, expr := range rowExpr {
			if i >= len(table.Columns) {
				return Response{
					Error: fmt.Errorf(
						"ERROR: INSERT has more expressions than target columns",
					),
				}
			}

			// Evaluate the expression to get the value
			val, err := evaluateExpr(expr, table.Columns[i].GoType)
			if err != nil {
				return Response{Error: err}
			}
			values[i] = val
		}

		// Insert the row
		if err := catalog.InsertRow(tableName, values); err != nil {
			return Response{Error: err}
		}

		insertedRows = append(insertedRows, values)
		rowsAffected++
	}

	// Handle RETURNING clause
	if stmt.Returning != nil {
		// Check if it's a NoReturningClause (no RETURNING)
		if _, ok := stmt.Returning.(*tree.NoReturningClause); ok {
			return Response{RowsAffected: rowsAffected}
		}

		returningExprs, ok := stmt.Returning.(*tree.ReturningExprs)
		if !ok {
			return Response{
				Error: fmt.Errorf(
					"unsupported RETURNING clause type: %T",
					stmt.Returning,
				),
			}
		}

		columns := make([]Column, 0, len(*returningExprs))
		returnRows := make([][]interface{}, 0, len(insertedRows))

		// Build column metadata
		for _, r := range *returningExprs {
			colName := getExprName(r.Expr)
			colIdx := findColumnIndex(table, colName)
			if colIdx < 0 {
				return Response{
					Error: fmt.Errorf(
						"ERROR: column \"%s\" does not exist",
						colName,
					),
				}
			}
			columns = append(columns, Column{
				Name:    colName,
				TypeOID: table.Columns[colIdx].TypeOID,
			})
		}

		// Build return rows
		for _, row := range insertedRows {
			returnRow := make([]interface{}, 0, len(*returningExprs))
			for _, r := range *returningExprs {
				colName := getExprName(r.Expr)
				colIdx := findColumnIndex(table, colName)
				if colIdx >= 0 {
					returnRow = append(returnRow, row[colIdx])
				}
			}
			returnRows = append(returnRows, returnRow)
		}

		return Response{
			Rows:         returnRows,
			Columns:      columns,
			RowsAffected: rowsAffected,
		}
	}

	return Response{RowsAffected: rowsAffected}
}

// evaluateExpr evaluates a constant expression to a Go value.
// For now, we handle basic literals; more complex expressions can be added.
func evaluateExpr(expr tree.Expr, expectedType string) (interface{}, error) {
	switch e := expr.(type) {
	case *tree.UnresolvedName:
		return nil, fmt.Errorf(
			"cannot evaluate unresolved name: %s",
			e.String(),
		)

	case *tree.NumVal:
		// Parse the numeric value based on expected type
		switch expectedType {
		case "int32":
			val, err := e.AsInt32()
			if err != nil {
				return nil, fmt.Errorf("cannot parse as int32: %v", err)
			}
			return int32(val), nil
		case "int64", "":
			val, err := e.AsInt64()
			if err != nil {
				return nil, fmt.Errorf("cannot parse as int64: %v", err)
			}
			return int64(val), nil
		case "float64":
			// Parse using string representation since AsFloat64 doesn't exist
			val, err := strconv.ParseFloat(e.String(), 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse as float64: %v", err)
			}
			return float64(val), nil
		default:
			// Try as int64 for other types
			val, err := e.AsInt64()
			if err != nil {
				return nil, fmt.Errorf("cannot parse numeric value: %v", err)
			}
			return int64(val), nil
		}

	case *tree.StrVal:
		return e.RawString(), nil

	case tree.Datum:
		return datumToGoValue(e, expectedType)

	default:
		// Try to convert via string representation for now
		return expr.String(), nil
	}
}

// datumToGoValue converts a tree.Datum to a Go value based on expected type.
func datumToGoValue(d tree.Datum, expectedType string) (interface{}, error) {
	switch val := d.(type) {
	case *tree.DString:
		return string(*val), nil

	case *tree.DInt:
		switch expectedType {
		case "int32":
			return int32(*val), nil
		case "int64":
			return int64(*val), nil
		default:
			return int64(*val), nil
		}

	case *tree.DFloat:
		return float64(*val), nil

	case *tree.DBool:
		return bool(*val), nil

	default:
		if d == tree.DNull {
			return nil, nil
		}
		// For other types, return the string representation
		return d.String(), nil
	}
}

// getExprName extracts the column name from an expression.
func getExprName(expr tree.Expr) string {
	switch e := expr.(type) {
	case *tree.UnresolvedName:
		return e.String()
	case *tree.ColumnItem:
		return e.Column()
	default:
		return expr.String()
	}
}

// findColumnIndex finds the index of a column in a table by name.
func findColumnIndex(table *Table, colName string) int {
	for i, col := range table.Columns {
		if col.Name == colName {
			return i
		}
	}
	return -1
}

package engine

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeUpdate handles UPDATE statements.
// Supports UPDATE table SET col = val, ... WHERE condition
func executeUpdate(
	stmt *tree.Update,
	catalog *Catalog,
	execCtx *ExecCtx,
) Response {
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

	// Build predicate and updater
	predicate := func(row []interface{}) bool { return true }
	if stmt.Where != nil {
		predicate = func(row []interface{}) bool {
			match, err := evaluateWhereExpr(
				stmt.Where.Expr,
				row,
				columns,
				catalog,
			)
			return err == nil && match
		}
	}
	updater := func(row []interface{}) []interface{} {
		newRow := make([]interface{}, len(row))
		copy(newRow, row)
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
	}

	var rowsAffected int64

	if execCtx != nil && execCtx.TxState != nil && execCtx.TxState.InTx {
		// Buffer the update for commit
		execCtx.TxState.PendingUpdates[tableName] = append(
			execCtx.TxState.PendingUpdates[tableName],
			struct {
				Predicate func([]interface{}) bool
				Updater   func([]interface{}) []interface{}
			}{Predicate: predicate, Updater: updater},
		)
		// Count affected rows from merged view
		rows, _ := getRowsForTable(catalog, execCtx.TxState, tableName)
		for _, row := range rows {
			if predicate(row) {
				rowsAffected++
			}
		}
	} else {
		// Apply directly to catalog
		count, err := catalog.UpdateRows(tableName, predicate, updater)
		if err != nil {
			return Response{Error: err}
		}
		rowsAffected = int64(count)
	}

	return Response{RowsAffected: rowsAffected}
}

package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeDelete handles DELETE statements.
// Supports DELETE FROM table WHERE condition
func executeDelete(
	stmt *tree.Delete,
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

	// Build the column list for WHERE evaluation
	columns := make([]Column, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = Column{Name: col.Name, TypeOID: col.TypeOID}
	}

	// Build predicate
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

	var rowsAffected int64

	if execCtx != nil && execCtx.TxState != nil && execCtx.TxState.InTx {
		// Buffer the delete for commit
		execCtx.TxState.PendingDeletes[tableName] = append(
			execCtx.TxState.PendingDeletes[tableName],
			predicate,
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
		count, err := catalog.DeleteRows(tableName, predicate)
		if err != nil {
			return Response{Error: err}
		}
		rowsAffected = int64(count)
	}

	return Response{RowsAffected: rowsAffected}
}

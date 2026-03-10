package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeSelect handles SELECT statements.
// Supports table scan, column projection, WHERE, ORDER BY, LIMIT, OFFSET, JOINs, and aggregates.
func executeSelect(
	stmt *tree.Select,
	catalog *Catalog,
	execCtx *ExecCtx,
) Response {
	// Handle the select statement
	if stmt.With != nil {
		return Response{Error: fmt.Errorf("WITH clauses not yet supported")}
	}

	// Get the select clause
	selectClause, ok := stmt.Select.(*tree.SelectClause)
	if !ok {
		// Handle other select types like SetOp (UNION, INTERSECT, EXCEPT)
		return Response{
			Error: fmt.Errorf("unsupported SELECT type: %T", stmt.Select),
		}
	}

	return executeSelectClause(stmt, selectClause, catalog, execCtx)
}

// executeSelectClause executes a simple SELECT clause.
// The outerSelect parameter provides access to ORDER BY, LIMIT, and OFFSET.
func executeSelectClause(
	outerSelect *tree.Select,
	stmt *tree.SelectClause,
	catalog *Catalog,
	execCtx *ExecCtx,
) Response {
	// Get table information
	_, columns, err := resolveFrom(&stmt.From, catalog)
	if err != nil {
		return Response{Error: err}
	}

	// Get all rows from tables (handle joins)
	rows, err := getRowsFrom(&stmt.From, catalog, execCtx)
	if err != nil {
		return Response{Error: err}
	}

	// Apply WHERE clause filtering
	if stmt.Where != nil {
		rows, err = filterRows(rows, columns, stmt.Where.Expr, catalog)
		if err != nil {
			return Response{Error: err}
		}
	}

	// Handle GROUP BY and aggregates
	if len(stmt.GroupBy) > 0 || hasAggregates(stmt.Exprs) {
		rows, columns, err = executeGroupBy(
			rows,
			columns,
			stmt.GroupBy,
			stmt.Having,
			stmt.Exprs,
			catalog,
		)
		if err != nil {
			return Response{Error: err}
		}
	} else {
		// Apply column projection for non-aggregate queries
		rows, columns, err = projectColumns(rows, columns, stmt.Exprs, catalog)
		if err != nil {
			return Response{Error: err}
		}
	}

	// Apply ORDER BY (from outer Select)
	if len(outerSelect.OrderBy) > 0 {
		rows, err = orderRows(rows, columns, outerSelect.OrderBy)
		if err != nil {
			return Response{Error: err}
		}
	}

	// Apply OFFSET (from outer Select's Limit)
	if outerSelect.Limit != nil && outerSelect.Limit.Offset != nil {
		offsetVal, err := evaluateOffsetCount(outerSelect.Limit.Offset)
		if err != nil {
			return Response{Error: err}
		}
		if offsetVal > 0 {
			if offsetVal >= len(rows) {
				rows = [][]interface{}{}
			} else {
				rows = rows[offsetVal:]
			}
		}
	}

	// Apply LIMIT (from outer Select)
	if outerSelect.Limit != nil && outerSelect.Limit.Count != nil {
		limitVal, err := evaluateOffsetCount(outerSelect.Limit.Count)
		if err != nil {
			return Response{Error: err}
		}
		if limitVal >= 0 && limitVal < len(rows) {
			rows = rows[:limitVal]
		}
	}

	return Response{
		Rows:         rows,
		Columns:      columns,
		RowsAffected: int64(len(rows)),
	}
}

// resolveFrom resolves table names from the FROM clause.
func resolveFrom(
	from *tree.From,
	catalog *Catalog,
) ([]*Table, []Column, error) {
	if from == nil || from.Tables == nil {
		return nil, nil, nil // No FROM clause (e.g., SELECT 1+1)
	}

	var tables []*Table
	var columns []Column

	for _, tableExpr := range from.Tables {
		t, cols, err := resolveTableExpr(tableExpr, catalog)
		if err != nil {
			return nil, nil, err
		}
		tables = append(tables, t...)
		columns = append(columns, cols...)
	}

	return tables, columns, nil
}

// resolveTableExpr resolves a single table expression.
func resolveTableExpr(
	expr tree.TableExpr,
	catalog *Catalog,
) ([]*Table, []Column, error) {
	switch e := expr.(type) {
	case *tree.AliasedTableExpr:
		return resolveTableExpr(e.Expr, catalog)

	case *tree.TableName:
		tableName := e.String()
		table, exists := catalog.GetTable(tableName)
		if !exists {
			return nil, nil, fmt.Errorf(
				"ERROR: relation \"%s\" does not exist",
				tableName,
			)
		}

		cols := make([]Column, len(table.Columns))
		for i, col := range table.Columns {
			cols[i] = Column{Name: col.Name, TypeOID: col.TypeOID}
		}
		return []*Table{table}, cols, nil

	case *tree.JoinTableExpr:
		// Handle JOINs - get tables from left and right
		leftTables, leftCols, err := resolveTableExpr(e.Left, catalog)
		if err != nil {
			return nil, nil, err
		}

		rightTables, rightCols, err := resolveTableExpr(e.Right, catalog)
		if err != nil {
			return nil, nil, err
		}

		return append(
				leftTables,
				rightTables...), append(
				leftCols,
				rightCols...), nil

	default:
		return nil, nil, fmt.Errorf("unsupported table expression: %T", expr)
	}
}

// getRowsFrom retrieves rows from tables, handling JOINs.
func getRowsFrom(
	from *tree.From,
	catalog *Catalog,
	execCtx *ExecCtx,
) ([][]interface{}, error) {
	if from == nil || from.Tables == nil || len(from.Tables) == 0 {
		return [][]interface{}{}, nil
	}

	// Process the first table expression
	return getRowsFromExpr(from.Tables[0], catalog, execCtx)
}

// getRowsFromExpr gets rows from a table expression (possibly with JOINs).
func getRowsFromExpr(
	expr tree.TableExpr,
	catalog *Catalog,
	execCtx *ExecCtx,
) ([][]interface{}, error) {
	var txState *TxState
	if execCtx != nil {
		txState = execCtx.TxState
	}

	switch e := expr.(type) {
	case *tree.AliasedTableExpr:
		return getRowsFromExpr(e.Expr, catalog, execCtx)

	case *tree.TableName:
		tableName := e.String()
		return getRowsForTable(catalog, txState, tableName)

	case *tree.JoinTableExpr:
		// Get rows from left and right
		leftRows, err := getRowsFromExpr(e.Left, catalog, execCtx)
		if err != nil {
			return nil, err
		}

		rightRows, err := getRowsFromExpr(e.Right, catalog, execCtx)
		if err != nil {
			return nil, err
		}

		// Resolve columns for ON condition evaluation
		_, leftCols, err := resolveTableExpr(e.Left, catalog)
		if err != nil {
			return nil, err
		}
		_, rightCols, err := resolveTableExpr(e.Right, catalog)
		if err != nil {
			return nil, err
		}
		combinedColumns := append(leftCols, rightCols...)

		// Perform the join
		return performJoin(
			leftRows,
			rightRows,
			combinedColumns,
			len(rightCols),
			e.JoinType,
			e.Cond,
			catalog,
		)

	default:
		return nil, fmt.Errorf("unsupported table expression: %T", expr)
	}
}

// performJoin performs a join between two row sets.
func performJoin(
	leftRows, rightRows [][]interface{},
	columns []Column,
	rightColCount int,
	joinType string,
	cond tree.JoinCond,
	catalog *Catalog,
) ([][]interface{}, error) {
	var result [][]interface{}

	for _, leftRow := range leftRows {
		matched := false

		for _, rightRow := range rightRows {
			// Combine rows
			combinedRow := append(
				append([]interface{}(nil), leftRow...),
				rightRow...)

			// Evaluate join condition if present
			if cond != nil {
				switch c := cond.(type) {
				case *tree.OnJoinCond:
					ok, err := evaluateWhereExpr(
						c.Expr,
						combinedRow,
						columns,
						catalog,
					)
					if err != nil {
						return nil, err
					}
					if ok {
						result = append(result, combinedRow)
						matched = true
					}
				default:
					// No condition or unsupported - do cross join
					result = append(result, combinedRow)
					matched = true
				}
			} else {
				// Cross join
				result = append(result, combinedRow)
				matched = true
			}
		}

		// For LEFT JOIN, include left row even if no match
		if !matched && joinType == "LEFT" {
			// Append NULLs for right side
			nullRow := make([]interface{}, rightColCount)
			result = append(
				result,
				append(append([]interface{}(nil), leftRow...), nullRow...),
			)
		}
	}

	return result, nil
}

// filterRows filters rows based on a WHERE clause expression.
func filterRows(
	rows [][]interface{},
	columns []Column,
	whereExpr tree.Expr,
	catalog *Catalog,
) ([][]interface{}, error) {
	var result [][]interface{}

	for _, row := range rows {
		val, err := evaluateWhereExpr(whereExpr, row, columns, catalog)
		if err != nil {
			return nil, err
		}

		if val {
			result = append(result, row)
		}
	}

	return result, nil
}

// evaluateWhereExpr evaluates a WHERE expression against a row.
func evaluateWhereExpr(
	expr tree.Expr,
	row []interface{},
	columns []Column,
	catalog *Catalog,
) (bool, error) {
	switch e := expr.(type) {
	case *tree.ComparisonExpr:
		return evaluateComparison(e, row, columns, catalog)

	case *tree.AndExpr:
		left, err := evaluateWhereExpr(e.Left, row, columns, catalog)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil // Short circuit
		}
		return evaluateWhereExpr(e.Right, row, columns, catalog)

	case *tree.OrExpr:
		left, err := evaluateWhereExpr(e.Left, row, columns, catalog)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil // Short circuit
		}
		return evaluateWhereExpr(e.Right, row, columns, catalog)

	case *tree.NotExpr:
		val, err := evaluateWhereExpr(e.Expr, row, columns, catalog)
		if err != nil {
			return false, err
		}
		return !val, nil

	case *tree.DBool:
		return bool(*e), nil

	case *tree.IsNullExpr:
		val, err := getExprValue(e.Expr, row, columns, catalog)
		if err != nil {
			return false, err
		}
		return val == nil, nil

	case *tree.IsNotNullExpr:
		val, err := getExprValue(e.Expr, row, columns, catalog)
		if err != nil {
			return false, err
		}
		return val != nil, nil

	default:
		// For unknown expressions, try to evaluate as boolean
		val, err := getExprValue(expr, row, columns, catalog)
		if err != nil {
			return false, err
		}
		if b, ok := val.(bool); ok {
			return b, nil
		}
		// Treat non-boolean expressions as true (accept the row)
		return true, nil
	}
}

// evaluateComparison evaluates a comparison expression.
func evaluateComparison(
	expr *tree.ComparisonExpr,
	row []interface{},
	columns []Column,
	catalog *Catalog,
) (bool, error) {
	leftVal, err := getExprValue(expr.Left, row, columns, catalog)
	if err != nil {
		return false, err
	}

	rightVal, err := getExprValue(expr.Right, row, columns, catalog)
	if err != nil {
		return false, err
	}

	// In SQL, comparison with NULL yields NULL (unknown), which filters out the row
	op := expr.Operator.String()
	if op != "IS" && op != "IS NOT" {
		if leftVal == nil || rightVal == nil {
			return false, nil
		}
	}

	switch op {
	case "=":
		return compareValues(leftVal, rightVal) == 0, nil

	case "!=", "<>":
		return compareValues(leftVal, rightVal) != 0, nil

	case "<":
		return compareValues(leftVal, rightVal) < 0, nil

	case "<=":
		return compareValues(leftVal, rightVal) <= 0, nil

	case ">":
		return compareValues(leftVal, rightVal) > 0, nil

	case ">=":
		return compareValues(leftVal, rightVal) >= 0, nil

	case "LIKE":
		return evaluateLike(leftVal, expr.Right, row, columns, catalog)

	case "NOT LIKE":
		result, err := evaluateLike(leftVal, expr.Right, row, columns, catalog)
		if err != nil {
			return false, err
		}
		return !result, nil

	case "IN":
		return evaluateIn(leftVal, expr.Right, row, columns, catalog)

	case "NOT IN":
		result, err := evaluateIn(leftVal, expr.Right, row, columns, catalog)
		if err != nil {
			return false, err
		}
		return !result, nil

	default:
		return false, fmt.Errorf(
			"unsupported comparison operator: %v",
			expr.Operator.String(),
		)
	}
}

// evaluateLike performs LIKE pattern matching.
func evaluateLike(
	leftVal interface{},
	patternExpr tree.Expr,
	row []interface{},
	columns []Column,
	catalog *Catalog,
) (bool, error) {
	leftStr, ok := leftVal.(string)
	if !ok {
		return false, nil // Non-string values don't match
	}

	pattern, err := getExprValue(patternExpr, row, columns, catalog)
	if err != nil {
		return false, err
	}

	patternStr, ok := pattern.(string)
	if !ok {
		return false, nil
	}

	return matchLikePattern(leftStr, patternStr), nil
}

// matchLikePattern matches a string against a LIKE pattern.
// Supports % (zero or more characters) and _ (single character).
func matchLikePattern(str, pattern string) bool {
	// Simple recursive implementation
	if len(pattern) == 0 {
		return len(str) == 0
	}

	if len(str) == 0 {
		// Check if remaining pattern is all %
		for _, ch := range pattern {
			if ch != '%' {
				return false
			}
		}
		return true
	}

	switch pattern[0] {
	case '%':
		// Match zero or more characters
		for i := 0; i <= len(str); i++ {
			if matchLikePattern(str[i:], pattern[1:]) {
				return true
			}
		}
		return false
	case '_':
		// Match any single character
		return matchLikePattern(str[1:], pattern[1:])
	default:
		// Match exact character (case-insensitive for simplicity)
		if strings.EqualFold(string(str[0]), string(pattern[0])) {
			return matchLikePattern(str[1:], pattern[1:])
		}
		return false
	}
}

// evaluateIn checks if a value is in a list.
func evaluateIn(
	leftVal interface{},
	rightExpr tree.Expr,
	row []interface{},
	columns []Column,
	catalog *Catalog,
) (bool, error) {
	switch e := rightExpr.(type) {
	case *tree.Tuple:
		for _, expr := range e.Exprs {
			val, err := getExprValue(expr, row, columns, catalog)
			if err != nil {
				return false, err
			}
			if compareValues(leftVal, val) == 0 {
				return true, nil
			}
		}
		return false, nil

	case *tree.DTuple:
		for _, val := range e.D {
			v, err := datumToGoValue(val, "string")
			if err != nil {
				return false, err
			}
			if compareValues(leftVal, v) == 0 {
				return true, nil
			}
		}
		return false, nil

	default:
		// Try to get value and compare
		val, err := getExprValue(rightExpr, row, columns, catalog)
		if err != nil {
			return false, err
		}
		return compareValues(leftVal, val) == 0, nil
	}
}

// getExprValue gets the value of an expression (column reference or constant).
func getExprValue(
	expr tree.Expr,
	row []interface{},
	columns []Column,
	catalog *Catalog,
) (interface{}, error) {
	switch e := expr.(type) {
	case *tree.UnresolvedName:
		colName := e.String()
		for i, col := range columns {
			if strings.EqualFold(col.Name, colName) {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s", colName)

	case *tree.NumVal:
		// Parse numeric literal
		val, err := e.AsInt64()
		if err != nil {
			return nil, err
		}
		return val, nil

	case *tree.StrVal:
		return e.RawString(), nil

	case *tree.DString:
		return string(*e), nil

	case *tree.DInt:
		return int64(*e), nil

	case *tree.DFloat:
		return float64(*e), nil

	case *tree.DBool:
		return bool(*e), nil

	default:
		// Check if this is a DNull
		if d, ok := expr.(tree.Datum); ok && d == tree.DNull {
			return nil, nil
		}
		// Try to find as column reference (e.g. for HAVING SUM(amount) > 150)
		exprStr := expr.String()
		for i, col := range columns {
			if strings.EqualFold(col.Name, exprStr) {
				return row[i], nil
			}
		}
		return expr.String(), nil
	}
}

// compareValues compares two values and returns:
// -1 if left < right, 0 if left == right, 1 if left > right
func compareValues(left, right interface{}) int {
	// Handle nil (NULL) values
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	// Handle numeric type conversions
	switch l := left.(type) {
	case int32:
		switch r := right.(type) {
		case int32:
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		case int64:
			li64 := int64(l)
			if li64 < r {
				return -1
			} else if li64 > r {
				return 1
			}
			return 0
		}

	case int64:
		switch r := right.(type) {
		case int64:
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		case int32:
			ri64 := int64(r)
			if l < ri64 {
				return -1
			} else if l > ri64 {
				return 1
			}
			return 0
		}

	case float64:
		switch r := right.(type) {
		case float64:
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		case int32:
			rf64 := float64(r)
			if l < rf64 {
				return -1
			} else if l > rf64 {
				return 1
			}
			return 0
		case int64:
			rf64 := float64(r)
			if l < rf64 {
				return -1
			} else if l > rf64 {
				return 1
			}
			return 0
		}

	case string:
		r, ok := right.(string)
		if !ok {
			// Convert right to string for comparison
			r = fmt.Sprintf("%v", right)
		}
		return strings.Compare(l, r)

	case bool:
		r, ok := right.(bool)
		if !ok {
			return 0
		}
		if l == r {
			return 0
		}
		if !l && r {
			return -1
		}
		return 1

	default:
		// Fall back to string comparison
		return strings.Compare(
			fmt.Sprintf("%v", left),
			fmt.Sprintf("%v", right),
		)
	}

	return 0
}

// projectColumns projects selected columns from rows.
func projectColumns(
	rows [][]interface{},
	allColumns []Column,
	selectExprs tree.SelectExprs,
	catalog *Catalog,
) ([][]interface{}, []Column, error) {
	if len(selectExprs) == 0 {
		return rows, allColumns, nil
	}

	// Check for wildcard
	if len(selectExprs) == 1 {
		if _, ok := selectExprs[0].Expr.(*tree.UnqualifiedStar); ok {
			return rows, allColumns, nil
		}
	}

	// Build projected column list
	projColumns := make([]Column, 0, len(selectExprs))
	colIndices := make([]int, 0, len(selectExprs))

	for _, expr := range selectExprs {
		switch e := expr.Expr.(type) {
		case *tree.UnresolvedName:
			colName := e.String()
			found := false
			for i, col := range allColumns {
				if strings.EqualFold(col.Name, colName) {
					projColumns = append(projColumns, col)
					colIndices = append(colIndices, i)
					found = true
					break
				}
			}
			if !found {
				return nil, nil, fmt.Errorf(
					"ERROR: column \"%s\" does not exist",
					colName,
				)
			}

		case *tree.UnqualifiedStar:
			// Should have been handled above, but just in case
			return rows, allColumns, nil

		default:
			// For expressions, we'll use the expression string as the column name
			colName := expr.Expr.String()
			projColumns = append(
				projColumns,
				Column{Name: colName, TypeOID: 25},
			) // text type
			colIndices = append(
				colIndices,
				-1,
			) // Mark as expression
		}
	}

	// Project rows
	projRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		projRow := make([]interface{}, len(colIndices))
		for j, idx := range colIndices {
			if idx >= 0 {
				projRow[j] = row[idx]
			} else {
				// Evaluate expression - for now, just return the string
				projRow[j] = selectExprs[j].Expr.String()
			}
		}
		projRows[i] = projRow
	}

	return projRows, projColumns, nil
}

// orderRows sorts rows based on ORDER BY clause.
func orderRows(
	rows [][]interface{},
	columns []Column,
	orderBy tree.OrderBy,
) ([][]interface{}, error) {
	if len(orderBy) == 0 {
		return rows, nil
	}

	// Build column index map for quick lookup
	colMap := make(map[string]int)
	for i, col := range columns {
		colMap[strings.ToLower(col.Name)] = i
	}

	// Create a sortable wrapper
	sortable := &rowSorter{
		rows:    rows,
		columns: columns,
		orderBy: orderBy,
		colMap:  colMap,
	}

	sort.Sort(sortable)
	return sortable.rows, nil
}

// rowSorter implements sort.Interface for sorting rows.
type rowSorter struct {
	rows    [][]interface{}
	columns []Column
	orderBy tree.OrderBy
	colMap  map[string]int
}

func (r *rowSorter) Len() int {
	return len(r.rows)
}

func (r *rowSorter) Swap(i, j int) {
	r.rows[i], r.rows[j] = r.rows[j], r.rows[i]
}

func (r *rowSorter) Less(i, j int) bool {
	rowA := r.rows[i]
	rowB := r.rows[j]

	for _, order := range r.orderBy {
		colName := order.Expr.String()
		colIdx, ok := r.colMap[strings.ToLower(colName)]
		if !ok {
			continue
		}

		cmp := compareValues(rowA[colIdx], rowB[colIdx])
		if cmp != 0 {
			if order.Direction == tree.Descending {
				return cmp > 0
			}
			return cmp < 0
		}
	}

	return false // Equal
}

// evaluateOffsetCount evaluates an OFFSET or LIMIT expression.
func evaluateOffsetCount(expr tree.Expr) (int, error) {
	switch e := expr.(type) {
	case *tree.DInt:
		return int(*e), nil
	case *tree.NumVal:
		val, err := e.AsInt64()
		if err != nil {
			return 0, err
		}
		return int(val), nil
	default:
		// Try to parse from string
		return 0, fmt.Errorf("unsupported OFFSET/LIMIT expression: %T", expr)
	}
}

// hasAggregates checks if the select expressions contain aggregate functions.
func hasAggregates(exprs tree.SelectExprs) bool {
	for _, expr := range exprs {
		if isAggregate(expr.Expr) {
			return true
		}
	}
	return false
}

// isAggregate checks if an expression is an aggregate function.
func isAggregate(expr tree.Expr) bool {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		funcName := e.Func.String()
		switch strings.ToUpper(funcName) {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
	}
	return false
}

// executeGroupBy performs GROUP BY aggregation.
func executeGroupBy(
	rows [][]interface{},
	columns []Column,
	groupBy tree.GroupBy,
	having *tree.Where,
	selectExprs tree.SelectExprs,
	catalog *Catalog,
) ([][]interface{}, []Column, error) {
	// Simple aggregation without GROUP BY (single result for entire table)
	if len(groupBy) == 0 {
		return executeSimpleAggregate(rows, columns, selectExprs, catalog)
	}

	// Group rows by GROUP BY expression values
	groups := make(map[string][][]interface{})
	for _, row := range rows {
		keyParts := make([]string, len(groupBy))
		for i, expr := range groupBy {
			val, err := getExprValue(expr, row, columns, catalog)
			if err != nil {
				return nil, nil, err
			}
			if val == nil {
				keyParts[i] = "NULL"
			} else {
				keyParts[i] = fmt.Sprintf("%v", val)
			}
		}
		key := strings.Join(keyParts, "|")
		groups[key] = append(groups[key], row)
	}

	// Produce one row per group
	var resultRows [][]interface{}
	var resultCols []Column
	// Get resultCols from first group (all groups have same schema)
	var firstGroupRows [][]interface{}
	for _, groupRows := range groups {
		firstGroupRows = groupRows
		break
	}
	for _, expr := range selectExprs {
		if isAggregate(expr.Expr) {
			_, col, err := evaluateAggregate(
				expr.Expr,
				firstGroupRows,
				columns,
				catalog,
			)
			if err != nil {
				return nil, nil, err
			}
			resultCols = append(resultCols, col)
		} else {
			resultCols = append(
				resultCols,
				Column{Name: expr.Expr.String(), TypeOID: 25},
			)
		}
	}

	for _, groupRows := range groups {
		resultRow := make([]interface{}, len(selectExprs))
		for i, expr := range selectExprs {
			if isAggregate(expr.Expr) {
				val, _, err := evaluateAggregate(
					expr.Expr,
					groupRows,
					columns,
					catalog,
				)
				if err != nil {
					return nil, nil, err
				}
				resultRow[i] = val
			} else {
				val, err := getExprValue(
					expr.Expr,
					groupRows[0],
					columns,
					catalog,
				)
				if err != nil {
					return nil, nil, err
				}
				resultRow[i] = val
			}
		}
		resultRows = append(resultRows, resultRow)
	}

	// Apply HAVING filter
	if having != nil {
		var filtered [][]interface{}
		for _, row := range resultRows {
			ok, err := evaluateWhereExpr(having.Expr, row, resultCols, catalog)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				filtered = append(filtered, row)
			}
		}
		resultRows = filtered
	}

	return resultRows, resultCols, nil
}

// executeSimpleAggregate computes aggregates over all rows.
func executeSimpleAggregate(
	rows [][]interface{},
	columns []Column,
	selectExprs tree.SelectExprs,
	catalog *Catalog,
) ([][]interface{}, []Column, error) {
	resultRow := make([]interface{}, len(selectExprs))
	resultCols := make([]Column, len(selectExprs))

	for i, expr := range selectExprs {
		val, col, err := evaluateAggregate(expr.Expr, rows, columns, catalog)
		if err != nil {
			return nil, nil, err
		}
		resultRow[i] = val
		resultCols[i] = col
	}

	return [][]interface{}{resultRow}, resultCols, nil
}

// evaluateAggregate evaluates an aggregate expression.
func evaluateAggregate(
	expr tree.Expr,
	rows [][]interface{},
	columns []Column,
	catalog *Catalog,
) (interface{}, Column, error) {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		funcName := strings.ToUpper(e.Func.String())
		colName := expr.String()

		switch funcName {
		case "COUNT":
			return int64(
					len(rows),
				), Column{
					Name:    colName,
					TypeOID: 20,
				}, nil // int8
		case "SUM":
			return evaluateSum(e, rows, columns, catalog)
		case "AVG":
			return evaluateAvg(e, rows, columns, catalog)
		case "MIN":
			return evaluateMin(e, rows, columns, catalog)
		case "MAX":
			return evaluateMax(e, rows, columns, catalog)
		default:
			return nil, Column{}, fmt.Errorf(
				"unsupported aggregate function: %s",
				funcName,
			)
		}

	default:
		// Non-aggregate expression
		return expr.String(), Column{Name: expr.String(), TypeOID: 25}, nil
	}
}

// evaluateSum computes the SUM aggregate.
func evaluateSum(
	expr *tree.FuncExpr,
	rows [][]interface{},
	columns []Column,
	catalog *Catalog,
) (interface{}, Column, error) {
	if len(expr.Exprs) != 1 {
		return nil, Column{}, fmt.Errorf("SUM requires exactly one argument")
	}

	colName := expr.Exprs[0].String()
	colIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		return nil, Column{}, fmt.Errorf("column not found: %s", colName)
	}

	var sum float64
	var hasValue bool

	for _, row := range rows {
		if row[colIdx] != nil {
			switch v := row[colIdx].(type) {
			case int32:
				sum += float64(v)
				hasValue = true
			case int64:
				sum += float64(v)
				hasValue = true
			case float64:
				sum += v
				hasValue = true
			}
		}
	}

	if !hasValue {
		return nil, Column{Name: expr.String(), TypeOID: 701}, nil // float8
	}

	return sum, Column{Name: expr.String(), TypeOID: 701}, nil
}

// evaluateAvg computes the AVG aggregate.
func evaluateAvg(
	expr *tree.FuncExpr,
	rows [][]interface{},
	columns []Column,
	catalog *Catalog,
) (interface{}, Column, error) {
	if len(expr.Exprs) != 1 {
		return nil, Column{}, fmt.Errorf("AVG requires exactly one argument")
	}

	colName := expr.Exprs[0].String()
	colIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		return nil, Column{}, fmt.Errorf("column not found: %s", colName)
	}

	var sum float64
	var count int64

	for _, row := range rows {
		if row[colIdx] != nil {
			switch v := row[colIdx].(type) {
			case int32:
				sum += float64(v)
				count++
			case int64:
				sum += float64(v)
				count++
			case float64:
				sum += v
				count++
			}
		}
	}

	if count == 0 {
		return nil, Column{Name: expr.String(), TypeOID: 701}, nil
	}

	return sum / float64(count), Column{Name: expr.String(), TypeOID: 701}, nil
}

// evaluateMin computes the MIN aggregate.
func evaluateMin(
	expr *tree.FuncExpr,
	rows [][]interface{},
	columns []Column,
	catalog *Catalog,
) (interface{}, Column, error) {
	if len(expr.Exprs) != 1 {
		return nil, Column{}, fmt.Errorf("MIN requires exactly one argument")
	}

	colName := expr.Exprs[0].String()
	colIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		return nil, Column{}, fmt.Errorf("column not found: %s", colName)
	}

	var min interface{}
	var hasValue bool

	for _, row := range rows {
		if row[colIdx] != nil {
			if !hasValue || compareValues(row[colIdx], min) < 0 {
				min = row[colIdx]
				hasValue = true
			}
		}
	}

	return min, Column{
		Name:    expr.String(),
		TypeOID: columns[colIdx].TypeOID,
	}, nil
}

// evaluateMax computes the MAX aggregate.
func evaluateMax(
	expr *tree.FuncExpr,
	rows [][]interface{},
	columns []Column,
	catalog *Catalog,
) (interface{}, Column, error) {
	if len(expr.Exprs) != 1 {
		return nil, Column{}, fmt.Errorf("MAX requires exactly one argument")
	}

	colName := expr.Exprs[0].String()
	colIdx := -1
	for i, col := range columns {
		if strings.EqualFold(col.Name, colName) {
			colIdx = i
			break
		}
	}

	if colIdx < 0 {
		return nil, Column{}, fmt.Errorf("column not found: %s", colName)
	}

	var max interface{}
	var hasValue bool

	for _, row := range rows {
		if row[colIdx] != nil {
			if !hasValue || compareValues(row[colIdx], max) > 0 {
				max = row[colIdx]
				hasValue = true
			}
		}
	}

	return max, Column{
		Name:    expr.String(),
		TypeOID: columns[colIdx].TypeOID,
	}, nil
}

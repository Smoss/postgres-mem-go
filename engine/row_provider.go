package engine

// getRowsForTable returns rows for a table, merging transaction buffer when in a transaction.
// When txState is nil or not in a transaction, returns catalog rows directly.
func getRowsForTable(
	catalog *Catalog,
	txState *TxState,
	tableName string,
) ([][]interface{}, error) {
	if txState == nil || !txState.InTx {
		return catalog.GetAllRows(tableName)
	}

	baseRows, err := catalog.GetAllRows(tableName)
	if err != nil {
		return nil, err
	}

	// Apply pending deletes (exclude rows matching any delete predicate)
	var afterDeletes [][]interface{}
	for _, row := range baseRows {
		excluded := false
		for _, pred := range txState.PendingDeletes[tableName] {
			if pred(row) {
				excluded = true
				break
			}
		}
		if !excluded {
			afterDeletes = append(afterDeletes, row)
		}
	}

	// Apply pending updates
	for _, u := range txState.PendingUpdates[tableName] {
		for i, row := range afterDeletes {
			if u.Predicate(row) {
				afterDeletes[i] = u.Updater(row)
			}
		}
	}

	// Add pending inserts
	afterDeletes = append(afterDeletes, txState.PendingInserts[tableName]...)

	return afterDeletes, nil
}

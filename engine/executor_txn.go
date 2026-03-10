package engine

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeBegin handles BEGIN TRANSACTION statements.
func executeBegin(
	stmt *tree.BeginTransaction,
	connID uint64,
	txState map[uint64]*TxState,
) Response {
	txState[connID] = newTxState()
	return Response{TxStatus: 'T'}
}

// executeCommit handles COMMIT statements.
func executeCommit(
	stmt *tree.CommitTransaction,
	connID uint64,
	txState map[uint64]*TxState,
	catalog *Catalog,
) Response {
	tx := txState[connID]
	if tx == nil || !tx.InTx {
		return Response{TxStatus: 'I'}
	}

	// Apply buffer to catalog: deletes, then updates, then inserts
	for tableName, predicates := range tx.PendingDeletes {
		for _, pred := range predicates {
			_, _ = catalog.DeleteRows(tableName, pred)
		}
	}
	for tableName, updates := range tx.PendingUpdates {
		for _, u := range updates {
			_, _ = catalog.UpdateRows(tableName, u.Predicate, u.Updater)
		}
	}
	for tableName, rows := range tx.PendingInserts {
		for _, row := range rows {
			_ = catalog.InsertRow(tableName, row)
		}
	}

	delete(txState, connID)
	return Response{TxStatus: 'I'}
}

// executeRollback handles ROLLBACK statements.
func executeRollback(
	stmt *tree.RollbackTransaction,
	connID uint64,
	txState map[uint64]*TxState,
) Response {
	delete(txState, connID)
	return Response{TxStatus: 'I'}
}

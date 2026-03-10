package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeBegin handles BEGIN TRANSACTION statements.
// This is a stub implementation for Phase 2.
func executeBegin(stmt *tree.BeginTransaction) Response {
	// For Phase 2, return a feature not supported error
	// Phase 5 will implement full transaction support
	return Response{
		Error: fmt.Errorf("BEGIN TRANSACTION not yet implemented"),
	}
}

// executeCommit handles COMMIT statements.
// This is a stub implementation for Phase 2.
func executeCommit(stmt *tree.CommitTransaction) Response {
	// For Phase 2, return a feature not supported error
	// Phase 5 will implement full transaction support
	return Response{
		Error: fmt.Errorf("COMMIT not yet implemented"),
	}
}

// executeRollback handles ROLLBACK statements.
// This is a stub implementation for Phase 2.
func executeRollback(stmt *tree.RollbackTransaction) Response {
	// For Phase 2, return a feature not supported error
	// Phase 5 will implement full transaction support
	return Response{
		Error: fmt.Errorf("ROLLBACK not yet implemented"),
	}
}

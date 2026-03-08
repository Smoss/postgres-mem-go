package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeInsert handles INSERT statements.
// This is a stub implementation for Phase 2.
func executeInsert(stmt *tree.Insert) Response {
	// For Phase 2, return a feature not supported error
	// Phase 4 will implement full INSERT execution
	return Response{
		Error: fmt.Errorf("INSERT not yet implemented"),
	}
}

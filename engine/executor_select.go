package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeSelect handles SELECT statements.
// This is a stub implementation for Phase 2.
func executeSelect(stmt *tree.Select) Response {
	// For Phase 2, return a feature not supported error
	// Phase 4 will implement full SELECT execution
	return Response{
		Error: fmt.Errorf("SELECT not yet implemented"),
	}
}

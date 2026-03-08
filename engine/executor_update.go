package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeUpdate handles UPDATE statements.
// This is a stub implementation for Phase 2.
func executeUpdate(stmt *tree.Update) Response {
	// For Phase 2, return a feature not supported error
	// Phase 4 will implement full UPDATE execution
	return Response{
		Error: fmt.Errorf("UPDATE not yet implemented"),
	}
}

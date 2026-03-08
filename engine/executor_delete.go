package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeDelete handles DELETE statements.
// This is a stub implementation for Phase 2.
func executeDelete(stmt *tree.Delete) Response {
	// For Phase 2, return a feature not supported error
	// Phase 4 will implement full DELETE execution
	return Response{
		Error: fmt.Errorf("DELETE not yet implemented"),
	}
}

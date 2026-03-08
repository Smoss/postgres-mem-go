package engine

import (
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// executeCreateTable handles CREATE TABLE statements.
// This is a stub implementation for Phase 2.
func executeCreateTable(stmt *tree.CreateTable) Response {
	// For Phase 2, return a feature not supported error
	// Phase 3 will implement full CREATE TABLE execution
	return Response{
		Error: fmt.Errorf("CREATE TABLE not yet implemented"),
	}
}

// executeDropTable handles DROP TABLE statements.
// This is a stub implementation for Phase 2.
func executeDropTable(stmt *tree.DropTable) Response {
	// For Phase 2, return a feature not supported error
	// Phase 3 will implement full DROP TABLE execution
	return Response{
		Error: fmt.Errorf("DROP TABLE not yet implemented"),
	}
}

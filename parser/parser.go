// Package parser provides SQL parsing using the CockroachDB parser.
package parser

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Parse parses a single SQL statement and returns the AST.
// It uses the CockroachDB parser which is PostgreSQL-compatible.
func Parse(sql string) (tree.Statement, error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	if len(stmts) == 0 {
		return nil, nil
	}

	return stmts[0].AST, nil
}

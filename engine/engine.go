// Package engine provides the SQL execution engine with request/response dispatch.
package engine

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

// Request represents a SQL execution request from a connection.
type Request struct {
	Stmt       tree.Statement
	ConnID     uint64
	ResponseCh chan Response
}

// Response represents the result of executing a SQL statement.
type Response struct {
	Rows         [][]interface{}
	Columns      []Column
	RowsAffected int64
	Error        error
}

// Column represents a column in the result set.
type Column struct {
	Name    string
	TypeOID uint32
}

// Engine is the central SQL execution engine that processes requests serially.
type Engine struct {
	requestCh chan Request
	ctx       context.Context
	cancel    context.CancelFunc
	catalog   *Catalog
}

// New creates a new Engine.
func New() *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		requestCh: make(chan Request),
		ctx:       ctx,
		cancel:    cancel,
		catalog:   NewCatalog(),
	}
}

// Start begins the engine's request processing loop.
func (e *Engine) Start() {
	go e.run()
}

// Stop gracefully shuts down the engine.
func (e *Engine) Stop() {
	e.cancel()
}

// Submit sends a request to the engine and returns the response channel.
func (e *Engine) Submit(req Request) {
	e.requestCh <- req
}

func (e *Engine) run() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case req := <-e.requestCh:
			resp := e.dispatch(req.Stmt)
			req.ResponseCh <- resp
		}
	}
}

func (e *Engine) dispatch(stmt tree.Statement) Response {
	switch s := stmt.(type) {
	case *tree.Select:
		return executeSelect(s, e.catalog)
	case *tree.Insert:
		return executeInsert(s, e.catalog)
	case *tree.Update:
		return executeUpdate(s, e.catalog)
	case *tree.Delete:
		return executeDelete(s, e.catalog)
	case *tree.CreateTable:
		return executeCreateTable(s, e.catalog)
	case *tree.DropTable:
		return executeDropTable(s, e.catalog)
	case *tree.BeginTransaction:
		return executeBegin(s)
	case *tree.CommitTransaction:
		return executeCommit(s)
	case *tree.RollbackTransaction:
		return executeRollback(s)
	default:
		return Response{
			Error: fmt.Errorf("unsupported statement type: %T", stmt),
		}
	}
}

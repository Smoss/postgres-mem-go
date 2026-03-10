// Package engine provides the SQL execution engine with request/response dispatch.
package engine

import (
	"context"
	"fmt"
	"sync/atomic"

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
	TxStatus     byte // 'I' = Idle, 'T' = In Transaction
}

// Column represents a column in the result set.
type Column struct {
	Name    string
	TypeOID uint32
}

// TxState holds per-connection transaction state for snapshot isolation.
type TxState struct {
	InTx           bool
	PendingInserts map[string][][]interface{}
	PendingDeletes map[string][]func([]interface{}) bool
	PendingUpdates map[string][]struct {
		Predicate func([]interface{}) bool
		Updater   func([]interface{}) []interface{}
	}
}

func newTxState() *TxState {
	return &TxState{
		InTx:           true,
		PendingInserts: make(map[string][][]interface{}),
		PendingDeletes: make(map[string][]func([]interface{}) bool),
		PendingUpdates: make(map[string][]struct {
			Predicate func([]interface{}) bool
			Updater   func([]interface{}) []interface{}
		}),
	}
}

// ExecCtx provides execution context for a request (catalog, connection, transaction state).
type ExecCtx struct {
	ConnID  uint64
	Catalog *Catalog
	TxState *TxState
}

// Engine is the central SQL execution engine that processes requests serially.
type Engine struct {
	requestCh  chan Request
	ctx        context.Context
	cancel     context.CancelFunc
	catalog    *Catalog
	txState    map[uint64]*TxState
	nextConnID atomic.Uint64
}

// New creates a new Engine.
func New() *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		requestCh: make(chan Request),
		ctx:       ctx,
		cancel:    cancel,
		catalog:   NewCatalog(),
		txState:   make(map[uint64]*TxState),
	}
}

// GetNextConnID returns a unique connection ID for a new connection.
func (e *Engine) GetNextConnID() uint64 {
	return e.nextConnID.Add(1)
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
			resp := e.dispatch(req)
			resp = e.ensureTxStatus(resp, req.ConnID)
			req.ResponseCh <- resp
		}
	}
}

func (e *Engine) ensureTxStatus(resp Response, connID uint64) Response {
	if resp.TxStatus == 0 {
		if tx := e.txState[connID]; tx != nil && tx.InTx {
			resp.TxStatus = 'T'
		} else {
			resp.TxStatus = 'I'
		}
	}
	return resp
}

// autoCommitIfNeeded commits any active transaction for the connection before DDL.
func (e *Engine) autoCommitIfNeeded(connID uint64) {
	tx := e.txState[connID]
	if tx == nil || !tx.InTx {
		return
	}
	// Apply buffer to catalog
	for tableName, predicates := range tx.PendingDeletes {
		for _, pred := range predicates {
			_, _ = e.catalog.DeleteRows(tableName, pred)
		}
	}
	for tableName, updates := range tx.PendingUpdates {
		for _, u := range updates {
			_, _ = e.catalog.UpdateRows(tableName, u.Predicate, u.Updater)
		}
	}
	for tableName, rows := range tx.PendingInserts {
		for _, row := range rows {
			_ = e.catalog.InsertRow(tableName, row)
		}
	}
	delete(e.txState, connID)
}

func (e *Engine) dispatch(req Request) Response {
	stmt := req.Stmt
	connID := req.ConnID
	tx := e.txState[connID]
	execCtx := &ExecCtx{ConnID: connID, Catalog: e.catalog, TxState: tx}

	switch s := stmt.(type) {
	case *tree.Select:
		return executeSelect(s, e.catalog, execCtx)
	case *tree.Insert:
		return executeInsert(s, e.catalog, execCtx)
	case *tree.Update:
		return executeUpdate(s, e.catalog, execCtx)
	case *tree.Delete:
		return executeDelete(s, e.catalog, execCtx)
	case *tree.CreateTable:
		e.autoCommitIfNeeded(connID)
		return executeCreateTable(s, e.catalog, execCtx)
	case *tree.DropTable:
		e.autoCommitIfNeeded(connID)
		return executeDropTable(s, e.catalog, execCtx)
	case *tree.BeginTransaction:
		return executeBegin(s, connID, e.txState)
	case *tree.CommitTransaction:
		return executeCommit(s, connID, e.txState, e.catalog)
	case *tree.RollbackTransaction:
		return executeRollback(s, connID, e.txState)
	default:
		return Response{
			Error: fmt.Errorf("unsupported statement type: %T", stmt),
		}
	}
}

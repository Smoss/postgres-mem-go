package server

import (
	"fmt"
	"net"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/smoss/postgres-mem-go/engine"
	"github.com/smoss/postgres-mem-go/parser"
)

// handleConnection handles a single client connection.
func handleConnection(conn net.Conn, eng *engine.Engine) {
	defer func() { _ = conn.Close() }()

	backend := pgproto3.NewBackend(conn, conn)

	// Wait for startup message
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	// Handle SSL request - we don't support it
	if _, ok := startupMsg.(*pgproto3.SSLRequest); ok {
		// Send 'N' to indicate SSL is not supported
		_, _ = conn.Write([]byte("N"))
		// Wait for the actual startup message
		startupMsg, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}

	// Cast to StartupMessage
	_, ok := startupMsg.(*pgproto3.StartupMessage)
	if !ok {
		return
	}

	// Send authentication OK (no actual authentication in Phase 1)
	backend.Send(&pgproto3.AuthenticationOk{})

	// Send parameter status messages
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "server_version",
		Value: "15.0",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "server_encoding",
		Value: "UTF8",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "client_encoding",
		Value: "UTF8",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "DateStyle",
		Value: "ISO, MDY",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "TimeZone",
		Value: "UTC",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "integer_datetimes",
		Value: "on",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "is_superuser",
		Value: "on",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "session_authorization",
		Value: "postgres",
	})
	backend.Send(&pgproto3.ParameterStatus{
		Name:  "standard_conforming_strings",
		Value: "on",
	})

	// Send ReadyForQuery to indicate we're ready for queries
	// Status byte 'I' = Idle (not in a transaction)
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if err := backend.Flush(); err != nil {
		return
	}

	// Main message handling loop
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			if err := handleQuery(backend, m.String, eng); err != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			// Send error for unsupported messages
			backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  fmt.Sprintf("unsupported message type: %T", msg),
			})
			_ = backend.Flush()
		}
	}
}

// handleQuery processes a simple query and returns appropriate responses.
func handleQuery(
	backend *pgproto3.Backend,
	sql string,
	eng *engine.Engine,
) error {
	// Parse the SQL statement
	stmt, err := parser.Parse(sql)
	if err != nil {
		// Send PostgreSQL error response for syntax error
		backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "42601", // syntax_error
			Message:  err.Error(),
		})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	}

	// Handle empty query (nil statement) - return empty CommandComplete
	// This is needed for pgx.Ping() which may send empty queries
	if stmt == nil {
		backend.Send(&pgproto3.CommandComplete{
			CommandTag: []byte("SELECT 0"),
		})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	}

	// Submit the parsed statement to the engine
	respCh := make(chan engine.Response, 1)
	eng.Submit(engine.Request{
		Stmt:       stmt,
		ConnID:     0, // TODO: assign connection IDs
		ResponseCh: respCh,
	})

	// Wait for the response
	resp := <-respCh

	// Handle engine response
	if resp.Error != nil {
		// Determine SQLSTATE based on error type
		code := "0A000" // feature_not_supported (default for Phase 2 stubs)
		backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     code,
			Message:  resp.Error.Error(),
		})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	}

	// Determine the statement type for CommandComplete tag
	commandTag := generateCommandTag(stmt, resp)

	// Send RowDescription and DataRow messages for SELECT results
	if len(resp.Rows) > 0 && len(resp.Columns) > 0 {
		// Send RowDescription
		fields := make([]pgproto3.FieldDescription, len(resp.Columns))
		for i, col := range resp.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(col.Name),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          col.TypeOID,
				DataTypeSize:         -1, // Variable size
				TypeModifier:         -1,
				Format:               0, // Text format
			}
		}
		backend.Send(&pgproto3.RowDescription{Fields: fields})

		// Send DataRow messages
		for _, row := range resp.Rows {
			values := make([][]byte, len(row))
			for i, val := range row {
				values[i] = formatValue(val)
			}
			backend.Send(&pgproto3.DataRow{Values: values})
		}
	}

	// Send CommandComplete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(commandTag),
	})

	// Send ReadyForQuery to indicate we're ready for more queries
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	return backend.Flush()
}

// generateCommandTag generates the appropriate CommandComplete tag based on statement type.
func generateCommandTag(stmt interface{}, resp engine.Response) string {
	switch stmt.(type) {
	case *tree.Select:
		return fmt.Sprintf("SELECT %d", resp.RowsAffected)
	case *tree.Insert:
		return fmt.Sprintf("INSERT 0 %d", resp.RowsAffected)
	case *tree.Update:
		return fmt.Sprintf("UPDATE %d", resp.RowsAffected)
	case *tree.Delete:
		return fmt.Sprintf("DELETE %d", resp.RowsAffected)
	case *tree.CreateTable:
		return "CREATE TABLE"
	case *tree.DropTable:
		return "DROP TABLE"
	case *tree.BeginTransaction:
		return "BEGIN"
	case *tree.CommitTransaction:
		return "COMMIT"
	case *tree.RollbackTransaction:
		return "ROLLBACK"
	default:
		return fmt.Sprintf("SELECT %d", resp.RowsAffected)
	}
}

// formatValue formats a Go value as a byte slice for the wire protocol.
func formatValue(val interface{}) []byte {
	if val == nil {
		return nil // NULL value
	}

	switch v := val.(type) {
	case int32:
		return []byte(fmt.Sprintf("%d", v))
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float64:
		return []byte(fmt.Sprintf("%g", v))
	case string:
		return []byte(v)
	case bool:
		if v {
			return []byte("t")
		}
		return []byte("f")
	case []byte:
		return v
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

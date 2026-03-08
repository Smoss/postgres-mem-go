package server

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

// handleConnection handles a single client connection.
func handleConnection(conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(conn, conn)

	// Wait for startup message
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	// Handle SSL request - we don't support it
	if _, ok := startupMsg.(*pgproto3.SSLRequest); ok {
		// Send 'N' to indicate SSL is not supported
		conn.Write([]byte("N"))
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
			if err := handleQuery(backend, m.String); err != nil {
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
			backend.Flush()
		}
	}
}

// handleQuery processes a simple query and returns appropriate responses.
func handleQuery(backend *pgproto3.Backend, sql string) error {
	// For Phase 1, we accept any query and return empty CommandComplete
	// This is enough for pgx.Ping() to work

	// Send CommandComplete with tag (e.g., "SELECT 0" or just empty for now)
	// For ping queries, we can respond with a basic acknowledgment
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 0"),
	})

	// Send ReadyForQuery to indicate we're ready for more queries
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	return backend.Flush()
}

# postgres-mem-go: Execution Plan

An in-memory database that speaks the PostgreSQL wire protocol, allowing standard
PostgreSQL drivers (pgx, lib/pq) to connect transparently. Designed as a drop-in
replacement for a real PostgreSQL instance in unit and integration tests.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Test Code (pgx / lib/pq)                                │
└────────────────────┬─────────────────────────────────────┘
                     │ TCP
┌────────────────────▼─────────────────────────────────────┐
│  postgres-mem-go                                         │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │  TCP Listener (:5432 or random port)             │    │
│  └──────────────────┬───────────────────────────────┘    │
│                     │ goroutine per connection            │
│  ┌──────────────────▼───────────────────────────────┐    │
│  │  Wire Protocol Handler (pgproto3 Backend)        │    │
│  │  - Startup / Auth handshake                      │    │
│  │  - Simple & Extended query protocol              │    │
│  └──────────────────┬───────────────────────────────┘    │
│                     │ SQL string                         │
│  ┌──────────────────▼───────────────────────────────┐    │
│  │  SQL Parser (cockroachdb-parser)                 │    │
│  │  - Returns tree.Statement typed AST              │    │
│  └──────────────────┬───────────────────────────────┘    │
│                     │ Request{stmt, responseCh}          │
│  ┌──────────────────▼───────────────────────────────┐    │
│  │  Engine Goroutine (single, owns all state)       │    │
│  │  ┌─────────────┐  ┌──────────────┐              │    │
│  │  │  Catalog     │  │  Storage     │              │    │
│  │  │  schemas     │  │  rows        │              │    │
│  │  │  tables      │  │  indexes     │              │    │
│  │  │  columns     │  │              │              │    │
│  │  └─────────────┘  └──────────────┘              │    │
│  └──────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

### Concurrency Model

Connection goroutines do NOT access catalog or storage directly. Instead, each
connection sends a typed `Request` (containing the parsed statement and a response
channel) to the engine goroutine over a shared channel. The engine goroutine
processes requests sequentially, then sends results back on the per-request response
channel. This actor model eliminates the need for mutexes and makes transaction
isolation trivial.

```
Connection 1 ──► ┐
Connection 2 ──► ├──► chan Request ──► Engine Goroutine ──► chan Response (per request)
Connection 3 ──► ┘
```

---

## Key Libraries

| Library | Purpose |
|---------|---------|
| `github.com/jackc/pgx/v5/pgproto3` | PostgreSQL wire protocol v3 message encoding/decoding. We use the `Backend` type (server-side) to handle startup, authentication, and query/response framing. |
| `github.com/cockroachdb/cockroachdb-parser` | Standalone SQL parser extracted from CockroachDB. Pure Go (no CGO), PostgreSQL-compatible grammar. Returns a rich typed AST (`tree.Statement` and subtypes). Supports deparsing via `tree.AsString()`. Apache 2.0. |

---

## Package Layout

```
postgres-mem-go/
  go.mod
  go.sum
  main.go                    # optional CLI entrypoint
  server/
    server.go                # TCP listener, accept loop, connection lifecycle
    connection.go            # per-connection wire protocol handler
  parser/
    parser.go                # thin wrapper around cockroachdb-parser
  engine/
    engine.go                # engine goroutine, request/response dispatch
    catalog.go               # schema/table/column metadata
    storage.go               # in-memory row storage (heap)
    executor_select.go       # SELECT execution
    executor_insert.go       # INSERT execution
    executor_update.go       # UPDATE execution
    executor_delete.go       # DELETE execution
    executor_ddl.go          # CREATE TABLE, DROP TABLE, ALTER TABLE
    executor_txn.go          # BEGIN / COMMIT / ROLLBACK
    types.go                 # PostgreSQL type system (OIDs, conversion, formatting)
    expr.go                  # expression evaluator (WHERE, computed columns)
    index.go                 # optional: basic B-tree / hash index on primary keys
  testutil/
    testutil.go              # helper: start server, return connection string, cleanup
```

---

## Implementation Phases

### Phase 1: Skeleton and Wire Protocol

**Goal:** A TCP server that a `pgx` client can connect to and receive a successful
startup handshake.

- [ ] Initialize Go module (`go mod init github.com/smoss/postgres-mem-go`)
- [ ] `server/server.go` — TCP listener on configurable or random port
- [ ] `server/connection.go` — handle StartupMessage, send AuthenticationOk,
      ReadyForQuery, ParameterStatus messages
- [ ] Handle the Simple Query Protocol: receive Query message, return empty
      CommandComplete + ReadyForQuery
- [ ] Integration test: `pgx.Connect(...)` succeeds against the in-memory server

**Exit criteria:** `pgx.Connect` / `pgx.Ping` pass against the server.

---

### Phase 2: SQL Parsing

**Goal:** Incoming SQL strings are parsed into typed AST nodes and routed to the
appropriate executor.

- [ ] `parser/parser.go` — wraps `parser.ParseOne(sql)` returning `tree.Statement`
- [ ] `engine/engine.go` — type-switch dispatch on parsed AST:
  - `*tree.Select` → SELECT executor
  - `*tree.Insert` → INSERT executor
  - `*tree.Update` → UPDATE executor
  - `*tree.Delete` → DELETE executor
  - `*tree.CreateTable` → DDL executor
  - `*tree.DropTable` → DDL executor
  - `*tree.BeginTransaction` / `*tree.CommitTransaction` / `*tree.RollbackTransaction` → TXN executor
- [ ] Return PostgreSQL error responses (with SQLSTATE codes) for unsupported or
      malformed SQL

**Exit criteria:** Arbitrary SQL parses without error; unknown statement types return
a clean error to the client.

---

### Phase 3: DDL — CREATE TABLE / DROP TABLE

**Goal:** Tables can be created and dropped; schema metadata is tracked.

- [ ] `engine/catalog.go` — in-memory catalog: schemas → tables → columns with
      PostgreSQL type info
- [ ] `engine/executor_ddl.go` — interpret `*tree.CreateTable` and `*tree.DropTable`
- [ ] `engine/types.go` — map PostgreSQL type names to OIDs and Go types:
      int4, int8, text, bool, float8, numeric, timestamp, timestamptz, uuid, bytea, jsonb
- [ ] Support `IF NOT EXISTS` / `IF EXISTS` modifiers
- [ ] Support column constraints: `NOT NULL`, `DEFAULT`, `PRIMARY KEY`

**Exit criteria:** Can round-trip `CREATE TABLE ... ; DROP TABLE ...` via pgx and
verify via catalog queries.

---

### Phase 4: DML — INSERT / SELECT / UPDATE / DELETE

**Goal:** Full basic CRUD against in-memory tables, with results returned over the
wire protocol.

- [ ] `engine/storage.go` — per-table row storage as `[][]Datum` (column-ordered tuples)
- [ ] `engine/executor_insert.go` — `INSERT INTO ... VALUES`, `INSERT ... RETURNING`
- [ ] `engine/executor_select.go`:
  - Table scan with column projection
  - WHERE clause filtering via `engine/expr.go` (comparison ops, AND/OR/NOT,
    IS NULL, LIKE, IN)
  - ORDER BY, LIMIT, OFFSET
  - Basic JOINs (INNER, LEFT) via nested-loop
  - Aggregate functions: COUNT, SUM, AVG, MIN, MAX with GROUP BY / HAVING
  - Subqueries (at least scalar subqueries)
- [ ] `engine/executor_update.go` — `UPDATE ... SET ... WHERE`
- [ ] `engine/executor_delete.go` — `DELETE FROM ... WHERE`
- [ ] Wire protocol responses: RowDescription + DataRow for SELECT; CommandComplete
      with row counts for INSERT/UPDATE/DELETE

**Exit criteria:** Can create a table, insert rows, query with filters/joins/aggregates,
update, and delete — all through pgx.

---

### Phase 5: Transactions and Concurrency

**Goal:** Per-connection transaction support with serialized access through a single
engine goroutine.

- [ ] Define `Request` struct: parsed statement, connection/transaction context,
      `chan Response` for the caller to block on
- [ ] Engine goroutine: reads from `chan Request`, executes against catalog/storage,
      sends result back on the per-request response channel
- [ ] `engine/executor_txn.go` — per-connection transaction state
      (autocommit by default)
- [ ] BEGIN / COMMIT / ROLLBACK with snapshot isolation (copy-on-write row sets per
      transaction); serialization through the engine goroutine makes isolation trivial
- [ ] SAVEPOINT support (stretch goal)

**Exit criteria:** Multiple concurrent connections can run transactions without data
races; BEGIN/COMMIT/ROLLBACK semantics are correct.

---

### Phase 6: Extended Query Protocol

**Goal:** Support prepared statements and parameterized queries (what pgx uses by
default).

- [ ] Handle Parse, Bind, Describe, Execute, Sync messages in `server/connection.go`
- [ ] Per-connection map of prepared statements and portals
- [ ] Parameter type inference from catalog metadata

**Exit criteria:** pgx parameterized queries (`$1`, `$2`, ...) work correctly.

---

### Phase 7: Test Utility and Polish

**Goal:** Ship a clean developer experience for test authors.

- [ ] `testutil/testutil.go`:
      ```go
      func NewTestDB(t *testing.T) (connString string, cleanup func())
      ```
      Spins up server on random port, returns connection string, tears down on cleanup.
- [ ] Support common `SET` commands (client_encoding, timezone, etc.) via session
      variables
- [ ] `pg_catalog` introspection for basic driver compatibility (`pg_type`, `pg_class`)
- [ ] `information_schema` support (tables, columns)
- [ ] Handle `\d` and other psql meta-commands that translate to catalog queries

**Exit criteria:** A user can write a Go test that calls `NewTestDB`, runs
migrations, exercises queries, and tears down — with no external dependencies.

---

## Internal Data Model

```go
type Datum interface{} // nil | int64 | float64 | string | bool | []byte | time.Time

type Column struct {
    Name       string
    TypeOID    uint32
    NotNull    bool
    Default    Datum
    PrimaryKey bool
}

type Table struct {
    Schema  string
    Name    string
    Columns []Column
    Rows    [][]Datum            // heap storage
    PKIndex map[interface{}]int  // primary key -> row index
}

type Request struct {
    Stmt       tree.Statement
    ConnID     uint64
    TxnState   *TxnState
    ResponseCh chan Response
}

type Response struct {
    Rows    [][]Datum
    Columns []Column
    RowsAffected int64
    Error   error
}
```

---

## Out of Scope (for now)

- TLS/SSL connections
- Role-based authentication (accept all connections)
- Persistent storage / WAL
- Full `pg_catalog` system tables
- Triggers, stored procedures, custom functions
- NOTIFY/LISTEN
- COPY protocol
- Replication protocol

---

## Testing Strategy

- **Unit tests** per package (parser, engine, storage)
- **Integration tests** using pgx to exercise the full stack end-to-end
- **Benchmark tests** for startup time (target: under 5ms) and basic query throughput

// Package database implements the private SQL execution and telemetry layer.
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.mau.fi/util/dbutil"
)

type transactionKey struct{}

// Event is the private representation of a completed, redacted SQL operation.
// It deliberately contains neither SQL text nor query arguments.
type Event struct {
	Operation  string
	Duration   time.Duration
	Failed     bool
	ErrorClass string
	ErrorCode  uint16
}

// ObserverFunc receives completed SQL operation measurements.
type ObserverFunc func(context.Context, Event)

// Snapshot is a cumulative telemetry snapshot.
type Snapshot struct {
	Operations           uint64
	Errors               uint64
	TotalLatency         time.Duration
	Transactions         uint64
	TransactionCommits   uint64
	TransactionRollbacks uint64
	TransactionErrors    uint64
	TransactionRetries   uint64
	ObserverPanics       uint64
}

// database is a small MariaDB-specific execution layer. Hypermeow uses
// PostgreSQL-style numbered parameters internally; prepare expands them to
// the positional parameters understood by MariaDB, including repeated ones.
type DB struct {
	RawDB    *sql.DB
	Dialect  dbutil.Dialect
	metrics  *metrics
	observer ObserverFunc
}

// New wraps a database/sql pool for use by the public adapter package.
func New(db *sql.DB, observer ObserverFunc) *DB {
	return &DB{RawDB: db, metrics: &metrics{}, observer: observer}
}

func (db *DB) executor(ctx context.Context) interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
} {
	if tx, ok := ctx.Value(transactionKey{}).(*sql.Tx); ok {
		return tx
	}
	return db.RawDB
}

func prepare(query string, args []any) (string, []any, error) {
	if !strings.Contains(query, "$") {
		return query, args, nil
	}
	var out strings.Builder
	out.Grow(len(query))
	bound := make([]any, 0, len(args))
	for i := 0; i < len(query); {
		if query[i] != '$' || i+1 >= len(query) || query[i+1] < '0' || query[i+1] > '9' {
			out.WriteByte(query[i])
			i++
			continue
		}
		j := i + 1
		for j < len(query) && query[j] >= '0' && query[j] <= '9' {
			j++
		}
		parameter, err := strconv.Atoi(query[i+1 : j])
		if err != nil || parameter < 1 || parameter > len(args) {
			return "", nil, fmt.Errorf("mdbstore: invalid SQL parameter %q", query[i:j])
		}
		out.WriteByte('?')
		bound = append(bound, args[parameter-1])
		i = j
	}
	return out.String(), bound, nil
}

func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	started := time.Now()
	operation := classifyOperation("exec", query)
	query, args, err := prepare(query, args)
	if err != nil {
		db.metrics.observe(ctx, db.observer, operation, started, err)
		return nil, err
	}
	result, err := db.executor(ctx).ExecContext(ctx, query, args...)
	db.metrics.observe(ctx, db.observer, operation, started, err)
	return result, err
}

func (db *DB) Query(ctx context.Context, query string, args ...any) (dbutil.Rows, error) {
	started := time.Now()
	operation := classifyOperation("query", query)
	query, args, err := prepare(query, args)
	if err != nil {
		db.metrics.observe(ctx, db.observer, operation, started, err)
		return nil, err
	}
	rows, err := db.executor(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		db.metrics.observe(ctx, db.observer, operation, started, err)
		return nil, err
	}
	return &observedRows{Rows: rows, ctx: ctx, db: db, operation: operation, started: started}, nil
}

func (db *DB) QueryRow(ctx context.Context, query string, args ...any) dbutil.Scannable {
	started := time.Now()
	operation := classifyOperation("query_row", query)
	query, args, err := prepare(query, args)
	if err != nil {
		// QueryRow cannot return an error directly. This deliberately executes an
		// invalid query so Scan returns a useful database error; all package-owned
		// queries are also covered by tests and cannot take this path.
		db.metrics.observe(ctx, db.observer, operation, started, err)
		return errorRow{err: err}
	}
	return &observedRow{row: db.executor(ctx).QueryRowContext(ctx, query, args...), ctx: ctx, db: db, operation: operation, started: started}
}

func classifyOperation(kind, query string) string {
	lower := strings.ToLower(query)
	for table, component := range map[string]string{
		"whatsmeow_device": "device", "whatsmeow_identity_keys": "identity",
		"whatsmeow_pre_keys": "prekey", "whatsmeow_sessions": "session",
		"whatsmeow_sender_keys": "sender_key", "whatsmeow_app_state": "app_state",
		"whatsmeow_contacts": "contact", "whatsmeow_chat_settings": "chat_settings",
		"whatsmeow_message_secrets": "message_secret", "whatsmeow_privacy_tokens": "privacy_token",
		"whatsmeow_nct_salt": "nct_salt", "whatsmeow_lid_map": "lid_map",
		"whatsmeow_event_buffer": "event_buffer", "whatsmeow_retry_buffer": "retry_buffer",
	} {
		if strings.Contains(lower, table) {
			return kind + "." + component
		}
	}
	return kind + ".database"
}

type observedRows struct {
	*sql.Rows
	ctx       context.Context
	db        *DB
	operation string
	started   time.Time
	once      sync.Once
}

func (rows *observedRows) finish(err error) {
	rows.once.Do(func() { rows.db.metrics.observe(rows.ctx, rows.db.observer, rows.operation, rows.started, err) })
}

func (rows *observedRows) Next() bool {
	ok := rows.Rows.Next()
	if !ok {
		rows.finish(rows.Rows.Err())
	}
	return ok
}

func (rows *observedRows) NextResultSet() bool {
	ok := rows.Rows.NextResultSet()
	if !ok {
		rows.finish(rows.Rows.Err())
	}
	return ok
}

func (rows *observedRows) Scan(dest ...any) error {
	err := rows.Rows.Scan(dest...)
	if err != nil {
		rows.finish(err)
	}
	return err
}

func (rows *observedRows) Columns() ([]string, error) {
	columns, err := rows.Rows.Columns()
	if err != nil {
		rows.finish(err)
	}
	return columns, err
}

func (rows *observedRows) ColumnTypes() ([]*sql.ColumnType, error) {
	types, err := rows.Rows.ColumnTypes()
	if err != nil {
		rows.finish(err)
	}
	return types, err
}

func (rows *observedRows) Close() error {
	err := rows.Rows.Close()
	if err == nil {
		err = rows.Rows.Err()
	}
	rows.finish(err)
	return err
}

type errorRow struct{ err error }

func (row errorRow) Scan(...any) error { return row.err }

type observedRow struct {
	row       *sql.Row
	ctx       context.Context
	db        *DB
	operation string
	started   time.Time
	once      sync.Once
}

func (row *observedRow) Scan(dest ...any) error {
	err := row.row.Scan(dest...)
	row.once.Do(func() { row.db.metrics.observe(row.ctx, row.db.observer, row.operation, row.started, err) })
	return err
}

func (db *DB) DoTxn(ctx context.Context, opts *sql.TxOptions, fn func(context.Context) error) error {
	if _, nested := ctx.Value(transactionKey{}).(*sql.Tx); nested {
		return fn(ctx)
	}
	for attempt := 0; attempt < 3; attempt++ {
		started := time.Now()
		db.metrics.transactions.Add(1)
		tx, err := db.RawDB.BeginTx(ctx, opts)
		if err != nil {
			db.metrics.transactionErrors.Add(1)
			db.metrics.observe(ctx, db.observer, "transaction.begin", started, err)
			return err
		}
		txCtx := context.WithValue(ctx, transactionKey{}, tx)
		err = fn(txCtx)
		if err == nil {
			err = tx.Commit()
			if err == nil {
				db.metrics.transactionCommits.Add(1)
			}
		} else {
			rollbackErr := tx.Rollback()
			db.metrics.transactionRollbacks.Add(1)
			if rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
		}
		if err != nil {
			db.metrics.transactionErrors.Add(1)
		}
		db.metrics.observe(ctx, db.observer, "transaction.complete", started, err)
		if !isRetryableTransactionError(err) || attempt == 2 {
			return err
		}
		db.metrics.transactionRetries.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 25 * time.Millisecond):
		}
	}
	return errors.New("mdbstore: unreachable transaction state")
}

func isRetryableTransactionError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1205 || mysqlErr.Number == 1213)
}

func (db *DB) Close() error { return db.RawDB.Close() }

// Observe records an operation performed outside the SQL wrapper, such as a
// health check or schema migration, using the same redacted telemetry stream.
func (db *DB) Observe(ctx context.Context, operation string, started time.Time, err error) {
	db.metrics.observe(ctx, db.observer, operation, started, err)
}

// Snapshot returns cumulative telemetry without blocking database operations.
func (db *DB) Snapshot() Snapshot { return db.metrics.snapshot() }

type metrics struct {
	operations           atomic.Uint64
	errors               atomic.Uint64
	latencyNS            atomic.Uint64
	transactions         atomic.Uint64
	transactionCommits   atomic.Uint64
	transactionRollbacks atomic.Uint64
	transactionErrors    atomic.Uint64
	transactionRetries   atomic.Uint64
	observerPanics       atomic.Uint64
}

func classifyError(err error) (failed bool, class string, code uint16) {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return false, "", 0
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return true, "mariadb", mysqlErr.Number
	}
	if errors.Is(err, context.Canceled) {
		return true, "context_canceled", 0
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, "deadline_exceeded", 0
	}
	return true, "other", 0
}

func notifyObserver(observer ObserverFunc, ctx context.Context, event Event) (panicked bool) {
	if observer == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	observer(ctx, event)
	return false
}

func (m *metrics) observe(ctx context.Context, observer ObserverFunc, operation string, started time.Time, err error) {
	duration := time.Since(started)
	failed, class, code := classifyError(err)
	m.operations.Add(1)
	m.latencyNS.Add(uint64(duration))
	if failed {
		m.errors.Add(1)
	}
	if notifyObserver(observer, ctx, Event{
		Operation: operation, Duration: duration, Failed: failed, ErrorClass: class, ErrorCode: code,
	}) {
		m.observerPanics.Add(1)
	}
}

func (m *metrics) snapshot() Snapshot {
	return Snapshot{
		Operations: m.operations.Load(), Errors: m.errors.Load(), TotalLatency: time.Duration(m.latencyNS.Load()),
		Transactions: m.transactions.Load(), TransactionCommits: m.transactionCommits.Load(),
		TransactionRollbacks: m.transactionRollbacks.Load(), TransactionErrors: m.transactionErrors.Load(),
		TransactionRetries: m.transactionRetries.Load(), ObserverPanics: m.observerPanics.Load(),
	}
}

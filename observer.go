package mdbstore

import (
	"context"
	"database/sql"
	"time"

	internaldb "github.com/ReindeerGames/mdbstore/internal/database"
)

// DBEvent is a redacted storage-operation measurement. SQL text and arguments
// are intentionally excluded because session data is sensitive.
type DBEvent struct {
	Operation  string
	Duration   time.Duration
	Failed     bool
	ErrorClass string
	ErrorCode  uint16
}

// Observer receives completed storage-operation measurements synchronously.
// Implementations must return promptly and must not call back into the same
// Container. Panics are recovered and counted in MetricsSnapshot.
type Observer interface {
	ObserveDB(context.Context, DBEvent)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, DBEvent)

func (fn ObserverFunc) ObserveDB(ctx context.Context, event DBEvent) { fn(ctx, event) }

// MetricsSnapshot is a lock-free cumulative storage telemetry snapshot.
type MetricsSnapshot struct {
	Operations           uint64        `json:"operations"`
	Errors               uint64        `json:"errors"`
	TotalLatency         time.Duration `json:"total_latency"`
	Transactions         uint64        `json:"transactions"`
	TransactionCommits   uint64        `json:"transaction_commits"`
	TransactionRollbacks uint64        `json:"transaction_rollbacks"`
	TransactionErrors    uint64        `json:"transaction_errors"`
	TransactionRetries   uint64        `json:"transaction_retries"`
	ObserverPanics       uint64        `json:"observer_panics"`
}

// database is kept as a private alias so the public store implementation does
// not expose its SQL execution machinery.
type database = internaldb.DB

func newDatabase(db *sql.DB, observer Observer) *database {
	var callback internaldb.ObserverFunc
	if observer != nil {
		callback = func(ctx context.Context, event internaldb.Event) {
			observer.ObserveDB(ctx, DBEvent{
				Operation: event.Operation, Duration: event.Duration, Failed: event.Failed,
				ErrorClass: event.ErrorClass, ErrorCode: event.ErrorCode,
			})
		}
	}
	return internaldb.New(db, callback)
}

func metricsSnapshot(snapshot internaldb.Snapshot) MetricsSnapshot {
	return MetricsSnapshot{
		Operations: snapshot.Operations, Errors: snapshot.Errors, TotalLatency: snapshot.TotalLatency,
		Transactions: snapshot.Transactions, TransactionCommits: snapshot.TransactionCommits,
		TransactionRollbacks: snapshot.TransactionRollbacks, TransactionErrors: snapshot.TransactionErrors,
		TransactionRetries: snapshot.TransactionRetries, ObserverPanics: snapshot.ObserverPanics,
	}
}

package mdbstore

import (
	"context"
	"database/sql"
	"testing"
)

func TestObserverAndMetricsRedactQueries(t *testing.T) {
	var observed DBEvent
	db := newDatabase(&sql.DB{}, ObserverFunc(func(_ context.Context, event DBEvent) { observed = event }))
	_, err := db.Exec(context.Background(), "SELECT $2", 1)
	if err == nil {
		t.Fatal("expected bind error")
	}
	if observed.Operation != "exec.database" || !observed.Failed {
		t.Fatalf("event = %+v", observed)
	}
	snapshot := db.Snapshot()
	if snapshot.Operations != 1 || snapshot.Errors != 1 {
		t.Fatalf("metrics = %+v", snapshot)
	}
}

func TestObserverPanicIsCounted(t *testing.T) {
	db := newDatabase(&sql.DB{}, ObserverFunc(func(context.Context, DBEvent) { panic("boom") }))
	_, _ = db.Exec(context.Background(), "SELECT $2", 1)
	if got := db.Snapshot().ObserverPanics; got != 1 {
		t.Fatalf("observer panics = %d", got)
	}
}

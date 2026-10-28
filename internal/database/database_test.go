package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
)

type testDriver struct{}

func (testDriver) Open(string) (driver.Conn, error) { return nil, errors.New("not supported") }

type beginFailureConnector struct{}

func (beginFailureConnector) Connect(context.Context) (driver.Conn, error) {
	return beginFailureConn{}, nil
}
func (beginFailureConnector) Driver() driver.Driver { return testDriver{} }

type beginFailureConn struct{}

func (beginFailureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (beginFailureConn) Close() error              { return nil }
func (beginFailureConn) Begin() (driver.Tx, error) { return nil, errors.New("begin failed") }

type rowErrorConnector struct{}

func (rowErrorConnector) Connect(context.Context) (driver.Conn, error) { return rowErrorConn{}, nil }
func (rowErrorConnector) Driver() driver.Driver                        { return testDriver{} }

type rowErrorConn struct{}

func (rowErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (rowErrorConn) Close() error              { return nil }
func (rowErrorConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (rowErrorConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return rowErrorRows{}, nil
}

type rowErrorRows struct{}

func (rowErrorRows) Columns() []string         { return []string{"value"} }
func (rowErrorRows) Close() error              { return nil }
func (rowErrorRows) Next([]driver.Value) error { return errors.New("stream failed") }

func TestPrepareExpandsNumberedAndRepeatedParameters(t *testing.T) {
	query, args, err := prepare("INSERT INTO t VALUES ($1,$2),($1,$3)", []any{"owner", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if query != "INSERT INTO t VALUES (?,?),(?,?)" {
		t.Fatalf("query = %q", query)
	}
	want := []any{"owner", "a", "owner", "b"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestPrepareRejectsMissingParameter(t *testing.T) {
	if _, _, err := prepare("SELECT $2", []any{1}); err == nil {
		t.Fatal("expected error")
	}
}

func TestTransactionBeginFailureIsObserved(t *testing.T) {
	raw := sql.OpenDB(beginFailureConnector{})
	t.Cleanup(func() { _ = raw.Close() })
	db := New(raw, nil)
	if err := db.DoTxn(context.Background(), nil, func(context.Context) error { return nil }); err == nil {
		t.Fatal("expected begin failure")
	}
	got := db.Snapshot()
	if got.Transactions != 1 || got.TransactionErrors != 1 || got.Errors != 1 {
		t.Fatalf("metrics = %+v", got)
	}
}

func TestRowIterationFailureIsObserved(t *testing.T) {
	raw := sql.OpenDB(rowErrorConnector{})
	t.Cleanup(func() { _ = raw.Close() })
	db := New(raw, nil)
	rows, err := db.Query(context.Background(), "SELECT value FROM whatsmeow_sessions")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("unexpected row")
	}
	if rows.Err() == nil {
		t.Fatal("expected row error")
	}
	got := db.Snapshot()
	if got.Operations != 1 || got.Errors != 1 {
		t.Fatalf("metrics = %+v", got)
	}
}

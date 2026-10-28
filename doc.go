// Package mdbstore provides a MariaDB-backed implementation of Hypermeow's
// complete session storage interfaces.
//
// New is the usual entry point. It owns the returned database connection pool,
// validates the server as MariaDB, and applies schema migrations before use.
package mdbstore

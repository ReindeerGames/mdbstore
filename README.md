# mdbstore

`mdbstore` is a MariaDB-backed session store for
[Hypermeow](https://github.com/polymorfa/hypermeow). It implements Hypermeow's
device container and all session-specific storage interfaces for applications
that need durable WhatsApp session state in MariaDB.

The module path is `github.com/ReindeerGames/mdbstore`.

> Hypermeow is an unofficial WhatsApp client. This project is not affiliated
> with, authorized by, or endorsed by WhatsApp or Meta.

## Compatibility

- Go 1.26.10 or newer. Earlier Go 1.26 patch releases contain standard-library
  vulnerabilities reachable through TLS certificate verification.
- MariaDB 11.4 LTS is continuously tested. MySQL is intentionally rejected
  because its behavior is not part of this adapter's compatibility contract.
- Hypermeow commit `7852ff1c171870a1fe43ce366799a00385572fd9` is pinned and
  compile-time interface assertions protect the complete store surface.

The schema covers device credentials, Signal identities, sessions, prekeys,
sender keys, app state, contacts, chat settings, message secrets, privacy
tokens, NCT salt, LID mappings, decryption buffering, and retry buffering.

## Repository layout

The module root is the public `mdbstore` package. Its files are grouped by the
API or Hypermeow store area they implement. Private implementation details are
kept below `internal/`:

- `internal/database` translates Hypermeow's SQL parameters, manages
  transactions, and records redacted storage telemetry.
- `internal/schema` owns schema validation and embedded, versioned MariaDB
  migrations.

This keeps the public import path stable and prevents applications from
depending on migration or SQL-execution internals.

## Installation

```sh
go get github.com/ReindeerGames/mdbstore
```

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/ReindeerGames/mdbstore"
	whatsmeow "github.com/polymorfa/hypermeow"
)

func main() {
	ctx := context.Background()
	container, err := mdbstore.New(
		ctx,
		"whatsapp:secret@tcp(db.example:3306)/whatsapp?tls=true",
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer container.Close()

	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		log.Fatal(err)
	}
	client := whatsmeow.NewClient(device, nil)
	_ = client
}
```

`New` parses the standard
[`go-sql-driver/mysql` DSN](https://github.com/go-sql-driver/mysql#dsn-data-source-name),
adds bounded network timeouts when they are omitted, configures the connection
pool, verifies that the server is MariaDB, and applies schema migrations under
a database-qualified advisory lock.

Use `NewWithConfig` to change pool and migration settings. Start with
`NewDefaultConfig`; a zero `Config` also selects all defaults. In a non-zero
configuration, zero idle connections and zero connection lifetimes are valid
and disable those features; use negative values to restore their defaults.

`NewWithDB` borrows an existing `*sql.DB` without changing its pool settings.
`NewWithDBConfig` borrows it and applies the supplied pool settings. Neither
constructor takes ownership: call `Upgrade` before use and close the pool in
the application. `Container.Close` only closes pools created by `New` or
`NewWithConfig`.

## Database setup

Use a dedicated database and account. The account needs DDL permission while
migrations run and DML permission while the service runs. Deployments that
separate migration and runtime identities can call `Upgrade` during a
privileged release step, then construct a borrowed container with a DML-only
account and call `Validate`. `Validate` performs the complete server, version,
and structural checks without executing DDL.

Production connections should use TLS with certificate verification. Keep
DSNs in a secret manager and never log them. MariaDB should use InnoDB and a
durability policy appropriate to the application's recovery requirements.

Migrations are embedded in the module. Every startup validates required
tables, columns, primary keys, indexes, cascading foreign keys, key-material
types, and exact-length constraints. A partial or incompatible schema fails
closed rather than being accepted because a version row exists.

## Concurrency and consistency

The connection pool and store implementations are safe for concurrent use.
Deadlocks and lock-wait timeouts are retried up to three times with bounded
backoff.

Run only one active Hypermeow client for a given device. This is a WhatsApp
session ownership requirement, not a MariaDB limitation. Multiple processes
may host different devices in the same database. Global LID mappings are read
authoritatively from MariaDB so updates made by another process are visible.

Large reads, writes, and deletes are chunked to remain below MariaDB parameter
and packet limits while transactionally preserving operations that must be
atomic.

## Health and observability

- `Ping` measures database reachability.
- `SchemaVersion` reports the installed adapter schema version.
- `DBStats` exposes the standard connection-pool statistics.
- `Metrics` returns cumulative operation, error, latency, transaction, retry,
  rollback, commit, and observer-panic counters.
- `Config.Observer` receives redacted operation events suitable for adapting
  to Prometheus or OpenTelemetry. Events contain safe logical operation names,
  duration, failure class, and MariaDB error number; SQL, arguments, session
  data, and raw database errors are excluded.

Observers run synchronously and must return promptly. They must not call back
into the same container. Panics are recovered and exposed in `Metrics`.

Recommended alerts include database errors, pool waits, failed or retried
transactions, connection saturation, schema validation failures, InnoDB
deadlocks, lock waits, storage growth, backup failure, and repeated client
disconnect or logout events.

## Testing

Fast checks:

```sh
go test -race ./...
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

The MariaDB integration test is enabled by setting `MDBSTORE_TEST_DSN`:

```sh
MDBSTORE_TEST_DSN='user:password@tcp(127.0.0.1:3306)/mdbstore_test' \
  go test -race ./...
```

Use an isolated disposable database. The integration test exercises migrations
and every Hypermeow storage family and removes its device data on success.

## Operational guidance

- Back up MariaDB and regularly prove restoration into a clean server.
- Monitor binary-log, disk, table, and index growth.
- Pin explicit `mdbstore` and Hypermeow versions; review protocol and schema
  changes before upgrading.
- Use graceful shutdown so Hypermeow disconnects before the database pool is
  closed.
- Never log QR data, pairing codes, session keys, message plaintext, uploaded
  media, SQL arguments, credentials, or full DSNs.

## License

Adapter files derived from Hypermeow's SQL store retain their Mozilla Public
License 2.0 notices. Upstream-derived tests covered by MIT terms identify that
license explicitly. See [LICENSE](LICENSE), [LICENSE-MIT](LICENSE-MIT), and
[NOTICE](NOTICE).

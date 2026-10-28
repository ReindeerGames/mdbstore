package mdbstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	internalSchema "github.com/ReindeerGames/mdbstore/internal/schema"
)

// LatestSchemaVersion is the schema version required by this adapter release.
const LatestSchemaVersion = internalSchema.LatestVersion

// Upgrade upgrades the database from its current schema to the latest version.
func (c *Container) Upgrade(ctx context.Context) error {
	if c == nil || c.db == nil || c.db.RawDB == nil {
		return errors.New("mdbstore: nil database")
	}
	if err := c.db.RawDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping MariaDB: %w", err)
	}
	started := time.Now()
	err := internalSchema.Upgrade(ctx, c.db.RawDB, c.config.MigrationLockWait)
	c.db.Observe(ctx, "migration.upgrade", started, err)
	return err
}

// Validate verifies the MariaDB server, installed schema version, and complete
// schema structure without applying DDL. It is suitable for startup checks
// performed with a DML-only runtime identity after a privileged migration step.
func (c *Container) Validate(ctx context.Context) error {
	if c == nil || c.db == nil || c.db.RawDB == nil {
		return errors.New("mdbstore: nil database")
	}
	started := time.Now()
	err := internalSchema.Validate(ctx, c.db.RawDB)
	c.db.Observe(ctx, "schema.validate", started, err)
	return err
}

// SchemaVersion returns the adapter schema version currently stored in MariaDB.
func (c *Container) SchemaVersion(ctx context.Context) (int, error) {
	var version sql.NullInt64
	if c == nil || c.db == nil || c.db.RawDB == nil {
		return 0, errors.New("mdbstore: nil database")
	}
	err := c.db.QueryRow(ctx, "SELECT MAX(version) FROM mdbstore_schema_version").Scan(&version)
	if err != nil {
		return 0, err
	}
	if version.Int64 > int64(^uint(0)>>1) {
		return 0, errors.New("schema version overflows int")
	}
	return int(version.Int64), nil
}

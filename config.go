package mdbstore

import "time"

// Config controls connection pooling and schema migration behavior.
type Config struct {
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	ConnMaxIdleTime   time.Duration
	MigrationLockWait time.Duration
	Observer          Observer
}

var defaultConfig = Config{
	MaxOpenConns:      32,
	MaxIdleConns:      8,
	ConnMaxLifetime:   3 * time.Minute,
	ConnMaxIdleTime:   time.Minute,
	MigrationLockWait: 30 * time.Second,
}

// NewDefaultConfig returns an independent configuration suitable for a
// service hosting multiple active sessions. Returning a copy prevents global
// configuration mutations and data races between callers.
func NewDefaultConfig() Config { return defaultConfig }

func (c Config) normalized() Config {
	zeroTuning := c.MaxOpenConns == 0 && c.MaxIdleConns == 0 &&
		c.ConnMaxLifetime == 0 && c.ConnMaxIdleTime == 0 && c.MigrationLockWait == 0
	if zeroTuning {
		defaults := NewDefaultConfig()
		defaults.Observer = c.Observer
		return defaults
	}
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = defaultConfig.MaxOpenConns
	}
	if c.MaxIdleConns < 0 {
		c.MaxIdleConns = defaultConfig.MaxIdleConns
	}
	if c.ConnMaxLifetime < 0 {
		c.ConnMaxLifetime = defaultConfig.ConnMaxLifetime
	}
	if c.ConnMaxIdleTime < 0 {
		c.ConnMaxIdleTime = defaultConfig.ConnMaxIdleTime
	}
	if c.MigrationLockWait <= 0 {
		c.MigrationLockWait = defaultConfig.MigrationLockWait
	}
	if c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	}
	return c
}

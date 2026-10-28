// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mdbstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	mathRand "math/rand/v2"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/random"

	"github.com/polymorfa/hypermeow/proto/waAdv"
	"github.com/polymorfa/hypermeow/store"
	"github.com/polymorfa/hypermeow/types"
	"github.com/polymorfa/hypermeow/util/keys"
	waLog "github.com/polymorfa/hypermeow/util/log"
)

// Container is a wrapper for a SQL database that can contain multiple whatsmeow sessions.
type Container struct {
	db     *database
	log    waLog.Logger
	config Config
	ownsDB bool
	LIDMap *CachedLIDMap
}

var _ store.DeviceContainer = (*Container)(nil)

// New opens a MariaDB-backed Hypermeow session container and applies pending
// schema migrations. The DSN uses github.com/go-sql-driver/mysql syntax.
func New(ctx context.Context, dsn string, log waLog.Logger) (*Container, error) {
	return NewWithConfig(ctx, dsn, log, NewDefaultConfig())
}

// NewWithConfig is New with explicit connection-pool and migration settings.
func NewWithConfig(ctx context.Context, dsn string, log waLog.Logger, config Config) (*Container, error) {
	driverConfig, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse MariaDB DSN: %w", err)
	}
	if driverConfig.Timeout == 0 {
		driverConfig.Timeout = 10 * time.Second
	}
	if driverConfig.ReadTimeout == 0 {
		driverConfig.ReadTimeout = 30 * time.Second
	}
	if driverConfig.WriteTimeout == 0 {
		driverConfig.WriteTimeout = 30 * time.Second
	}
	driverConfig.ParseTime = true
	driverConfig.Collation = "utf8mb4_bin"
	driverConfig.RejectReadOnly = true
	db, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open MariaDB: %w", err)
	}
	container := newContainerWithDB(db, log, config, true, true)
	err = container.Upgrade(ctx)
	if err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("failed to upgrade database: %w", err)
	}
	return container, nil
}

// NewWithDB borrows an existing MariaDB connection pool without changing its
// settings. It does not run migrations; call Upgrade before serving clients.
// Close does not close a borrowed pool; the caller retains ownership.
func NewWithDB(db *sql.DB, log waLog.Logger) *Container {
	return newContainerWithDB(db, log, Config{}, false, false)
}

// NewWithDBConfig borrows an existing pool and applies the explicit pool
// settings. Close does not close the borrowed pool.
func NewWithDBConfig(db *sql.DB, log waLog.Logger, config Config) *Container {
	return newContainerWithDB(db, log, config, true, false)
}

func newContainerWithDB(db *sql.DB, log waLog.Logger, config Config, configurePool, ownsDB bool) *Container {
	if db == nil {
		panic("mdbstore: nil *sql.DB")
	}
	if log == nil {
		log = waLog.Noop
	}
	config = config.normalized()
	if configurePool && db != nil {
		db.SetMaxOpenConns(config.MaxOpenConns)
		db.SetMaxIdleConns(config.MaxIdleConns)
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
		db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	}
	wrapped := newDatabase(db, config.Observer)
	return &Container{
		db:     wrapped,
		log:    log,
		config: config,
		ownsDB: ownsDB,
		LIDMap: newCachedLIDMap(wrapped),
	}
}

// DBStats returns a snapshot of the underlying database/sql connection pool.
func (c *Container) DBStats() sql.DBStats { return c.db.RawDB.Stats() }

// Metrics returns cumulative, redacted storage-operation telemetry.
func (c *Container) Metrics() MetricsSnapshot { return metricsSnapshot(c.db.Snapshot()) }

// Ping verifies that MariaDB is reachable.
func (c *Container) Ping(ctx context.Context) error {
	if c == nil || c.db == nil || c.db.RawDB == nil {
		return errors.New("mdbstore: nil database")
	}
	started := time.Now()
	err := c.db.RawDB.PingContext(ctx)
	c.db.Observe(ctx, "health.ping", started, err)
	return err
}

const getAllDevicesQuery = `
SELECT jid, lid, registration_id, noise_key, identity_key,
       signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
       adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
       platform, business_name, push_name, facebook_uuid, lid_migration_ts, companion_meta_nonce
FROM whatsmeow_device
`

const getDeviceQuery = getAllDevicesQuery + " WHERE jid=$1"

func (c *Container) scanDevice(row dbutil.Scannable) (*store.Device, error) {
	var device store.Device
	device.Log = c.log
	device.SignedPreKey = &keys.PreKey{}
	var noisePriv, identityPriv, preKeyPriv, preKeySig []byte
	var account waAdv.ADVSignedDeviceIdentity
	var fbUUID uuid.NullUUID

	err := row.Scan(
		&device.ID, &device.LID, &device.RegistrationID, &noisePriv, &identityPriv,
		&preKeyPriv, &device.SignedPreKey.KeyID, &preKeySig,
		&device.AdvSecretKey, &account.Details, &account.AccountSignature, &account.AccountSignatureKey, &account.DeviceSignature,
		&device.Platform, &device.BusinessName, &device.PushName, &fbUUID, &device.LIDMigrationTimestamp, &device.CompanionMetaNonce)
	if err != nil {
		return nil, fmt.Errorf("failed to scan session: %w", err)
	} else if len(noisePriv) != 32 || len(identityPriv) != 32 || len(preKeyPriv) != 32 || len(preKeySig) != 64 {
		return nil, ErrInvalidLength
	}

	device.NoiseKey = keys.NewKeyPairFromPrivateKey(*(*[32]byte)(noisePriv))
	device.IdentityKey = keys.NewKeyPairFromPrivateKey(*(*[32]byte)(identityPriv))
	device.SignedPreKey.KeyPair = *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(preKeyPriv))
	device.SignedPreKey.Signature = (*[64]byte)(preKeySig)
	device.Account = &account
	device.FacebookUUID = fbUUID.UUID

	c.initializeDevice(&device)

	return &device, nil
}

// GetAllDevices finds all the devices in the database.
func (c *Container) GetAllDevices(ctx context.Context) ([]*store.Device, error) {
	res, err := c.db.Query(ctx, getAllDevicesQuery)
	return dbutil.NewRowIterWithError(res, c.scanDevice, err).AsList()
}

// GetFirstDevice is a convenience method for getting the first device in the store. If there are
// no devices, then a new device will be created. You should only use this if you don't want to
// have multiple sessions simultaneously.
func (c *Container) GetFirstDevice(ctx context.Context) (*store.Device, error) {
	devices, err := c.GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	if len(devices) == 0 {
		return c.NewDevice(), nil
	} else {
		return devices[0], nil
	}
}

// GetDevice finds the device with the specified JID in the database.
//
// If the device is not found, nil is returned instead.
//
// Note that the parameter usually must be an AD-JID.
func (c *Container) GetDevice(ctx context.Context, jid types.JID) (*store.Device, error) {
	sess, err := c.scanDevice(c.db.QueryRow(ctx, getDeviceQuery, jid))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return sess, err
}

const (
	insertDeviceQuery = `
		INSERT INTO whatsmeow_device (jid, lid, registration_id, noise_key, identity_key,
									  signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
									  adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
									  platform, business_name, push_name, facebook_uuid, lid_migration_ts, companion_meta_nonce)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		ON DUPLICATE KEY UPDATE lid=VALUES(lid),
				platform=VALUES(platform),
				business_name=VALUES(business_name),
				push_name=VALUES(push_name),
				lid_migration_ts=VALUES(lid_migration_ts),
				companion_meta_nonce=VALUES(companion_meta_nonce)
	`
	deleteDeviceQuery = `DELETE FROM whatsmeow_device WHERE jid=$1`
)

// NewDevice creates a new device in this database.
//
// No data is actually stored before Save is called. However, the pairing process will automatically
// call Save after a successful pairing, so you most likely don't need to call it yourself.
func (c *Container) NewDevice() *store.Device {
	device := &store.Device{
		Log:       c.log,
		Container: c,

		NoiseKey:       keys.NewKeyPair(),
		IdentityKey:    keys.NewKeyPair(),
		RegistrationID: mathRand.Uint32(),
		AdvSecretKey:   random.Bytes(32),
	}
	device.SignedPreKey = device.IdentityKey.CreateSignedPreKey(1)
	return device
}

// ErrDeviceIDMustBeSet is the error returned by PutDevice if you try to save a device before knowing its JID.
var ErrDeviceIDMustBeSet = errors.New("device JID must be known before accessing database")

// Close closes pools opened by New. Borrowed pools passed to NewWithDB or
// NewWithDBConfig remain owned by their caller.
func (c *Container) Close() error {
	if c != nil && c.db != nil && c.ownsDB {
		return c.db.Close()
	}
	return nil
}

// PutDevice stores the given device in this database. This should be called through Device.Save()
// (which usually doesn't need to be called manually, as the library does that automatically when relevant).
func (c *Container) PutDevice(ctx context.Context, device *store.Device) error {
	if device.ID == nil {
		return ErrDeviceIDMustBeSet
	}
	_, err := c.db.Exec(ctx, insertDeviceQuery,
		device.ID, device.LID, device.RegistrationID, device.NoiseKey.Priv[:], device.IdentityKey.Priv[:],
		device.SignedPreKey.Priv[:], device.SignedPreKey.KeyID, device.SignedPreKey.Signature[:],
		device.AdvSecretKey, device.Account.Details, device.Account.AccountSignature, device.Account.AccountSignatureKey, device.Account.DeviceSignature,
		device.Platform, device.BusinessName, device.PushName, uuid.NullUUID{UUID: device.FacebookUUID, Valid: device.FacebookUUID != uuid.Nil},
		device.LIDMigrationTimestamp, device.CompanionMetaNonce,
	)

	if !device.Initialized {
		c.initializeDevice(device)
	}
	return err
}

func (c *Container) initializeDevice(device *store.Device) {
	innerStore := NewSQLStore(c, *device.ID)
	device.SetAllStores(innerStore)
	device.LIDs = c.LIDMap
	device.Container = c
	device.Initialized = true
}

// DeleteDevice deletes the given device from this database. This should be called through Device.Delete()
func (c *Container) DeleteDevice(ctx context.Context, store *store.Device) error {
	if store.ID == nil {
		return ErrDeviceIDMustBeSet
	}
	_, err := c.db.Exec(ctx, deleteDeviceQuery, store.ID)
	return err
}

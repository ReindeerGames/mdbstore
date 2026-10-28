// Package schema owns the adapter's embedded MariaDB schema and migrations.
package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// LatestVersion is the schema version required by this adapter release.
const LatestVersion = 2

var migrationPaths = [...]string{"migrations/001_initial.sql", "migrations/002_integrity_constraints.sql"}

var migration2StatementChecks = [][]string{
	{"chk_device_registration_id", "chk_device_signed_pre_key_id", "chk_device_noise_key", "chk_device_identity_key", "chk_device_signed_pre_key", "chk_device_signed_pre_key_sig", "chk_device_adv_account_sig", "chk_device_adv_account_sig_key", "chk_device_adv_device_sig"},
	{"chk_identity_key"},
	{"chk_prekey_id", "chk_prekey_data"},
	{"chk_app_state_hash"},
	{"chk_mutation_index_mac", "chk_mutation_value_mac"},
	{"chk_event_ciphertext_hash"},
}

// Upgrade applies all pending migrations while holding a database-scoped
// advisory lock, then verifies the resulting schema.
func Upgrade(ctx context.Context, db *sql.DB, migrationLockWait time.Duration) (returnErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Close()

	var serverVersion string
	if err = conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&serverVersion); err != nil {
		return fmt.Errorf("read database version: %w", err)
	}
	if !strings.Contains(strings.ToLower(serverVersion), "mariadb") {
		return fmt.Errorf("mdbstore requires MariaDB, server reported %q", serverVersion)
	}
	var databaseName string
	if err = conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&databaseName); err != nil {
		return fmt.Errorf("read database name: %w", err)
	}
	if databaseName == "" {
		return errors.New("mdbstore requires a database name in the DSN")
	}

	waitSeconds := max(1, int(migrationLockWait.Seconds()))
	lockName := fmt.Sprintf("mdbstore:migrate:%x", sha256.Sum256([]byte(databaseName)))[:63]
	var locked sql.NullInt64
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, waitSeconds).Scan(&locked); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if !locked.Valid || locked.Int64 != 1 {
		return errors.New("timed out acquiring mdbstore schema migration lock")
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var released sql.NullInt64
		releaseErr := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released)
		if releaseErr != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("release migration lock: %w", releaseErr))
		} else if !released.Valid || released.Int64 != 1 {
			returnErr = errors.Join(returnErr, errors.New("migration lock was not held when released"))
		}
	}()

	if _, err = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS mdbstore_schema_version (
		version INT UNSIGNED NOT NULL PRIMARY KEY,
		applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	) ENGINE=InnoDB`); err != nil {
		return fmt.Errorf("create schema version table: %w", err)
	}
	var current sql.NullInt64
	if err = conn.QueryRowContext(ctx, "SELECT MAX(version) FROM mdbstore_schema_version").Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	version := int(current.Int64)
	if version > LatestVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version, LatestVersion)
	}
	for next := version + 1; next <= LatestVersion; next++ {
		path := migrationPaths[next-1]
		contents, readErr := migrationFiles.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read migration %d: %w", next, readErr)
		}
		statements := splitSQLStatements(string(contents))
		if len(statements) == 0 && strings.TrimSpace(string(contents)) != "" {
			return fmt.Errorf("parse migration %d: unterminated SQL quote or comment", next)
		}
		for statementNumber, statement := range statements {
			if next == 2 {
				applied, checkErr := migration2StatementApplied(ctx, conn, databaseName, statementNumber)
				if checkErr != nil {
					return fmt.Errorf("inspect migration %d statement %d: %w", next, statementNumber+1, checkErr)
				}
				if applied {
					continue
				}
			}
			if _, err = conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply migration %d statement %d: %w", next, statementNumber+1, err)
			}
		}
		if _, err = conn.ExecContext(ctx, "INSERT INTO mdbstore_schema_version (version) VALUES (?)", next); err != nil {
			return fmt.Errorf("record migration %d: %w", next, err)
		}
	}
	if err = validateSchema(ctx, conn, databaseName); err != nil {
		return fmt.Errorf("validate schema version %d: %w", LatestVersion, err)
	}
	return nil
}

func migration2StatementApplied(ctx context.Context, conn *sql.Conn, databaseName string, statementNumber int) (bool, error) {
	if statementNumber < 0 || statementNumber >= len(migration2StatementChecks) {
		return false, errors.New("unexpected integrity migration statement")
	}
	wanted := migration2StatementChecks[statementNumber]
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(wanted)), ",")
	args := make([]any, 0, len(wanted)+1)
	args = append(args, databaseName)
	for _, name := range wanted {
		args = append(args, name)
	}
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND CONSTRAINT_TYPE='CHECK' AND CONSTRAINT_NAME IN (%s)`, placeholders)
	if err := conn.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	if count != len(wanted) {
		return false, fmt.Errorf("partially applied integrity constraints: found %d of %d", count, len(wanted))
	}
	return true, nil
}

var expectedColumns = map[string][]string{
	"mdbstore_schema_version":           {"version", "applied_at"},
	"whatsmeow_device":                  {"jid", "lid", "facebook_uuid", "registration_id", "noise_key", "identity_key", "signed_pre_key", "signed_pre_key_id", "signed_pre_key_sig", "adv_key", "adv_details", "adv_account_sig", "adv_account_sig_key", "adv_device_sig", "platform", "business_name", "push_name", "lid_migration_ts", "companion_meta_nonce"},
	"whatsmeow_identity_keys":           {"our_jid", "their_id", "identity"},
	"whatsmeow_pre_keys":                {"jid", "key_id", "prekey", "uploaded"},
	"whatsmeow_sessions":                {"our_jid", "their_id", "session"},
	"whatsmeow_sender_keys":             {"our_jid", "chat_id", "sender_id", "sender_key"},
	"whatsmeow_app_state_sync_keys":     {"jid", "key_id", "key_data", "timestamp", "fingerprint"},
	"whatsmeow_app_state_version":       {"jid", "name", "version", "hash"},
	"whatsmeow_app_state_mutation_macs": {"jid", "name", "version", "index_mac", "value_mac"},
	"whatsmeow_contacts":                {"our_jid", "their_jid", "first_name", "full_name", "push_name", "business_name", "redacted_phone", "username"},
	"whatsmeow_chat_settings":           {"our_jid", "chat_jid", "muted_until", "pinned", "archived", "wasa_root_secret_id"},
	"whatsmeow_message_secrets":         {"our_jid", "chat_jid", "sender_jid", "message_id", "secret"},
	"whatsmeow_privacy_tokens":          {"our_jid", "their_jid", "token", "timestamp", "sender_timestamp"},
	"whatsmeow_nct_salt":                {"our_jid", "salt"},
	"whatsmeow_lid_map":                 {"lid", "pn"},
	"whatsmeow_event_buffer":            {"our_jid", "ciphertext_hash", "plaintext", "server_timestamp", "insert_timestamp"},
	"whatsmeow_retry_buffer":            {"our_jid", "chat_jid", "message_id", "format", "plaintext", "timestamp"},
}

var expectedChecks = map[string]string{
	"chk_device_registration_id":     "registration_id < 4294967296",
	"chk_device_signed_pre_key_id":   "signed_pre_key_id < 16777216",
	"chk_device_noise_key":           "OCTET_LENGTH(noise_key) = 32",
	"chk_device_identity_key":        "OCTET_LENGTH(identity_key) = 32",
	"chk_device_signed_pre_key":      "OCTET_LENGTH(signed_pre_key) = 32",
	"chk_device_signed_pre_key_sig":  "OCTET_LENGTH(signed_pre_key_sig) = 64",
	"chk_device_adv_account_sig":     "OCTET_LENGTH(adv_account_sig) = 64",
	"chk_device_adv_account_sig_key": "OCTET_LENGTH(adv_account_sig_key) = 32",
	"chk_device_adv_device_sig":      "OCTET_LENGTH(adv_device_sig) = 64",
	"chk_identity_key":               "OCTET_LENGTH(identity) = 32",
	"chk_prekey_id":                  "key_id < 16777216",
	"chk_prekey_data":                "OCTET_LENGTH(prekey) = 32",
	"chk_app_state_hash":             "OCTET_LENGTH(hash) = 128",
	"chk_mutation_index_mac":         "OCTET_LENGTH(index_mac) = 32",
	"chk_mutation_value_mac":         "OCTET_LENGTH(value_mac) = 32",
	"chk_event_ciphertext_hash":      "OCTET_LENGTH(ciphertext_hash) = 32",
}

var expectedForeignKeys = []struct {
	name, table, referencedTable string
	columns, referencedColumns   []string
}{
	{"fk_identity_device", "whatsmeow_identity_keys", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_prekey_device", "whatsmeow_pre_keys", "whatsmeow_device", []string{"jid"}, []string{"jid"}},
	{"fk_session_device", "whatsmeow_sessions", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_senderkey_device", "whatsmeow_sender_keys", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_sync_key_device", "whatsmeow_app_state_sync_keys", "whatsmeow_device", []string{"jid"}, []string{"jid"}},
	{"fk_state_version_device", "whatsmeow_app_state_version", "whatsmeow_device", []string{"jid"}, []string{"jid"}},
	{"fk_mutation_version", "whatsmeow_app_state_mutation_macs", "whatsmeow_app_state_version", []string{"jid", "name"}, []string{"jid", "name"}},
	{"fk_contact_device", "whatsmeow_contacts", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_settings_device", "whatsmeow_chat_settings", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_secret_device", "whatsmeow_message_secrets", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_privacy_device", "whatsmeow_privacy_tokens", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_salt_device", "whatsmeow_nct_salt", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_event_device", "whatsmeow_event_buffer", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
	{"fk_retry_device", "whatsmeow_retry_buffer", "whatsmeow_device", []string{"our_jid"}, []string{"jid"}},
}

var expectedIndexes = []struct {
	table, name string
	unique      bool
	columns     []string
}{
	{"mdbstore_schema_version", "PRIMARY", true, []string{"version"}},
	{"whatsmeow_device", "PRIMARY", true, []string{"jid"}},
	{"whatsmeow_identity_keys", "PRIMARY", true, []string{"our_jid", "their_id"}},
	{"whatsmeow_pre_keys", "PRIMARY", true, []string{"jid", "key_id"}},
	{"whatsmeow_sessions", "PRIMARY", true, []string{"our_jid", "their_id"}},
	{"whatsmeow_sender_keys", "PRIMARY", true, []string{"our_jid", "chat_id", "sender_id"}},
	{"whatsmeow_sender_keys", "idx_sender_keys_sender", false, []string{"our_jid", "sender_id"}},
	{"whatsmeow_app_state_sync_keys", "PRIMARY", true, []string{"jid", "key_id"}},
	{"whatsmeow_app_state_version", "PRIMARY", true, []string{"jid", "name"}},
	{"whatsmeow_app_state_mutation_macs", "PRIMARY", true, []string{"jid", "name", "version", "index_mac"}},
	{"whatsmeow_contacts", "PRIMARY", true, []string{"our_jid", "their_jid"}},
	{"whatsmeow_chat_settings", "PRIMARY", true, []string{"our_jid", "chat_jid"}},
	{"whatsmeow_message_secrets", "PRIMARY", true, []string{"our_jid", "chat_jid", "sender_jid", "message_id"}},
	{"whatsmeow_privacy_tokens", "PRIMARY", true, []string{"our_jid", "their_jid"}},
	{"whatsmeow_privacy_tokens", "idx_privacy_timestamp", false, []string{"our_jid", "timestamp"}},
	{"whatsmeow_nct_salt", "PRIMARY", true, []string{"our_jid"}},
	{"whatsmeow_lid_map", "PRIMARY", true, []string{"lid"}},
	{"whatsmeow_lid_map", "uq_lid_map_pn", true, []string{"pn"}},
	{"whatsmeow_event_buffer", "PRIMARY", true, []string{"our_jid", "ciphertext_hash"}},
	{"whatsmeow_retry_buffer", "PRIMARY", true, []string{"our_jid", "chat_jid", "message_id"}},
	{"whatsmeow_retry_buffer", "idx_retry_timestamp", false, []string{"our_jid", "timestamp"}},
}

var expectedBinaryColumns = []struct {
	table, column string
	length        int64
}{
	{"whatsmeow_device", "noise_key", 32}, {"whatsmeow_device", "identity_key", 32},
	{"whatsmeow_device", "signed_pre_key", 32}, {"whatsmeow_device", "signed_pre_key_sig", 64},
	{"whatsmeow_device", "adv_account_sig", 64}, {"whatsmeow_device", "adv_account_sig_key", 32},
	{"whatsmeow_device", "adv_device_sig", 64}, {"whatsmeow_identity_keys", "identity", 32},
	{"whatsmeow_pre_keys", "prekey", 32}, {"whatsmeow_app_state_version", "hash", 128},
	{"whatsmeow_app_state_mutation_macs", "index_mac", 32}, {"whatsmeow_app_state_mutation_macs", "value_mac", 32},
	{"whatsmeow_event_buffer", "ciphertext_hash", 32},
}

var expectedASCIIColumns = []struct{ table, column string }{
	{"whatsmeow_device", "jid"}, {"whatsmeow_device", "lid"},
	{"whatsmeow_identity_keys", "our_jid"}, {"whatsmeow_identity_keys", "their_id"},
	{"whatsmeow_pre_keys", "jid"}, {"whatsmeow_sessions", "our_jid"}, {"whatsmeow_sessions", "their_id"},
	{"whatsmeow_sender_keys", "our_jid"}, {"whatsmeow_sender_keys", "chat_id"}, {"whatsmeow_sender_keys", "sender_id"},
	{"whatsmeow_app_state_sync_keys", "jid"}, {"whatsmeow_app_state_version", "jid"}, {"whatsmeow_app_state_version", "name"},
	{"whatsmeow_app_state_mutation_macs", "jid"}, {"whatsmeow_app_state_mutation_macs", "name"},
	{"whatsmeow_contacts", "our_jid"}, {"whatsmeow_contacts", "their_jid"},
	{"whatsmeow_chat_settings", "our_jid"}, {"whatsmeow_chat_settings", "chat_jid"}, {"whatsmeow_chat_settings", "wasa_root_secret_id"},
	{"whatsmeow_message_secrets", "our_jid"}, {"whatsmeow_message_secrets", "chat_jid"}, {"whatsmeow_message_secrets", "sender_jid"}, {"whatsmeow_message_secrets", "message_id"},
	{"whatsmeow_privacy_tokens", "our_jid"}, {"whatsmeow_privacy_tokens", "their_jid"},
	{"whatsmeow_nct_salt", "our_jid"}, {"whatsmeow_lid_map", "lid"}, {"whatsmeow_lid_map", "pn"},
	{"whatsmeow_event_buffer", "our_jid"}, {"whatsmeow_retry_buffer", "our_jid"}, {"whatsmeow_retry_buffer", "chat_jid"}, {"whatsmeow_retry_buffer", "message_id"}, {"whatsmeow_retry_buffer", "format"},
}

var expectedUnsignedColumns = []struct{ table, column, columnType string }{
	{"whatsmeow_device", "registration_id", "bigint(20) unsigned"},
	{"whatsmeow_device", "signed_pre_key_id", "int(10) unsigned"},
	{"whatsmeow_pre_keys", "key_id", "int(10) unsigned"},
	{"whatsmeow_app_state_version", "version", "bigint(20) unsigned"},
	{"whatsmeow_app_state_mutation_macs", "version", "bigint(20) unsigned"},
}

func validateSchema(ctx context.Context, conn *sql.Conn, databaseName string) error {
	for table, wanted := range expectedColumns {
		var engine sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, databaseName, table).Scan(&engine); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("required table %s is missing", table)
			}
			return err
		}
		if !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") {
			return fmt.Errorf("table %s must use InnoDB", table)
		}
		rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, databaseName, table)
		if err != nil {
			return err
		}
		var got []string
		for rows.Next() {
			var column string
			if err = rows.Scan(&column); err != nil {
				_ = rows.Close()
				return err
			}
			got = append(got, column)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if err = rows.Err(); err != nil {
			return err
		}
		if !slices.Equal(got, wanted) {
			return fmt.Errorf("table %s columns are incompatible: got %v, want %v", table, got, wanted)
		}
	}
	for constraint, expectedClause := range expectedChecks {
		var clause string
		if err := conn.QueryRowContext(ctx, `SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND CONSTRAINT_NAME=?`, databaseName, constraint).Scan(&clause); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("required check constraint %s is missing", constraint)
			}
			return err
		}
		if normalizeCheckClause(clause) != normalizeCheckClause(expectedClause) {
			return fmt.Errorf("check constraint %s is incompatible", constraint)
		}
	}
	for _, foreignKey := range expectedForeignKeys {
		var updateRule, deleteRule string
		if err := conn.QueryRowContext(ctx, `SELECT UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND CONSTRAINT_NAME=? AND TABLE_NAME=? AND REFERENCED_TABLE_NAME=?`, databaseName, foreignKey.name, foreignKey.table, foreignKey.referencedTable).Scan(&updateRule, &deleteRule); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("required foreign key %s is missing", foreignKey.name)
			}
			return err
		}
		if updateRule != "CASCADE" || deleteRule != "CASCADE" {
			return fmt.Errorf("foreign key %s must cascade updates and deletes", foreignKey.name)
		}
		rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_SCHEMA=? AND CONSTRAINT_NAME=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, databaseName, foreignKey.name, foreignKey.table)
		if err != nil {
			return err
		}
		var columns, referenced []string
		for rows.Next() {
			var column, target string
			if err = rows.Scan(&column, &target); err != nil {
				_ = rows.Close()
				return err
			}
			columns = append(columns, column)
			referenced = append(referenced, target)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if !slices.Equal(columns, foreignKey.columns) || !slices.Equal(referenced, foreignKey.referencedColumns) {
			return fmt.Errorf("foreign key %s columns are incompatible", foreignKey.name)
		}
	}
	for _, column := range expectedBinaryColumns {
		var dataType, nullable string
		var length sql.NullInt64
		if err := conn.QueryRowContext(ctx, `SELECT DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, databaseName, column.table, column.column).Scan(&dataType, &nullable, &length); err != nil {
			return err
		}
		if dataType != "varbinary" || nullable != "NO" || !length.Valid || length.Int64 != column.length {
			return fmt.Errorf("column %s.%s must be non-null VARBINARY(%d)", column.table, column.column, column.length)
		}
	}
	for _, column := range expectedASCIIColumns {
		var dataType string
		var length sql.NullInt64
		var charset, collation sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, databaseName, column.table, column.column).Scan(&dataType, &length, &charset, &collation); err != nil {
			return err
		}
		if dataType != "varchar" || !length.Valid || length.Int64 != 255 || charset.String != "ascii" || collation.String != "ascii_bin" {
			return fmt.Errorf("column %s.%s must be VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin", column.table, column.column)
		}
	}
	for _, column := range expectedUnsignedColumns {
		var columnType string
		if err := conn.QueryRowContext(ctx, `SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, databaseName, column.table, column.column).Scan(&columnType); err != nil {
			return err
		}
		if columnType != column.columnType {
			return fmt.Errorf("column %s.%s must be %s", column.table, column.column, column.columnType)
		}
	}
	for _, index := range expectedIndexes {
		rows, err := conn.QueryContext(ctx, `SELECT NON_UNIQUE, COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX`, databaseName, index.table, index.name)
		if err != nil {
			return err
		}
		var columns []string
		var nonUnique int
		for rows.Next() {
			var value int
			var column string
			if err = rows.Scan(&value, &column); err != nil {
				_ = rows.Close()
				return err
			}
			nonUnique = value
			columns = append(columns, column)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if len(columns) == 0 {
			return fmt.Errorf("required index %s.%s is missing", index.table, index.name)
		}
		if (nonUnique == 0) != index.unique || !slices.Equal(columns, index.columns) {
			return fmt.Errorf("index %s.%s is incompatible", index.table, index.name)
		}
	}
	return nil
}

func normalizeCheckClause(clause string) string {
	return strings.NewReplacer(" ", "", "\t", "", "\r", "", "\n", "", "`", "", "(", "", ")", "").Replace(strings.ToLower(clause))
}

// Validate verifies the MariaDB server, installed schema version, and complete
// schema structure without applying DDL. It is suitable for startup checks
// performed with a DML-only runtime identity after a privileged migration step.
func Validate(ctx context.Context, db *sql.DB) (returnErr error) {
	if db == nil {
		return errors.New("mdbstore: nil database")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire validation connection: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, conn.Close()) }()
	var serverVersion, databaseName string
	if err = conn.QueryRowContext(ctx, "SELECT VERSION(), DATABASE()").Scan(&serverVersion, &databaseName); err != nil {
		return fmt.Errorf("read database identity: %w", err)
	}
	if !strings.Contains(strings.ToLower(serverVersion), "mariadb") {
		return fmt.Errorf("mdbstore requires MariaDB, server reported %q", serverVersion)
	}
	if databaseName == "" {
		return errors.New("mdbstore requires a database name in the DSN")
	}
	var version sql.NullInt64
	if err = conn.QueryRowContext(ctx, "SELECT MAX(version) FROM mdbstore_schema_version").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if !version.Valid || version.Int64 != LatestVersion {
		return fmt.Errorf("database schema version %d, adapter requires %d", version.Int64, LatestVersion)
	}
	if err = validateSchema(ctx, conn, databaseName); err != nil {
		return fmt.Errorf("validate schema: %w", err)
	}
	return nil
}

func splitSQLStatements(source string) []string {
	var statements []string
	var current strings.Builder
	var quote byte
	lineComment, blockComment := false, false
	for i := 0; i < len(source); i++ {
		ch := source[i]
		if lineComment {
			if ch == '\n' {
				lineComment = false
				current.WriteByte(ch)
			}
			continue
		}
		if blockComment {
			if ch == '*' && i+1 < len(source) && source[i+1] == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if quote != 0 {
			current.WriteByte(ch)
			if ch == '\\' && i+1 < len(source) {
				i++
				current.WriteByte(source[i])
				continue
			}
			if ch == quote {
				if i+1 < len(source) && source[i+1] == quote {
					i++
					current.WriteByte(source[i])
					continue
				}
				quote = 0
			}
			continue
		}
		if ch == '-' && i+1 < len(source) && source[i+1] == '-' {
			lineComment = true
			i++
			continue
		}
		if ch == '#' {
			lineComment = true
			continue
		}
		if ch == '/' && i+1 < len(source) && source[i+1] == '*' {
			blockComment = true
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			current.WriteByte(ch)
			continue
		}
		if ch == ';' {
			if statement := strings.TrimSpace(current.String()); statement != "" {
				statements = append(statements, statement)
			}
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	if quote != 0 || blockComment {
		return nil
	}
	if statement := strings.TrimSpace(current.String()); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

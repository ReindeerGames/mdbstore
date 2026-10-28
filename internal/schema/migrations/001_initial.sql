-- MariaDB-native Hypermeow storage schema, matching Hypermeow main schema v19.
CREATE TABLE IF NOT EXISTS whatsmeow_device (
  jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  lid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
  facebook_uuid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
  registration_id BIGINT UNSIGNED NOT NULL,
  noise_key BINARY(32) NOT NULL,
  identity_key BINARY(32) NOT NULL,
  signed_pre_key BINARY(32) NOT NULL,
  signed_pre_key_id INT UNSIGNED NOT NULL,
  signed_pre_key_sig BINARY(64) NOT NULL,
  adv_key LONGBLOB NOT NULL,
  adv_details LONGBLOB NOT NULL,
  adv_account_sig BINARY(64) NOT NULL,
  adv_account_sig_key BINARY(32) NOT NULL,
  adv_device_sig BINARY(64) NOT NULL,
  platform VARCHAR(255) NOT NULL DEFAULT '',
  business_name VARCHAR(1024) NOT NULL DEFAULT '',
  push_name VARCHAR(255) NOT NULL DEFAULT '',
  lid_migration_ts BIGINT NOT NULL DEFAULT 0,
  companion_meta_nonce VARCHAR(255) NOT NULL DEFAULT '',
  PRIMARY KEY (jid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_identity_keys (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  their_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  identity BINARY(32) NOT NULL,
  PRIMARY KEY (our_jid, their_id),
  CONSTRAINT fk_identity_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_pre_keys (
  jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  key_id INT UNSIGNED NOT NULL,
  prekey BINARY(32) NOT NULL,
  uploaded BOOLEAN NOT NULL,
  PRIMARY KEY (jid, key_id),
  CONSTRAINT fk_prekey_device FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_sessions (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  their_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  session LONGBLOB NULL,
  PRIMARY KEY (our_jid, their_id),
  CONSTRAINT fk_session_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_sender_keys (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  chat_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  sender_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  sender_key LONGBLOB NOT NULL,
  PRIMARY KEY (our_jid, chat_id, sender_id),
  KEY idx_sender_keys_sender (our_jid, sender_id),
  CONSTRAINT fk_senderkey_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_app_state_sync_keys (
  jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  key_id VARBINARY(255) NOT NULL,
  key_data LONGBLOB NOT NULL,
  timestamp BIGINT NOT NULL,
  fingerprint LONGBLOB NOT NULL,
  PRIMARY KEY (jid, key_id),
  CONSTRAINT fk_sync_key_device FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_app_state_version (
  jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  version BIGINT UNSIGNED NOT NULL,
  hash BINARY(128) NOT NULL,
  PRIMARY KEY (jid, name),
  CONSTRAINT fk_state_version_device FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_app_state_mutation_macs (
  jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  version BIGINT UNSIGNED NOT NULL,
  index_mac BINARY(32) NOT NULL,
  value_mac BINARY(32) NOT NULL,
  PRIMARY KEY (jid, name, version, index_mac),
  CONSTRAINT fk_mutation_version FOREIGN KEY (jid, name) REFERENCES whatsmeow_app_state_version(jid, name) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_contacts (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  their_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  first_name VARCHAR(1024) NULL,
  full_name VARCHAR(1024) NULL,
  push_name VARCHAR(1024) NULL,
  business_name VARCHAR(1024) NULL,
  redacted_phone VARCHAR(255) NULL,
  username VARCHAR(255) NULL,
  PRIMARY KEY (our_jid, their_jid),
  CONSTRAINT fk_contact_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_chat_settings (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  chat_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  muted_until BIGINT NOT NULL DEFAULT 0,
  pinned BOOLEAN NOT NULL DEFAULT FALSE,
  archived BOOLEAN NOT NULL DEFAULT FALSE,
  wasa_root_secret_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  PRIMARY KEY (our_jid, chat_jid),
  CONSTRAINT fk_settings_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_message_secrets (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  chat_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  sender_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  message_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  secret LONGBLOB NOT NULL,
  PRIMARY KEY (our_jid, chat_jid, sender_jid, message_id),
  CONSTRAINT fk_secret_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_privacy_tokens (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  their_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  token LONGBLOB NOT NULL,
  timestamp BIGINT NOT NULL,
  sender_timestamp BIGINT NULL,
  PRIMARY KEY (our_jid, their_jid),
  KEY idx_privacy_timestamp (our_jid, timestamp),
  CONSTRAINT fk_privacy_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_nct_salt (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  salt LONGBLOB NOT NULL,
  PRIMARY KEY (our_jid),
  CONSTRAINT fk_salt_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_lid_map (
  lid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  pn VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (lid),
  UNIQUE KEY uq_lid_map_pn (pn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_event_buffer (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  ciphertext_hash BINARY(32) NOT NULL,
  plaintext LONGBLOB NULL,
  server_timestamp BIGINT NOT NULL,
  insert_timestamp BIGINT NOT NULL,
  PRIMARY KEY (our_jid, ciphertext_hash),
  CONSTRAINT fk_event_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS whatsmeow_retry_buffer (
  our_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  chat_jid VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  message_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  format VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  plaintext LONGBLOB NOT NULL,
  timestamp BIGINT NOT NULL,
  PRIMARY KEY (our_jid, chat_jid, message_id),
  KEY idx_retry_timestamp (our_jid, timestamp),
  CONSTRAINT fk_retry_device FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


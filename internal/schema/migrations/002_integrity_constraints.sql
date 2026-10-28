ALTER TABLE whatsmeow_device
  MODIFY noise_key VARBINARY(32) NOT NULL,
  MODIFY identity_key VARBINARY(32) NOT NULL,
  MODIFY signed_pre_key VARBINARY(32) NOT NULL,
  MODIFY signed_pre_key_sig VARBINARY(64) NOT NULL,
  MODIFY adv_account_sig VARBINARY(64) NOT NULL,
  MODIFY adv_account_sig_key VARBINARY(32) NOT NULL,
  MODIFY adv_device_sig VARBINARY(64) NOT NULL,
  ADD CONSTRAINT chk_device_registration_id CHECK (registration_id < 4294967296),
  ADD CONSTRAINT chk_device_signed_pre_key_id CHECK (signed_pre_key_id < 16777216),
  ADD CONSTRAINT chk_device_noise_key CHECK (OCTET_LENGTH(noise_key) = 32),
  ADD CONSTRAINT chk_device_identity_key CHECK (OCTET_LENGTH(identity_key) = 32),
  ADD CONSTRAINT chk_device_signed_pre_key CHECK (OCTET_LENGTH(signed_pre_key) = 32),
  ADD CONSTRAINT chk_device_signed_pre_key_sig CHECK (OCTET_LENGTH(signed_pre_key_sig) = 64),
  ADD CONSTRAINT chk_device_adv_account_sig CHECK (OCTET_LENGTH(adv_account_sig) = 64),
  ADD CONSTRAINT chk_device_adv_account_sig_key CHECK (OCTET_LENGTH(adv_account_sig_key) = 32),
  ADD CONSTRAINT chk_device_adv_device_sig CHECK (OCTET_LENGTH(adv_device_sig) = 64);

ALTER TABLE whatsmeow_identity_keys
  MODIFY identity VARBINARY(32) NOT NULL,
  ADD CONSTRAINT chk_identity_key CHECK (OCTET_LENGTH(identity) = 32);

ALTER TABLE whatsmeow_pre_keys
  MODIFY prekey VARBINARY(32) NOT NULL,
  ADD CONSTRAINT chk_prekey_id CHECK (key_id < 16777216),
  ADD CONSTRAINT chk_prekey_data CHECK (OCTET_LENGTH(prekey) = 32);

ALTER TABLE whatsmeow_app_state_version
  MODIFY hash VARBINARY(128) NOT NULL,
  ADD CONSTRAINT chk_app_state_hash CHECK (OCTET_LENGTH(hash) = 128);

ALTER TABLE whatsmeow_app_state_mutation_macs
  MODIFY index_mac VARBINARY(32) NOT NULL,
  MODIFY value_mac VARBINARY(32) NOT NULL,
  ADD CONSTRAINT chk_mutation_index_mac CHECK (OCTET_LENGTH(index_mac) = 32),
  ADD CONSTRAINT chk_mutation_value_mac CHECK (OCTET_LENGTH(value_mac) = 32);

ALTER TABLE whatsmeow_event_buffer
  MODIFY ciphertext_hash VARBINARY(32) NOT NULL,
  ADD CONSTRAINT chk_event_ciphertext_hash CHECK (OCTET_LENGTH(ciphertext_hash) = 32);

package mdbstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/polymorfa/hypermeow/proto/waAdv"
	"github.com/polymorfa/hypermeow/store"
	"github.com/polymorfa/hypermeow/types"
)

func TestMariaDBStoreIntegration(t *testing.T) {
	dsn := os.Getenv("MDBSTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("MDBSTORE_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := New(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer container.Close()
	if version, err := container.SchemaVersion(ctx); err != nil || version != LatestSchemaVersion {
		t.Fatalf("schema version = %d, %v", version, err)
	}
	if err = container.Upgrade(ctx); err != nil {
		t.Fatalf("idempotent upgrade: %v", err)
	}
	if err = container.Validate(ctx); err != nil {
		t.Fatalf("validate schema: %v", err)
	}
	upgradeErrors := make(chan error, 2)
	for range 2 {
		go func() { upgradeErrors <- container.Upgrade(ctx) }()
	}
	for range 2 {
		if err = <-upgradeErrors; err != nil {
			t.Fatalf("concurrent upgrade: %v", err)
		}
	}

	device := container.NewDevice()
	jid := types.NewADJID("15550001111", types.WhatsAppDomain, 1)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte("integration"), AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err = device.Save(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = device.Delete(cleanupCtx)
	})
	if _, err = container.db.RawDB.ExecContext(ctx, `UPDATE whatsmeow_device SET noise_key=? WHERE jid=?`, []byte{1}, jid.String()); err == nil {
		t.Fatal("exact-length constraint accepted a short noise key")
	}
	s := device.Sessions.(*SQLStore)

	identity := [32]byte{1, 2, 3}
	if err = s.PutIdentity(ctx, "15550002222:1", identity); err != nil {
		t.Fatal(err)
	}
	if trusted, err := s.IsTrustedIdentity(ctx, "15550002222:1", identity); err != nil || !trusted {
		t.Fatalf("identity trusted=%v err=%v", trusted, err)
	}
	if identities, _, err := s.GetManyIdentities(ctx, []string{"15550002222:1", "missing:1"}); err != nil || identities["15550002222:1"] != identity {
		t.Fatalf("identities=%v err=%v", identities, err)
	}
	bulkAddresses := make([]string, readBatchSize*2+1)
	for i := range bulkAddresses {
		bulkAddresses[i] = fmt.Sprintf("1555999%06d:1", i)
		key := [32]byte{byte(i), byte(i >> 8)}
		if err = s.PutIdentity(ctx, bulkAddresses[i], key); err != nil {
			t.Fatalf("put bulk identity %d: %v", i, err)
		}
	}
	identityReader := NewSQLStore(container, jid)
	if identities, _, err := identityReader.GetManyIdentities(ctx, bulkAddresses); err != nil || len(identities) != len(bulkAddresses) {
		t.Fatalf("bulk identities=%d err=%v", len(identities), err)
	}
	sessions := map[string][]byte{"15550002222:1": {1, 2}, "15550002222:2": {3, 4}}
	if err = s.PutManySessions(ctx, sessions); err != nil {
		t.Fatal(err)
	}
	gotSessions, err := s.GetManySessions(ctx, []string{"15550002222:1", "15550002222:2"})
	if err != nil || !bytes.Equal(gotSessions["15550002222:2"], []byte{3, 4}) {
		t.Fatalf("sessions=%v err=%v", gotSessions, err)
	}
	bulkSessions := make(map[string][]byte, readBatchSize*2+1)
	bulkSessionAddresses := make([]string, 0, len(bulkSessions))
	for i := 0; i < readBatchSize*2+1; i++ {
		address := fmt.Sprintf("1555888%06d:1", i)
		bulkSessionAddresses = append(bulkSessionAddresses, address)
		bulkSessions[address] = []byte{byte(i), byte(i >> 8)}
	}
	if err = s.PutManySessions(ctx, bulkSessions); err != nil {
		t.Fatalf("put bulk sessions: %v", err)
	}
	if got, getErr := s.GetManySessions(ctx, bulkSessionAddresses); getErr != nil || len(got) != len(bulkSessions) {
		t.Fatalf("bulk sessions=%d err=%v", len(got), getErr)
	}
	iterated := 0
	if err = s.IterateSessions(ctx, bulkSessionAddresses, func(string, []byte) error { iterated++; return nil }); err != nil || iterated != len(bulkSessions) {
		t.Fatalf("iterated sessions=%d err=%v", iterated, err)
	}

	if err = s.PutSenderKey(ctx, "group", "sender", []byte{5, 6}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetSenderKey(ctx, "group", "sender"); err != nil || !bytes.Equal(got, []byte{5, 6}) {
		t.Fatalf("sender key=%x err=%v", got, err)
	}
	if keys, err := s.GetOrGenPreKeys(ctx, 3); err != nil || len(keys) != 3 {
		t.Fatalf("prekeys=%d err=%v", len(keys), err)
	}
	if err = s.MarkPreKeysAsUploaded(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if count, err := s.UploadedPreKeyCount(ctx); err != nil || count != 3 {
		t.Fatalf("uploaded prekeys=%d err=%v", count, err)
	}

	syncKey := store.AppStateSyncKey{Data: []byte{1}, Fingerprint: []byte{2}, Timestamp: 10}
	if err = s.PutAppStateSyncKey(ctx, []byte("sync"), syncKey); err != nil {
		t.Fatal(err)
	}
	if err = s.PutAppStateSyncKey(ctx, []byte("sync"), store.AppStateSyncKey{Data: []byte{9}, Fingerprint: []byte{9}, Timestamp: 9}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetAppStateSyncKey(ctx, []byte("sync")); err != nil || got.Timestamp != 10 || got.Data[0] != 1 {
		t.Fatalf("sync key=%+v err=%v", got, err)
	}
	hash := [128]byte{7}
	if err = s.PutAppStateVersion(ctx, "regular", 1, hash); err != nil {
		t.Fatal(err)
	}
	mutation := store.AppStateMutationMAC{IndexMAC: bytes.Repeat([]byte{8}, 32), ValueMAC: bytes.Repeat([]byte{9}, 32)}
	if err = s.PutAppStateMutationMACs(ctx, "regular", 1, []store.AppStateMutationMAC{mutation}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetAppStateMutationMAC(ctx, "regular", mutation.IndexMAC); err != nil || !bytes.Equal(got, mutation.ValueMAC) {
		t.Fatalf("mutation=%x err=%v", got, err)
	}
	if err = s.DeleteAppStateMutationMACs(ctx, "regular", [][]byte{mutation.IndexMAC}); err != nil {
		t.Fatal(err)
	}
	bulkMutations := make([]store.AppStateMutationMAC, mutationBatchSize*2+1)
	bulkIndexes := make([][]byte, len(bulkMutations))
	for i := range bulkMutations {
		index := make([]byte, 32)
		index[0], index[1] = byte(i), byte(i>>8)
		value := make([]byte, 32)
		value[0] = 1
		bulkMutations[i] = store.AppStateMutationMAC{IndexMAC: index, ValueMAC: value}
		bulkIndexes[i] = index
	}
	if err = s.PutAppStateMutationMACs(ctx, "regular", 1, bulkMutations); err != nil {
		t.Fatalf("put bulk mutations: %v", err)
	}
	if err = s.DeleteAppStateMutationMACs(ctx, "regular", bulkIndexes); err != nil {
		t.Fatalf("delete bulk mutations: %v", err)
	}

	contact := types.NewJID("15550002222", types.DefaultUserServer)
	if err = s.PutContactName(ctx, contact, "Ada", "Ada Lovelace"); err != nil {
		t.Fatal(err)
	}
	if err = s.PutContactUsername(ctx, contact, "ada"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetContact(ctx, contact); err != nil || got.Username != "ada" {
		t.Fatalf("contact=%+v err=%v", got, err)
	}
	bulkContact := types.NewJID("15550003333", types.DefaultUserServer)
	if err = s.PutAllContactNames(ctx, []store.ContactEntry{{JID: bulkContact, FirstName: "Grace", FullName: "Grace Hopper", Username: "grace", UsernameSet: true}}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutManyRedactedPhones(ctx, []store.RedactedPhoneEntry{{JID: bulkContact, RedactedPhone: "+1…3333"}}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutPinned(ctx, contact, true); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetChatSettings(ctx, contact); err != nil || !got.Pinned {
		t.Fatalf("settings=%+v err=%v", got, err)
	}

	if err = s.PutMessageSecret(ctx, contact, contact, "message-1", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if got, _, err := s.GetMessageSecret(ctx, contact, contact, "message-1"); err != nil || !bytes.Equal(got, []byte("secret")) {
		t.Fatalf("message secret=%q err=%v", got, err)
	}
	if err = s.PutMessageSecrets(ctx, []store.MessageSecretInsert{{Chat: contact, Sender: contact, ID: "message-2", Secret: []byte("batch")}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	if err = s.PutPrivacyTokens(ctx, store.PrivacyToken{User: contact, Token: []byte("token"), Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	if err = s.PutPrivacyTokens(ctx); err != nil {
		t.Fatal(err)
	}
	bulkTokens := make([]store.PrivacyToken, mutationBatchSize*2+1)
	for i := range bulkTokens {
		bulkTokens[i] = store.PrivacyToken{User: types.NewJID(fmt.Sprintf("1555777%06d", i), types.DefaultUserServer), Token: []byte{byte(i)}, Timestamp: now}
	}
	if err = s.PutPrivacyTokens(ctx, bulkTokens...); err != nil {
		t.Fatalf("put bulk privacy tokens: %v", err)
	}
	if err = s.PutPrivacyTokens(ctx, store.PrivacyToken{User: contact, Token: []byte("stale"), Timestamp: now.Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetPrivacyToken(ctx, contact); err != nil || !bytes.Equal(got.Token, []byte("token")) {
		t.Fatalf("privacy token=%+v err=%v", got, err)
	}
	if err = s.PutNCTSalt(ctx, []byte("salt")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetNCTSalt(ctx); err != nil || !bytes.Equal(got, []byte("salt")) {
		t.Fatalf("salt=%q err=%v", got, err)
	}

	lid := types.NewJID("100000000000001", types.HiddenUserServer)
	if err = container.LIDMap.PutLIDMapping(ctx, lid, contact); err != nil {
		t.Fatal(err)
	}
	if got, err := container.LIDMap.GetPNForLID(ctx, lid); err != nil || got.User != contact.User {
		t.Fatalf("mapped PN=%s err=%v", got, err)
	}
	lid2 := types.NewJID("100000000000002", types.HiddenUserServer)
	if err = container.LIDMap.PutManyLIDMappings(ctx, []store.LIDMapping{{LID: lid2, PN: bulkContact}}); err != nil {
		t.Fatal(err)
	}
	if got, err := container.LIDMap.GetManyPNsForLIDs(ctx, []types.JID{lid, lid2}); err != nil || len(got) != 2 {
		t.Fatalf("PN mappings=%v err=%v", got, err)
	}
	secondContainer, err := New(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer secondContainer.Close()
	lid3 := types.NewJID("100000000000003", types.HiddenUserServer)
	if err = container.LIDMap.PutLIDMapping(ctx, lid3, bulkContact); err != nil {
		t.Fatal(err)
	}
	if got, getErr := secondContainer.LIDMap.GetLIDForPN(ctx, bulkContact); getErr != nil || got.User != lid3.User {
		t.Fatalf("cross-process LID=%s err=%v", got, getErr)
	}

	pnSignal := contact.SignalAddressUser() + ":7"
	lidSignal := lid.SignalAddressUser() + ":7"
	if err = s.PutSession(ctx, pnSignal, []byte("migrate")); err != nil {
		t.Fatal(err)
	}
	if err = s.MigratePNToLID(ctx, contact, lid); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetSession(ctx, lidSignal); err != nil || !bytes.Equal(got, []byte("migrate")) {
		t.Fatalf("migrated session=%q err=%v", got, err)
	}

	eventHash := [32]byte{4}
	if err = s.PutBufferedEvent(ctx, eventHash, []byte("event"), now); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetBufferedEvent(ctx, eventHash); err != nil || !bytes.Equal(got.Plaintext, []byte("event")) {
		t.Fatalf("event=%+v err=%v", got, err)
	}
	if err = s.ClearBufferedEventPlaintext(ctx, eventHash); err != nil {
		t.Fatal(err)
	}
	if err = s.AddOutgoingEvent(ctx, contact, "out-1", "proto", []byte("out")); err != nil {
		t.Fatal(err)
	}
	if _, got, err := s.GetOutgoingEvent(ctx, contact, types.EmptyJID, "out-1"); err != nil || !bytes.Equal(got, []byte("out")) {
		t.Fatalf("outgoing=%q err=%v", got, err)
	}

	devices, err := container.GetAllDevices(ctx)
	if err != nil || len(devices) != 1 || devices[0].ID.String() != jid.String() {
		t.Fatalf("devices=%d err=%v", len(devices), err)
	}
	if err = device.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = container.db.RawDB.ExecContext(ctx, `ALTER TABLE whatsmeow_retry_buffer DROP INDEX idx_retry_timestamp`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = container.db.RawDB.ExecContext(context.Background(), `ALTER TABLE whatsmeow_retry_buffer ADD INDEX idx_retry_timestamp (our_jid, timestamp)`)
	})
	if err = container.Upgrade(ctx); err == nil {
		t.Fatal("schema validation accepted a missing required index")
	}
	if _, err = container.db.RawDB.ExecContext(ctx, `ALTER TABLE whatsmeow_retry_buffer ADD INDEX idx_retry_timestamp (our_jid, timestamp)`); err != nil {
		t.Fatal(err)
	}
	if err = container.Upgrade(ctx); err != nil {
		t.Fatalf("schema validation after index repair: %v", err)
	}
	if _, err = container.db.RawDB.ExecContext(ctx, `DELETE FROM mdbstore_schema_version WHERE version=?`, LatestSchemaVersion); err != nil {
		t.Fatal(err)
	}
	if err = container.Upgrade(ctx); err != nil {
		t.Fatalf("resume migration after lost version marker: %v", err)
	}
	if version, versionErr := container.SchemaVersion(ctx); versionErr != nil || version != LatestSchemaVersion {
		t.Fatalf("resumed schema version=%d err=%v", version, versionErr)
	}
}

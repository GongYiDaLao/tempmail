package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func openIntegrationStore(tb testing.TB) *Store {
	tb.Helper()
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		tb.Skip("TEST_DATABASE_DSN is not set")
	}
	s, err := NewWithOptions(context.Background(), dsn, PoolOptions{MaxConns: 128, MinConns: 8})
	if err != nil {
		tb.Fatalf("open integration store: %v", err)
	}
	tb.Cleanup(s.Close)
	return s
}

func createDeliveryFixture(tb testing.TB, s *Store) (string, uuid.UUID) {
	tb.Helper()
	ctx := context.Background()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	username := "delivery_" + suffix
	apiKey := "tm_test_" + suffix
	domain := "delivery-" + suffix + ".example.test"
	address := "mailbox@" + domain

	var accountID uuid.UUID
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO accounts (username, api_key) VALUES ($1, $2) RETURNING id`,
		username, apiKey,
	).Scan(&accountID); err != nil {
		tb.Fatalf("insert account: %v", err)
	}

	var domainID int
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO domains (domain, domain_type, base_domain, supports_single, is_active, status)
		 VALUES ($1, 'exact', $1, TRUE, TRUE, 'active') RETURNING id`,
		domain,
	).Scan(&domainID); err != nil {
		tb.Fatalf("insert domain: %v", err)
	}

	var mailboxID uuid.UUID
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO mailboxes (account_id, address, domain_id, full_address, expires_at)
		 VALUES ($1, 'mailbox', $2, $3, NOW() + INTERVAL '1 hour') RETURNING id`,
		accountID, domainID, address,
	).Scan(&mailboxID); err != nil {
		tb.Fatalf("insert mailbox: %v", err)
	}

	tb.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM domains WHERE id = $1`, domainID)
	})
	return address, mailboxID
}

func TestDeliverEmailsAtomicOrderAndCounters(t *testing.T) {
	s := openIntegrationStore(t)
	address, mailboxID := createDeliveryFixture(t, s)
	ctx := context.Background()

	var totalBefore int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(NULLIF(value, ''), '0')::BIGINT FROM app_settings WHERE key = 'total_emails_received'`,
	).Scan(&totalBefore); err != nil {
		t.Fatalf("read initial total: %v", err)
	}

	recipients := []string{strings.ToUpper(address), "missing@example.test", address, address}
	results, err := s.DeliverEmails(ctx, recipients, "sender@example.test", "subject", "text", "<p>html</p>", "raw-message")
	if err != nil {
		t.Fatalf("DeliverEmails: %v", err)
	}
	if len(results) != len(recipients) {
		t.Fatalf("got %d results, want %d", len(results), len(recipients))
	}
	for i, result := range results {
		if result.Ordinal != int64(i+1) || result.Recipient != recipients[i] {
			t.Fatalf("result %d lost input order: %+v", i, result)
		}
		wantDelivered := i != 1
		if result.Delivered != wantDelivered {
			t.Fatalf("result %d delivered=%v, want %v", i, result.Delivered, wantDelivered)
		}
		if result.Delivered && (result.EmailID == uuid.Nil || result.MailboxID != mailboxID) {
			t.Fatalf("result %d has invalid IDs: %+v", i, result)
		}
	}

	var inserted int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emails WHERE mailbox_id = $1`, mailboxID).Scan(&inserted); err != nil {
		t.Fatalf("count inserted emails: %v", err)
	}
	if inserted != 3 {
		t.Fatalf("inserted %d emails, want 3", inserted)
	}

	// An oversized subject makes the entire set-based statement fail. No row
	// from the attempted batch may remain committed.
	if _, err := s.DeliverEmails(ctx, []string{address, address}, "sender@example.test", strings.Repeat("x", 999), "", "", "raw"); err == nil {
		t.Fatal("expected oversized subject to fail")
	}
	var afterFailure int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emails WHERE mailbox_id = $1`, mailboxID).Scan(&afterFailure); err != nil {
		t.Fatalf("count after failed batch: %v", err)
	}
	if afterFailure != inserted {
		t.Fatalf("failed batch committed rows: before=%d after=%d", inserted, afterFailure)
	}

	s.flushCounters(ctx)
	var mailboxCount, totalAfter int64
	if err := s.pool.QueryRow(ctx, `SELECT received_email_count FROM mailboxes WHERE id = $1`, mailboxID).Scan(&mailboxCount); err != nil {
		t.Fatalf("read mailbox counter: %v", err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(NULLIF(value, ''), '0')::BIGINT FROM app_settings WHERE key = 'total_emails_received'`,
	).Scan(&totalAfter); err != nil {
		t.Fatalf("read final total: %v", err)
	}
	if mailboxCount != 3 || totalAfter-totalBefore != 3 {
		t.Fatalf("counter mismatch: mailbox=%d total_delta=%d", mailboxCount, totalAfter-totalBefore)
	}

	var accountID uuid.UUID
	if err := s.pool.QueryRow(ctx, `SELECT account_id FROM mailboxes WHERE id = $1`, mailboxID).Scan(&accountID); err != nil {
		t.Fatalf("read fixture account: %v", err)
	}
	accounts, _, err := s.ListAccounts(ctx, 1, 10000)
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}
	found := false
	for _, account := range accounts {
		if account.ID != accountID {
			continue
		}
		found = true
		if account.MailboxCount != 1 || account.ActiveMailboxCount != 1 || account.CurrentEmailCount != 3 || account.ReceivedEmailCount != 3 {
			t.Fatalf("incorrect account stats: %+v", account)
		}
	}
	if !found {
		t.Fatal("fixture account missing from ListAccounts")
	}
}

func TestRunStatsFlusherFlushesOnCancelAndSupportsBigInt(t *testing.T) {
	s := openIntegrationStore(t)
	address, mailboxID := createDeliveryFixture(t, s)
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx, `UPDATE mailboxes SET received_email_count = 2147483647 WHERE id = $1`, mailboxID); err != nil {
		t.Fatalf("seed BIGINT counter: %v", err)
	}
	results, err := s.DeliverEmails(ctx, []string{address}, "sender", "subject", "text", "", "raw")
	if err != nil || len(results) != 1 || !results[0].Delivered {
		t.Fatalf("DeliverEmails: results=%v err=%v", results, err)
	}

	flusherCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunStatsFlusher(flusherCtx, time.Hour)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("stats flusher did not stop")
	}

	var count int64
	if err := s.pool.QueryRow(ctx, `SELECT received_email_count FROM mailboxes WHERE id = $1`, mailboxID).Scan(&count); err != nil {
		t.Fatalf("read BIGINT counter: %v", err)
	}
	if count != 2147483648 {
		t.Fatalf("received_email_count=%d, want 2147483648", count)
	}
}

func TestDeliverMessageBatchDistinctPayloadsAndAtomicFailure(t *testing.T) {
	s := openIntegrationStore(t)
	address, mailboxID := createDeliveryFixture(t, s)
	ctx := context.Background()
	firstReceivedAt := time.Now().UTC().Truncate(time.Microsecond)
	secondReceivedAt := firstReceivedAt.Add(time.Microsecond)

	deliveries := []MessageDelivery{
		{Recipient: strings.ToUpper(address), Sender: "sender-1", Subject: "subject-1", BodyText: "text-1", Raw: "raw-1", ReceivedAt: firstReceivedAt},
		{Recipient: "missing@example.test", Sender: "missing", Subject: "missing", Raw: "missing"},
		{Recipient: address, Sender: "sender-2", Subject: "subject-2", BodyHTML: "<p>two</p>", Raw: "raw-2", ReceivedAt: secondReceivedAt},
	}
	results, err := s.DeliverMessageBatch(ctx, deliveries)
	if err != nil {
		t.Fatalf("DeliverMessageBatch: %v", err)
	}
	if len(results) != len(deliveries) {
		t.Fatalf("got %d results, want %d", len(results), len(deliveries))
	}
	for i, result := range results {
		if result.Ordinal != int64(i+1) || result.Recipient != deliveries[i].Recipient {
			t.Fatalf("result %d lost order: %+v", i, result)
		}
		wantDelivered := i != 1
		if result.Delivered != wantDelivered {
			t.Fatalf("result %d delivered=%v, want %v", i, result.Delivered, wantDelivered)
		}
		if !result.Delivered {
			continue
		}
		var sender, subject, bodyText, bodyHTML, raw string
		var receivedAt time.Time
		if err := s.pool.QueryRow(ctx,
			`SELECT sender, subject, body_text, body_html, raw_message, received_at FROM emails WHERE id = $1`,
			result.EmailID,
		).Scan(&sender, &subject, &bodyText, &bodyHTML, &raw, &receivedAt); err != nil {
			t.Fatalf("read result %d: %v", i, err)
		}
		got := MessageDelivery{Sender: sender, Subject: subject, BodyText: bodyText, BodyHTML: bodyHTML, Raw: raw}
		want := deliveries[i]
		if got.Sender != want.Sender || got.Subject != want.Subject || got.BodyText != want.BodyText || got.BodyHTML != want.BodyHTML || got.Raw != want.Raw {
			t.Fatalf("result %d payload mismatch: got=%+v want=%+v", i, got, want)
		}
		if !receivedAt.Equal(want.ReceivedAt) {
			t.Fatalf("result %d received_at=%s, want %s", i, receivedAt, want.ReceivedAt)
		}
	}

	var before int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emails WHERE mailbox_id = $1`, mailboxID).Scan(&before); err != nil {
		t.Fatalf("count before atomic failure: %v", err)
	}
	_, err = s.DeliverMessageBatch(ctx, []MessageDelivery{
		{Recipient: address, Subject: "valid", Raw: "valid"},
		{Recipient: address, Subject: strings.Repeat("x", 999), Raw: "invalid"},
	})
	if err == nil {
		t.Fatal("expected oversized subject batch to fail")
	}
	var after int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM emails WHERE mailbox_id = $1`, mailboxID).Scan(&after); err != nil {
		t.Fatalf("count after atomic failure: %v", err)
	}
	if after != before {
		t.Fatalf("failed message batch committed rows: before=%d after=%d", before, after)
	}
}

func TestDeliveryBatcherConcurrentCalls(t *testing.T) {
	s := openIntegrationStore(t)
	address, mailboxID := createDeliveryFixture(t, s)
	batcherCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher := NewDeliveryBatcher(batcherCtx, s, 64, 10*time.Millisecond)
	t.Cleanup(func() { _ = batcher.CloseAndDrain(context.Background()) })

	const count = 64
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := batcher.Deliver(context.Background(), MessageDelivery{
				Recipient: address,
				Sender:    "batcher",
				Subject:   "concurrent",
				Raw:       "raw-" + uuid.NewString(),
			})
			if err != nil {
				errs <- err
				return
			}
			if !result.Delivered || result.EmailID == uuid.Nil || result.MailboxID != mailboxID {
				errs <- fmt.Errorf("invalid result %d: %+v", i, result)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	var inserted int
	if err := s.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM emails WHERE mailbox_id = $1`, mailboxID).Scan(&inserted); err != nil {
		t.Fatalf("count batcher inserts: %v", err)
	}
	if inserted != count {
		t.Fatalf("batcher inserted %d messages, want %d", inserted, count)
	}

	if _, err := batcher.Deliver(context.Background(), MessageDelivery{
		Recipient: address,
		Subject:   strings.Repeat("x", 999),
	}); err == nil {
		t.Fatal("expected schema validation error")
	}
	malformedUnknown := MessageDelivery{
		Recipient: "missing@example.test",
		Subject:   strings.Repeat("x", 999),
	}
	directResult, err := s.DeliverMessage(context.Background(), malformedUnknown)
	if err != nil || directResult.Delivered || directResult.Recipient != malformedUnknown.Recipient {
		t.Fatalf("direct unknown malformed contract: result=%+v err=%v", directResult, err)
	}
	if _, err := s.DeliverMessage(context.Background(), MessageDelivery{
		Recipient: address,
		Subject:   strings.Repeat("x", 999),
	}); err == nil {
		t.Fatal("direct known malformed delivery unexpectedly succeeded")
	}
	unknownResult, err := batcher.Deliver(context.Background(), malformedUnknown)
	if err != nil || unknownResult.Delivered || unknownResult.Recipient != "missing@example.test" {
		t.Fatalf("unknown malformed recipient contract changed: result=%+v err=%v", unknownResult, err)
	}

	cancelledCtx, cancelRequest := context.WithCancel(context.Background())
	cancelledResult := make(chan error, 1)
	go func() {
		_, err := batcher.Deliver(cancelledCtx, MessageDelivery{Recipient: address, Subject: "cancelled", Raw: "cancelled"})
		cancelledResult <- err
	}()
	time.Sleep(time.Millisecond)
	cancelRequest()
	validResult, err := batcher.Deliver(context.Background(), MessageDelivery{Recipient: address, Subject: "after-cancel", Raw: "valid"})
	if err != nil || !validResult.Delivered {
		t.Fatalf("valid request failed after peer cancellation: result=%+v err=%v", validResult, err)
	}
	if err := <-cancelledResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error=%v, want context.Canceled", err)
	}

	largeRaw := strings.Repeat("x", 3<<20)
	bytesFlushBefore := batcher.bytesFlush.Load()
	largeStart := make(chan struct{})
	largeErrs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-largeStart
			_, err := batcher.Deliver(context.Background(), MessageDelivery{Recipient: address, Subject: "large", Raw: largeRaw})
			largeErrs <- err
		}()
	}
	close(largeStart)
	for i := 0; i < 2; i++ {
		if err := <-largeErrs; err != nil {
			t.Fatalf("large delivery %d: %v", i, err)
		}
	}
	if batcher.bytesFlush.Load() <= bytesFlushBefore {
		t.Fatal("large deliveries did not trigger the batch byte limit")
	}

	if err := batcher.CloseAndDrain(context.Background()); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
	if _, err := batcher.Deliver(context.Background(), MessageDelivery{Recipient: address}); !errors.Is(err, ErrDeliveryBatcherClosed) {
		t.Fatalf("Deliver after close error=%v, want ErrDeliveryBatcherClosed", err)
	}
}

func TestDeliveryBatcherConstructorBounds(t *testing.T) {
	batcher := NewDeliveryBatcher(context.Background(), nil, 1<<30, time.Hour)
	if batcher.maxBatch != deliveryBatchDefaultMax || batcher.maxWait != deliveryBatchDefaultWait {
		t.Fatalf("constructor did not clamp unsafe settings: max=%d wait=%s", batcher.maxBatch, batcher.maxWait)
	}
	if err := batcher.CloseAndDrain(context.Background()); err != nil {
		t.Fatalf("CloseAndDrain: %v", err)
	}
}

func TestInsertEmailReturnsContentWithoutDatabaseRoundTrip(t *testing.T) {
	s := openIntegrationStore(t)
	_, mailboxID := createDeliveryFixture(t, s)

	email, err := s.InsertEmail(context.Background(), mailboxID, "sender", "subject", "text", "html", "raw")
	if err != nil {
		t.Fatalf("InsertEmail: %v", err)
	}
	if email.ID == uuid.Nil || email.MailboxID != mailboxID || email.RawMessage != "raw" || email.ReceivedAt.IsZero() {
		t.Fatalf("unexpected returned email: %+v", email)
	}
}

func BenchmarkDeliverEmail(b *testing.B) {
	s := openIntegrationStore(b)
	address, _ := createDeliveryFixture(b, s)
	raw := strings.Repeat("x", 1024)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			results, err := s.DeliverEmails(context.Background(), []string{address}, "sender@example.test", "verification", "code: 123456", "", raw)
			if err != nil || len(results) != 1 || !results[0].Delivered {
				b.Errorf("delivery failed: results=%v err=%v", results, err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deliveries/s")
}

func BenchmarkDeliverBatch10(b *testing.B) {
	s := openIntegrationStore(b)
	address, _ := createDeliveryFixture(b, s)
	recipients := make([]string, 10)
	for i := range recipients {
		recipients[i] = address
	}
	raw := strings.Repeat("x", 1024)
	b.SetBytes(int64(len(raw) * len(recipients)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		results, err := s.DeliverEmails(context.Background(), recipients, "sender@example.test", "verification", "code: 123456", "", raw)
		if err != nil || len(results) != len(recipients) {
			b.Fatalf("batch delivery failed: results=%d err=%v", len(results), err)
		}
	}
	b.ReportMetric(float64(b.N*len(recipients))/b.Elapsed().Seconds(), "deliveries/s")
}

func TestMain(m *testing.M) {
	// Keep the default test process deterministic when integration tests and
	// benchmarks share the same temporary database.
	time.Local = time.UTC
	os.Exit(m.Run())
}

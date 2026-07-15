package store

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// MessageDelivery is one distinct message submitted through the existing
// /internal/deliver path. DeliveryBatcher groups several of these requests
// into one durable PostgreSQL statement without changing the HTTP contract.
type MessageDelivery struct {
	Recipient  string
	Sender     string
	Subject    string
	BodyText   string
	BodyHTML   string
	Raw        string
	ReceivedAt time.Time
}

// DeliverMessage preserves the original single-delivery behavior while using
// one set-based statement for valid input. Invalid content is resolved against
// the recipient first: unknown mailboxes are discarded, while a poison message
// for a real mailbox fails on its own and cannot contaminate a shared batch.
func (s *Store) DeliverMessage(ctx context.Context, delivery MessageDelivery) (DeliveredEmail, error) {
	if validationErr := validateMessageDelivery(delivery); validationErr != nil {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM mailboxes WHERE full_address = $1)`,
			strings.ToLower(delivery.Recipient),
		).Scan(&exists); err != nil {
			return DeliveredEmail{}, err
		}
		if !exists {
			return DeliveredEmail{Ordinal: 1, Recipient: delivery.Recipient}, nil
		}
		return DeliveredEmail{}, validationErr
	}

	results, err := s.DeliverEmails(ctx, []string{delivery.Recipient},
		delivery.Sender, delivery.Subject, delivery.BodyText, delivery.BodyHTML, delivery.Raw)
	if err != nil {
		return DeliveredEmail{}, err
	}
	if len(results) != 1 {
		return DeliveredEmail{}, fmt.Errorf("single delivery result count mismatch: got %d, want 1", len(results))
	}
	return results[0], nil
}

// DeliverMessageBatch atomically resolves and inserts multiple distinct
// messages. Results preserve input order and unknown recipients are returned
// with Delivered=false, matching DeliverEmails semantics.
func (s *Store) DeliverMessageBatch(ctx context.Context, deliveries []MessageDelivery) ([]DeliveredEmail, error) {
	if len(deliveries) == 0 {
		return []DeliveredEmail{}, nil
	}

	recipients := make([]string, len(deliveries))
	senders := make([]string, len(deliveries))
	subjects := make([]string, len(deliveries))
	bodyTexts := make([]string, len(deliveries))
	bodyHTMLs := make([]string, len(deliveries))
	rawMessages := make([]string, len(deliveries))
	sizes := make([]int32, len(deliveries))
	receivedTimes := make([]time.Time, len(deliveries))
	for i, delivery := range deliveries {
		if len(delivery.Raw) > math.MaxInt32 {
			return nil, fmt.Errorf("delivery %d raw message is too large", i)
		}
		recipients[i] = delivery.Recipient
		senders[i] = delivery.Sender
		subjects[i] = delivery.Subject
		bodyTexts[i] = delivery.BodyText
		bodyHTMLs[i] = delivery.BodyHTML
		rawMessages[i] = delivery.Raw
		sizes[i] = int32(len(delivery.Raw))
		if delivery.ReceivedAt.IsZero() {
			delivery.ReceivedAt = time.Now()
		}
		receivedTimes[i] = delivery.ReceivedAt
	}

	rows, err := s.pool.Query(ctx, `
		WITH input AS MATERIALIZED (
			SELECT
				ordinality::BIGINT AS ordinal,
				recipient,
				sender,
				subject,
				body_text,
				body_html,
				raw_message,
				size_bytes,
				received_at
			FROM unnest(
				$1::TEXT[], $2::TEXT[], $3::TEXT[], $4::TEXT[],
				$5::TEXT[], $6::TEXT[], $7::INT[], $8::TIMESTAMPTZ[]
			) WITH ORDINALITY AS d(
				recipient, sender, subject, body_text, body_html,
				raw_message, size_bytes, received_at, ordinality
			)
		),
		matched AS MATERIALIZED (
			SELECT
				i.*,
				m.id AS mailbox_id,
				gen_random_uuid() AS email_id
			FROM input AS i
			JOIN mailboxes AS m ON m.full_address = lower(i.recipient)
		),
		inserted AS (
			INSERT INTO emails (
				id, mailbox_id, sender, subject, body_text,
				body_html, raw_message, size_bytes, received_at
			)
			SELECT
				email_id, mailbox_id, sender, subject, body_text,
				body_html, raw_message, size_bytes, received_at
			FROM matched
			ORDER BY ordinal
			RETURNING id
		)
		SELECT
			i.ordinal,
			i.recipient,
			COALESCE(m.email_id, '00000000-0000-0000-0000-000000000000'::UUID),
			COALESCE(m.mailbox_id, '00000000-0000-0000-0000-000000000000'::UUID),
			(ins.id IS NOT NULL) AS delivered
		FROM input AS i
		LEFT JOIN matched AS m ON m.ordinal = i.ordinal
		LEFT JOIN inserted AS ins ON ins.id = m.email_id
		ORDER BY i.ordinal`,
		recipients, senders, subjects, bodyTexts, bodyHTMLs, rawMessages, sizes, receivedTimes,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DeliveredEmail, 0, len(deliveries))
	deliveredMailboxIDs := make([]uuid.UUID, 0, len(deliveries))
	for rows.Next() {
		var result DeliveredEmail
		if err := rows.Scan(
			&result.Ordinal,
			&result.Recipient,
			&result.EmailID,
			&result.MailboxID,
			&result.Delivered,
		); err != nil {
			return nil, err
		}
		results = append(results, result)
		if result.Delivered {
			deliveredMailboxIDs = append(deliveredMailboxIDs, result.MailboxID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(results) != len(deliveries) {
		return nil, fmt.Errorf("message batch result count mismatch: got %d, want %d", len(results), len(deliveries))
	}

	s.counters.incMany(deliveredMailboxIDs)
	return results, nil
}

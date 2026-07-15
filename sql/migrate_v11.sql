-- ============================================================
-- TempMail v11 migration - widen per-mailbox receive counters
-- ============================================================

ALTER TABLE mailboxes
  ALTER COLUMN received_email_count TYPE BIGINT;

-- ============================================================
-- TempMail v6 迁移 — 账户收件统计
-- ============================================================

ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS received_email_count BIGINT NOT NULL DEFAULT 0;

UPDATE mailboxes m
SET received_email_count = counts.total
FROM (
  SELECT mailbox_id, COUNT(*)::BIGINT AS total
  FROM emails
  GROUP BY mailbox_id
) counts
WHERE counts.mailbox_id = m.id
  AND m.received_email_count = 0;

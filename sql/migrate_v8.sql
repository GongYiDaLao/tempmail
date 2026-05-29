-- ============================================================
-- TempMail v8 迁移 - GitHub OAuth 登录
-- ============================================================

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS github_id VARCHAR(128) UNIQUE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_github_id ON accounts (github_id) WHERE github_id IS NOT NULL;

INSERT INTO app_settings (key, value) VALUES ('github_login_enabled', 'false') ON CONFLICT DO NOTHING;
INSERT INTO app_settings (key, value) VALUES ('github_client_id', '') ON CONFLICT DO NOTHING;
INSERT INTO app_settings (key, value) VALUES ('github_client_secret', '') ON CONFLICT DO NOTHING;
INSERT INTO app_settings (key, value) VALUES ('github_redirect_url', '') ON CONFLICT DO NOTHING;

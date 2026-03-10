-- Lead prioritization + follow-up drafting schema (BusinessMarketing Story 3)
-- Target DB: Project_Phase_3/kb/BusinessMarketing/brand_feedback.db

PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS member_registry (
  member_id TEXT PRIMARY KEY
);

-- Seed MB001..MB050 for optional member linkage validation.
WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 50
)
INSERT OR IGNORE INTO member_registry(member_id)
SELECT 'MB' || printf('%03d', n) FROM seq;

CREATE TABLE IF NOT EXISTS leads (
  lead_id TEXT PRIMARY KEY,
  member_id TEXT,
  first_name TEXT,
  company_name TEXT,
  email TEXT,
  phone TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (member_id) REFERENCES member_registry(member_id)
);

CREATE TABLE IF NOT EXISTS lead_engagement_signals (
  signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
  lead_id TEXT NOT NULL,
  member_id TEXT,
  as_of_date TEXT NOT NULL, -- YYYY-MM-DD
  lookback_days INTEGER NOT NULL CHECK (lookback_days IN (7, 14, 30)),
  pages_viewed INTEGER NOT NULL DEFAULT 0 CHECK (pages_viewed >= 0),
  primary_class_interest TEXT,
  cart_abandonments INTEGER NOT NULL DEFAULT 0 CHECK (cart_abandonments >= 0),
  trial_used INTEGER NOT NULL DEFAULT 0 CHECK (trial_used IN (0, 1)),
  days_since_last_visit INTEGER NOT NULL CHECK (days_since_last_visit >= 0),
  email_opens INTEGER NOT NULL DEFAULT 0 CHECK (email_opens >= 0),
  last_visit_at TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (lead_id, as_of_date, lookback_days),
  FOREIGN KEY (lead_id) REFERENCES leads(lead_id),
  FOREIGN KEY (member_id) REFERENCES member_registry(member_id)
);

CREATE TABLE IF NOT EXISTS suppression_list (
  lead_id TEXT NOT NULL,
  channel TEXT NOT NULL CHECK (channel IN ('email', 'call', 'sms', 'all')),
  is_suppressed INTEGER NOT NULL DEFAULT 1 CHECK (is_suppressed IN (0, 1)),
  reason TEXT NOT NULL CHECK (reason IN ('unsubscribed', 'do_not_contact', 'legal_hold', 'other')),
  suppressed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (lead_id, channel),
  FOREIGN KEY (lead_id) REFERENCES leads(lead_id)
);

CREATE TABLE IF NOT EXISTS message_templates (
  template_id TEXT PRIMARY KEY,
  intent TEXT NOT NULL,
  primary_class_interest TEXT,
  channel TEXT NOT NULL CHECK (channel IN ('email', 'call', 'sms')),
  tone TEXT NOT NULL CHECK (tone IN ('friendly', 'consultative', 'urgent', 'neutral')),
  subject_template TEXT,
  body_template TEXT NOT NULL,
  cta_template TEXT,
  priority INTEGER NOT NULL DEFAULT 100,
  is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lead_scoring_config (
  metric_name TEXT PRIMARY KEY,
  weight REAL NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS intent_priority_config (
  intent TEXT PRIMARY KEY,
  priority_rank INTEGER NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signals_window_interest
  ON lead_engagement_signals (as_of_date, lookback_days, primary_class_interest);

CREATE INDEX IF NOT EXISTS idx_signals_lead
  ON lead_engagement_signals (lead_id);

CREATE INDEX IF NOT EXISTS idx_suppression_lookup
  ON suppression_list (lead_id, channel, is_suppressed);

CREATE INDEX IF NOT EXISTS idx_templates_match
  ON message_templates (intent, primary_class_interest, channel, tone, is_active, priority);

INSERT INTO lead_scoring_config(metric_name, weight, is_active)
VALUES
  ('pages_viewed', 2.0, 1),
  ('cart_abandonments', 5.0, 1),
  ('trial_used', 8.0, 1),
  ('email_opens', 1.0, 1),
  ('recency_bonus_base', 10.0, 1)
ON CONFLICT(metric_name) DO UPDATE SET
  weight = excluded.weight,
  is_active = excluded.is_active,
  updated_at = CURRENT_TIMESTAMP;

INSERT INTO intent_priority_config(intent, priority_rank)
VALUES
  ('purchase_ready', 1),
  ('trial_engaged', 2),
  ('considering', 3),
  ('browsing', 4)
ON CONFLICT(intent) DO UPDATE SET
  priority_rank = excluded.priority_rank,
  updated_at = CURRENT_TIMESTAMP;

COMMIT;

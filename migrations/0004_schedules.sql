CREATE TABLE IF NOT EXISTS schedules (
  id uuid PRIMARY KEY,
  tenant_id text NOT NULL DEFAULT 'default',
  workflow text NOT NULL,
  cron text NOT NULL,
  timezone text NOT NULL DEFAULT 'UTC',
  input jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  catchup text NOT NULL DEFAULT 'latest' CHECK (catchup IN ('none', 'latest')),
  last_fire_time timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS schedules_enabled_idx
  ON schedules (enabled, last_fire_time)
  WHERE enabled;

CREATE INDEX IF NOT EXISTS schedules_tenant_idx
  ON schedules (tenant_id);

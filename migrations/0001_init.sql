CREATE TABLE IF NOT EXISTS runs (
  id uuid PRIMARY KEY,
  tenant_id text NOT NULL DEFAULT 'default',
  workflow text NOT NULL,
  input jsonb NOT NULL,
  output jsonb,
  status text NOT NULL CHECK (status IN ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
  idempotency_key text,
  error jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS runs_tenant_idempotency_key_unique
  ON runs (tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS steps (
  run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  step_key text NOT NULL,
  status text NOT NULL CHECK (status IN ('completed', 'failed')),
  output jsonb,
  attempts integer NOT NULL DEFAULT 0,
  last_error jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_key)
);

CREATE TABLE IF NOT EXISTS tasks (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  tenant_id text NOT NULL DEFAULT 'default',
  run_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL CHECK (status IN ('ready', 'leased', 'done')),
  priority integer NOT NULL DEFAULT 0,
  attempts integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 10,
  locked_by text,
  lease_token uuid,
  lease_expires_at timestamptz,
  last_error jsonb
);

CREATE INDEX IF NOT EXISTS tasks_ready_priority_run_at_idx
  ON tasks (priority DESC, run_at)
  WHERE status = 'ready';

CREATE INDEX IF NOT EXISTS tasks_lease_expires_idx
  ON tasks (lease_expires_at)
  WHERE status = 'leased';

CREATE TABLE IF NOT EXISTS dead_letters (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  task_id uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
  tenant_id text NOT NULL DEFAULT 'default',
  reason text NOT NULL,
  error jsonb,
  payload jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  replayed_at timestamptz
);

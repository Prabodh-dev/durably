CREATE TABLE IF NOT EXISTS tenants (
  id text PRIMARY KEY,
  name text NOT NULL,
  max_concurrent_tasks integer NOT NULL DEFAULT 0 CHECK (max_concurrent_tasks >= 0),
  last_claim_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO tenants (id, name, max_concurrent_tasks)
VALUES ('default', 'Default', 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS api_keys (
  id uuid PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
  name text NOT NULL,
  key_prefix text NOT NULL,
  key_hash text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz
);

CREATE INDEX IF NOT EXISTS api_keys_tenant_idx
  ON api_keys (tenant_id)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS tasks_tenant_running_idx
  ON tasks (tenant_id)
  WHERE status = 'leased';

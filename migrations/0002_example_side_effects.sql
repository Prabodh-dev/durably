CREATE TABLE IF NOT EXISTS example_side_effects (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  step_key text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  note text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS example_side_effects_run_id_idx
  ON example_side_effects (run_id);

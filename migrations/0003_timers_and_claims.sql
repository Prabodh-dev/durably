ALTER TABLE steps ADD COLUMN IF NOT EXISTS wake_at timestamptz;

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claimed_at timestamptz;

CREATE INDEX IF NOT EXISTS steps_wake_at_idx
  ON steps (wake_at)
  WHERE wake_at IS NOT NULL;

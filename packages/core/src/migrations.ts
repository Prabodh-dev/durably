import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import type { Pool, PoolClient } from 'pg';

const MIGRATION_LOCK_KEY = '8291047712';
const MIGRATION_LOCK_DEADLINE_MS = 60000;

async function acquireMigrationLock(client: PoolClient): Promise<void> {
  // A blocking advisory lock would wait forever if the holder were wedged, so
  // the wait is polled and bounded instead.
  const deadline = Date.now() + MIGRATION_LOCK_DEADLINE_MS;
  for (;;) {
    const result = await client.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock($1) AS locked',
      [MIGRATION_LOCK_KEY]
    );
    if (result.rows[0]?.locked === true) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error('timed out waiting for the migration lock');
    }
    await sleep(100);
  }
}

export async function runMigrations(
  pool: Pool,
  migrationsDir: string
): Promise<void> {
  const client = await pool.connect();
  const swallowError = (): void => undefined;
  client.on('error', swallowError);

  try {
    // A server and a worker start at the same time and both run migrations.
    // Without a lock they can execute the same DDL concurrently and collide on
    // a unique index, for example a CREATE TYPE racing itself.
    await acquireMigrationLock(client);
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      file_name text PRIMARY KEY,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`);

    const entries = await readdir(migrationsDir);
    const migrationFiles = entries
      .filter((entry) => entry.endsWith('.sql'))
      .sort();

    for (const fileName of migrationFiles) {
      const applied = await client.query(
        'SELECT 1 FROM schema_migrations WHERE file_name = $1',
        [fileName]
      );
      if ((applied.rowCount ?? 0) > 0) {
        continue;
      }

      const filePath = join(migrationsDir, fileName);
      const sql = await readFile(filePath, 'utf8');

      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query(
          'INSERT INTO schema_migrations (file_name) VALUES ($1)',
          [fileName]
        );
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
  } finally {
    client.off('error', swallowError);
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY]);
    client.release();
  }
}

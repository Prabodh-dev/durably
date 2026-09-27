import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Pool } from 'pg';

export async function runMigrations(
  pool: Pool,
  migrationsDir: string
): Promise<void> {
  await pool.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
    file_name text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
  )`);

  const entries = await readdir(migrationsDir);
  const migrationFiles = entries
    .filter((entry) => entry.endsWith('.sql'))
    .sort();

  for (const fileName of migrationFiles) {
    const applied = await pool.query(
      'SELECT 1 FROM schema_migrations WHERE file_name = $1',
      [fileName]
    );
    if ((applied.rowCount ?? 0) > 0) {
      continue;
    }

    const filePath = join(migrationsDir, fileName);
    const sql = await readFile(filePath, 'utf8');
    const client = await pool.connect();

    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query(
        'INSERT INTO schema_migrations (file_name) VALUES ($1)',
        [fileName]
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

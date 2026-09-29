import { afterAll, beforeAll, describe, expect, test } from 'vitest';

import { createDatabasePool, runMigrations } from '@durably/core';
import type { Pool } from 'pg';

import { startPostgres, stopPostgres } from './harness.js';
import type { PostgresFixture } from './harness.js';
import { fileURLToPath } from 'node:url';

const migrationsDir = fileURLToPath(
  new URL('../../../migrations', import.meta.url)
);

let fixture: PostgresFixture;

beforeAll(async () => {
  fixture = await startPostgres();
}, 180000);

afterAll(async () => {
  await stopPostgres(fixture);
});

async function dropEverything(pool: Pool): Promise<void> {
  await pool.query('DROP SCHEMA public CASCADE');
  await pool.query('CREATE SCHEMA public');
}

describe('concurrent migration runners', () => {
  test('a server and a worker starting together never collide on the same migration', async () => {
    await dropEverything(fixture.pool);

    const runners = await Promise.all(
      Array.from({ length: 4 }, async () => {
        const pool = await createDatabasePool(fixture.databaseUrl);
        try {
          await runMigrations(pool, migrationsDir);
        } finally {
          await pool.end();
        }
      })
    );

    expect(runners).toHaveLength(4);

    const applied = await fixture.pool.query<{ file_name: string }>(
      'SELECT file_name FROM schema_migrations ORDER BY file_name'
    );
    expect(applied.rows.length).toBeGreaterThan(0);
    expect(new Set(applied.rows.map((row) => row.file_name)).size).toBe(
      applied.rows.length
    );

    const tables = await fixture.pool.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'
       ORDER BY table_name`
    );
    const names = tables.rows.map((row) => row.table_name);
    expect(names).toContain('runs');
    expect(names).toContain('steps');
    expect(names).toContain('tasks');
    expect(names).toContain('schedules');
    expect(names).toContain('tenants');
    expect(names).toContain('api_keys');
  }, 120000);

  test('a second run applies nothing and leaves the schema intact', async () => {
    const before = await fixture.pool.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'
       ORDER BY table_name`
    );

    const pool = await createDatabasePool(fixture.databaseUrl);
    try {
      await runMigrations(pool, migrationsDir);
    } finally {
      await pool.end();
    }

    const after = await fixture.pool.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'
       ORDER BY table_name`
    );
    expect(after.rows).toEqual(before.rows);
  }, 120000);
});

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';
import type { FastifyInstance } from 'fastify';

import { createSchedule, tickSchedules } from '@durably/core';
import { createServer } from '@durably/server';

import { startPostgres, stopPostgres } from './harness.js';
import type { PostgresFixture } from './harness.js';

let fixture: PostgresFixture;
let app: FastifyInstance;

beforeAll(async () => {
  fixture = await startPostgres();
  app = await createServer({ databaseUrl: fixture.databaseUrl });
  await app.ready();
});

beforeEach(async () => {
  await truncateAllTables();
});

afterAll(async () => {
  await app.close();
  await stopPostgres(fixture);
});

async function truncateAllTables(): Promise<void> {
  await fixture.pool.query(
    'TRUNCATE TABLE schedules, dead_letters, example_side_effects, steps, tasks, runs RESTART IDENTITY CASCADE'
  );
  await fixture.pool.query("DELETE FROM api_keys WHERE tenant_id <> 'default'");
  await fixture.pool.query("DELETE FROM tenants WHERE id <> 'default'");
  await fixture.pool.query(
    "UPDATE tenants SET last_claim_at = NULL WHERE id = 'default'"
  );
}

describe.sequential('schedule API', () => {
  test('a schedule is created, read, listed, updated and deleted', async () => {
    const created = await app.inject({
      method: 'POST',
      url: '/v1/schedules',
      payload: {
        workflow: 'onboard-user',
        cron: '0 9 * * *',
        timezone: 'America/New_York',
        input: { email: 'api@example.com' },
        catchup: 'none'
      }
    });
    expect(created.statusCode).toBe(201);
    const scheduleId = (created.json() as { id: string }).id;
    expect(created.json()).toMatchObject({
      workflow: 'onboard-user',
      cron: '0 9 * * *',
      timezone: 'America/New_York',
      enabled: true,
      catchup: 'none'
    });

    const fetched = await app.inject({
      method: 'GET',
      url: `/v1/schedules/${scheduleId}`
    });
    expect(fetched.statusCode).toBe(200);
    expect((fetched.json() as { id: string }).id).toBe(scheduleId);

    const listed = await app.inject({ method: 'GET', url: '/v1/schedules' });
    expect(listed.statusCode).toBe(200);
    expect((listed.json() as unknown[]).length).toBe(1);

    const patched = await app.inject({
      method: 'PATCH',
      url: `/v1/schedules/${scheduleId}`,
      payload: { enabled: false }
    });
    expect(patched.statusCode).toBe(200);
    expect((patched.json() as { enabled: boolean }).enabled).toBe(false);

    const reenabled = await app.inject({
      method: 'PATCH',
      url: `/v1/schedules/${scheduleId}`,
      payload: { enabled: true, cron: '30 6 * * *' }
    });
    expect(reenabled.statusCode).toBe(200);
    expect((reenabled.json() as { cron: string }).cron).toBe('30 6 * * *');

    const removed = await app.inject({
      method: 'DELETE',
      url: `/v1/schedules/${scheduleId}`
    });
    expect(removed.statusCode).toBe(204);

    const gone = await app.inject({
      method: 'GET',
      url: `/v1/schedules/${scheduleId}`
    });
    expect(gone.statusCode).toBe(404);
  });

  test('invalid payloads are rejected before they reach the database', async () => {
    const badCron = await app.inject({
      method: 'POST',
      url: '/v1/schedules',
      payload: { workflow: 'onboard-user', cron: 'every day', input: {} }
    });
    expect(badCron.statusCode).toBe(400);

    const badTimezone = await app.inject({
      method: 'POST',
      url: '/v1/schedules',
      payload: {
        workflow: 'onboard-user',
        cron: '0 9 * * *',
        timezone: 'Mars/Olympus',
        input: {}
      }
    });
    expect(badTimezone.statusCode).toBe(400);

    const badId = await app.inject({
      method: 'GET',
      url: '/v1/schedules/not-a-uuid'
    });
    expect(badId.statusCode).toBe(400);

    const emptyPatch = await app.inject({
      method: 'PATCH',
      url: '/v1/schedules/00000000-0000-0000-0000-000000000000',
      payload: {}
    });
    expect(emptyPatch.statusCode).toBe(400);

    const missing = await app.inject({
      method: 'PATCH',
      url: '/v1/schedules/00000000-0000-0000-0000-000000000000',
      payload: { enabled: false }
    });
    expect(missing.statusCode).toBe(404);
  });

  test('a schedule created in the database is visible and picked up by the ticker', async () => {
    const schedule = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 * * * *',
      input: { email: 'ticked@example.com' },
      catchup: 'latest'
    });
    await fixture.pool.query(
      "UPDATE schedules SET created_at = now() - interval '70 minutes', last_fire_time = NULL WHERE id = $1",
      [schedule.id]
    );

    const listed = await app.inject({
      method: 'GET',
      url: `/v1/schedules/${schedule.id}`
    });
    expect(listed.statusCode).toBe(200);

    const outcomes = await tickSchedules(fixture.pool, new Date());
    expect(outcomes).toHaveLength(1);
    expect(outcomes[0]?.firedAt).not.toBeNull();
  });
});

import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';
import type { FastifyInstance } from 'fastify';

import {
  claimTasks,
  createRun,
  createTenant,
  getTenant,
  hashApiKey,
  issueApiKey,
  markTaskDone,
  resolveApiKeyTenant,
  tenantRunningTasks,
  updateTenant
} from '@durably/core';
import type { TaskRecord } from '@durably/core';
import { createServer } from '@durably/server';

import {
  startPostgres,
  stopPostgres,
  truncateAll,
  waitForCondition
} from './harness.js';
import type { PostgresFixture } from './harness.js';

const ADMIN_KEY = 'admin-secret-for-tests';

let fixture: PostgresFixture;
let app: FastifyInstance;

beforeAll(async () => {
  fixture = await startPostgres();
  app = await createServer({
    databaseUrl: fixture.databaseUrl,
    adminKey: ADMIN_KEY
  });
  await app.ready();
});

beforeEach(async () => {
  await truncateAll(fixture.pool);
});

afterAll(async () => {
  await app.close();
  await stopPostgres(fixture);
});

async function seedRun(
  tenantId: string,
  options: { priority?: number; email?: string } = {}
): Promise<string> {
  const run = await createRun(fixture.pool, {
    tenantId,
    workflow: 'onboard-user',
    input: {
      email: options.email ?? `${tenantId}@example.com`,
      name: 'Fairness Tester',
      plan: 'pro'
    },
    ...(options.priority !== undefined ? { priority: options.priority } : {})
  });
  return run.id;
}

describe.sequential('tenant capacity in the claim path', () => {
  test('a tenant never exceeds its max concurrent task count', async () => {
    await createTenant(fixture.pool, {
      id: 'capped',
      name: 'Capped',
      maxConcurrentTasks: 2
    });
    for (let index = 0; index < 5; index += 1) {
      await seedRun('capped', { email: `capped-${index}@example.com` });
    }

    const first = await claimTasks(fixture.pool, {
      workerId: 'w1',
      limit: 10,
      leaseMs: 60_000
    });
    expect(first.tasks).toHaveLength(2);

    const second = await claimTasks(fixture.pool, {
      workerId: 'w2',
      limit: 10,
      leaseMs: 60_000
    });
    expect(second.tasks).toHaveLength(0);
    expect(await tenantRunningTasks(fixture.pool, 'capped')).toBe(2);

    const finished = first.tasks[0] as TaskRecord;
    expect(
      await markTaskDone(fixture.pool, finished.id, finished.lease_token ?? '')
    ).toBe(true);

    const third = await claimTasks(fixture.pool, {
      workerId: 'w3',
      limit: 10,
      leaseMs: 60_000
    });
    expect(third.tasks).toHaveLength(1);
    expect(await tenantRunningTasks(fixture.pool, 'capped')).toBe(2);
  });

  test('an uncapped tenant is not limited by a neighbour at its cap', async () => {
    await createTenant(fixture.pool, {
      id: 'capped',
      name: 'Capped',
      maxConcurrentTasks: 1
    });
    await createTenant(fixture.pool, { id: 'open', name: 'Open' });
    await seedRun('capped', { email: 'capped@example.com' });
    for (let index = 0; index < 4; index += 1) {
      await seedRun('open', { email: `open-${index}@example.com` });
    }

    const claimed = await claimTasks(fixture.pool, {
      workerId: 'w1',
      limit: 10,
      leaseMs: 60_000
    });
    const byTenant = new Map<string, number>();
    for (const task of claimed.tasks) {
      byTenant.set(task.tenant_id, (byTenant.get(task.tenant_id) ?? 0) + 1);
    }
    expect(byTenant.get('capped')).toBe(1);
    expect(byTenant.get('open')).toBe(4);
  });

  test('one tenant backlog does not starve another tenant', async () => {
    await createTenant(fixture.pool, { id: 'tenant-a', name: 'A' });
    await createTenant(fixture.pool, { id: 'tenant-b', name: 'B' });
    for (let index = 0; index < 20; index += 1) {
      await seedRun('tenant-a', { email: `a-${index}@example.com` });
    }
    await seedRun('tenant-b', { email: 'b-0@example.com' });

    const claimedTenants: string[] = [];
    for (let round = 0; round < 4; round += 1) {
      const claimed = await claimTasks(fixture.pool, {
        workerId: `w-${round}`,
        limit: 1,
        leaseMs: 60_000
      });
      expect(claimed.tasks).toHaveLength(1);
      claimedTenants.push((claimed.tasks[0] as TaskRecord).tenant_id);
    }

    expect(new Set(claimedTenants).size).toBe(2);
    expect(claimedTenants).toContain('tenant-b');
  });

  test('a run priority still orders tasks inside a tenant', async () => {
    await createTenant(fixture.pool, { id: 'prio', name: 'Priority' });
    await seedRun('prio', { priority: 1, email: 'low@example.com' });
    await seedRun('prio', { priority: 9, email: 'high@example.com' });

    const claimed = await claimTasks(fixture.pool, {
      workerId: 'w1',
      limit: 10,
      leaseMs: 60_000
    });
    expect(claimed.tasks).toHaveLength(2);
    expect(claimed.tasks.map((task) => task.priority)).toEqual([9, 1]);
  });

  test('a run cannot be created for a tenant that does not exist', async () => {
    await expect(
      createRun(fixture.pool, {
        tenantId: 'ghost',
        workflow: 'onboard-user',
        input: {}
      })
    ).rejects.toThrow(/unknown tenant/);
  });

  test('concurrent claimers never push a tenant past its cap', async () => {
    await createTenant(fixture.pool, {
      id: 'raced',
      name: 'Raced',
      maxConcurrentTasks: 3
    });
    for (let index = 0; index < 40; index += 1) {
      await seedRun('raced', { email: `raced-${index}@example.com` });
    }

    const claimers = Array.from({ length: 6 }, (_unused, index) =>
      claimTasks(fixture.pool, {
        workerId: `racer-${index}`,
        limit: 5,
        leaseMs: 60_000
      })
    );
    const results = await Promise.all(claimers);
    const total = results.reduce((sum, result) => sum + result.tasks.length, 0);
    expect(total).toBe(3);

    const claimedIds = new Set(
      results.flatMap((result) => result.tasks.map((task) => task.id))
    );
    expect(claimedIds.size).toBe(total);
  });
});

describe.sequential('api keys and tenant resolution', () => {
  test('only the hash and a lookup prefix are stored', async () => {
    await createTenant(fixture.pool, { id: 'secure', name: 'Secure' });
    const issued = await issueApiKey(fixture.pool, {
      tenantId: 'secure',
      name: 'primary'
    });

    expect(issued.secret.startsWith('dk_')).toBe(true);
    expect(issued.key.key_prefix).toBe(issued.secret.slice(0, 12));
    expect(issued.key.key_prefix).not.toBe(issued.secret);

    const stored = await fixture.pool.query<{ key_hash: string }>(
      'SELECT key_hash FROM api_keys WHERE id = $1',
      [issued.key.id]
    );
    expect(stored.rows[0]?.key_hash).toBe(hashApiKey(issued.secret));
    expect(stored.rows[0]?.key_hash).not.toBe(issued.secret);

    const secretLeak = await fixture.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM api_keys WHERE name LIKE '%' || $1 || '%'`,
      [issued.secret]
    );
    expect(secretLeak.rows[0]?.count).toBe(0);

    expect(await resolveApiKeyTenant(fixture.pool, issued.secret)).toBe(
      'secure'
    );
  });

  test('a revoked key stops resolving', async () => {
    await createTenant(fixture.pool, { id: 'rotating', name: 'Rotating' });
    const issued = await issueApiKey(fixture.pool, {
      tenantId: 'rotating',
      name: 'primary'
    });
    await fixture.pool.query(
      'UPDATE api_keys SET revoked_at = now() WHERE id = $1',
      [issued.key.id]
    );
    await expect(
      resolveApiKeyTenant(fixture.pool, issued.secret)
    ).rejects.toThrow(/invalid or revoked api key/);
  });

  test('an unauthenticated request runs as the default tenant and a key switches tenant', async () => {
    await createTenant(fixture.pool, { id: 'tenant-x', name: 'Tenant X' });
    const issued = await issueApiKey(fixture.pool, {
      tenantId: 'tenant-x',
      name: 'primary'
    });

    const anonymous = await app.inject({
      method: 'POST',
      url: '/v1/runs',
      payload: {
        workflow: 'onboard-user',
        input: { email: 'anon@example.com' }
      }
    });
    expect(anonymous.statusCode).toBe(201);
    expect((anonymous.json() as { tenant_id: string }).tenant_id).toBe(
      'default'
    );

    const authenticated = await app.inject({
      method: 'POST',
      url: '/v1/runs',
      headers: { authorization: `Bearer ${issued.secret}` },
      payload: {
        workflow: 'onboard-user',
        input: { email: 'x@example.com' }
      }
    });
    expect(authenticated.statusCode).toBe(201);
    expect((authenticated.json() as { tenant_id: string }).tenant_id).toBe(
      'tenant-x'
    );
  });

  test('an unknown key is rejected', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/runs',
      headers: { authorization: 'Bearer dk_not-a-real-key' }
    });
    expect(response.statusCode).toBe(401);
  });
});

describe.sequential('cross tenant isolation', () => {
  test('a run from another tenant reads as not found everywhere', async () => {
    await createTenant(fixture.pool, { id: 'owner', name: 'Owner' });
    await createTenant(fixture.pool, { id: 'intruder', name: 'Intruder' });
    const run = await createRun(fixture.pool, {
      tenantId: 'owner',
      workflow: 'onboard-user',
      input: { email: 'owner@example.com' }
    });
    const ownerKey = (
      await issueApiKey(fixture.pool, { tenantId: 'owner', name: 'primary' })
    ).secret;
    const intruderKey = (
      await issueApiKey(fixture.pool, { tenantId: 'intruder', name: 'primary' })
    ).secret;

    const asOwner = await app.inject({
      method: 'GET',
      url: `/v1/runs/${run.id}`,
      headers: { authorization: `Bearer ${ownerKey}` }
    });
    expect(asOwner.statusCode).toBe(200);

    const asIntruder = await app.inject({
      method: 'GET',
      url: `/v1/runs/${run.id}`,
      headers: { authorization: `Bearer ${intruderKey}` }
    });
    expect(asIntruder.statusCode).toBe(404);

    const cancelled = await app.inject({
      method: 'POST',
      url: `/v1/runs/${run.id}/cancel`,
      headers: { authorization: `Bearer ${intruderKey}` }
    });
    expect(cancelled.statusCode).toBe(404);

    const stillPending = await fixture.pool.query<{ status: string }>(
      'SELECT status FROM runs WHERE id = $1',
      [run.id]
    );
    expect(stillPending.rows[0]?.status).toBe('pending');

    const ownerList = await app.inject({
      method: 'GET',
      url: '/v1/runs',
      headers: { authorization: `Bearer ${ownerKey}` }
    });
    const intruderList = await app.inject({
      method: 'GET',
      url: '/v1/runs',
      headers: { authorization: `Bearer ${intruderKey}` }
    });
    expect((ownerList.json() as unknown[]).length).toBe(1);
    expect((intruderList.json() as unknown[]).length).toBe(0);
  });

  test('a schedule from another tenant reads as not found', async () => {
    await createTenant(fixture.pool, { id: 'sched-owner', name: 'Owner' });
    await createTenant(fixture.pool, { id: 'sched-other', name: 'Other' });
    const created = await app.inject({
      method: 'POST',
      url: '/v1/schedules',
      headers: {
        authorization: `Bearer ${
          (
            await issueApiKey(fixture.pool, {
              tenantId: 'sched-owner',
              name: 'primary'
            })
          ).secret
        }`
      },
      payload: { workflow: 'onboard-user', cron: '0 9 * * *', input: {} }
    });
    const scheduleId = (created.json() as { id: string }).id;

    const other = await app.inject({
      method: 'GET',
      url: `/v1/schedules/${scheduleId}`,
      headers: {
        authorization: `Bearer ${
          (
            await issueApiKey(fixture.pool, {
              tenantId: 'sched-other',
              name: 'primary'
            })
          ).secret
        }`
      }
    });
    expect(other.statusCode).toBe(404);

    const removed = await app.inject({
      method: 'DELETE',
      url: `/v1/schedules/${scheduleId}`,
      headers: {
        authorization: `Bearer ${
          (
            await issueApiKey(fixture.pool, {
              tenantId: 'sched-other',
              name: 'primary'
            })
          ).secret
        }`
      }
    });
    expect(removed.statusCode).toBe(404);

    const survivor = await fixture.pool.query<{ count: number }>(
      'SELECT count(*)::int AS count FROM schedules WHERE id = $1',
      [scheduleId]
    );
    expect(survivor.rows[0]?.count).toBe(1);
  });

  test('the tenant cannot widen its own concurrency cap', async () => {
    await createTenant(fixture.pool, {
      id: 'escalate',
      name: 'Escalate',
      maxConcurrentTasks: 1
    });
    const key = (
      await issueApiKey(fixture.pool, {
        tenantId: 'escalate',
        name: 'primary'
      })
    ).secret;

    const escalation = await app.inject({
      method: 'PATCH',
      url: '/v1/admin/tenants/escalate',
      headers: { authorization: `Bearer ${key}` },
      payload: { maxConcurrentTasks: 500 }
    });
    expect(escalation.statusCode).toBe(404);

    const tenant = await getTenant(fixture.pool, 'escalate');
    expect(tenant?.max_concurrent_tasks).toBe(1);
  });
});

describe.sequential('admin endpoints', () => {
  test('admin routes are hidden without the admin key', async () => {
    const anonymous = await app.inject({
      method: 'GET',
      url: '/v1/admin/tenants'
    });
    expect(anonymous.statusCode).toBe(404);

    const wrongKey = await app.inject({
      method: 'GET',
      url: '/v1/admin/tenants',
      headers: { authorization: 'Bearer nope' }
    });
    expect(wrongKey.statusCode).toBe(404);

    const tenantKey = (
      await issueApiKey(fixture.pool, { tenantId: 'default', name: 'primary' })
    ).secret;
    const asTenant = await app.inject({
      method: 'GET',
      url: '/v1/admin/tenants',
      headers: { authorization: `Bearer ${tenantKey}` }
    });
    expect(asTenant.statusCode).toBe(404);
  });

  test('the admin key provisions tenants and api keys', async () => {
    const adminHeaders = { authorization: `Bearer ${ADMIN_KEY}` };
    const created = await app.inject({
      method: 'POST',
      url: '/v1/admin/tenants',
      headers: adminHeaders,
      payload: { id: 'provisioned', name: 'Provisioned', maxConcurrentTasks: 4 }
    });
    expect(created.statusCode).toBe(201);
    expect(created.json()).toMatchObject({
      id: 'provisioned',
      max_concurrent_tasks: 4
    });

    const listed = await app.inject({
      method: 'GET',
      url: '/v1/admin/tenants',
      headers: adminHeaders
    });
    const tenants = listed.json() as Array<{ id: string }>;
    expect(tenants.map((tenant) => tenant.id)).toContain('provisioned');
    expect(tenants.map((tenant) => tenant.id)).toContain('default');

    const issued = await app.inject({
      method: 'POST',
      url: '/v1/admin/tenants/provisioned/keys',
      headers: adminHeaders,
      payload: { name: 'primary' }
    });
    expect(issued.statusCode).toBe(201);
    const payload = issued.json() as { id: string; secret: string };
    expect(payload.secret.length).toBeGreaterThan(20);

    const keys = await app.inject({
      method: 'GET',
      url: '/v1/admin/tenants/provisioned/keys',
      headers: adminHeaders
    });
    expect((keys.json() as unknown[]).length).toBe(1);

    const run = await app.inject({
      method: 'POST',
      url: '/v1/runs',
      headers: { authorization: `Bearer ${payload.secret}` },
      payload: { workflow: 'onboard-user', input: { email: 'p@example.com' } }
    });
    expect((run.json() as { tenant_id: string }).tenant_id).toBe('provisioned');

    const revoked = await app.inject({
      method: 'DELETE',
      url: `/v1/admin/tenants/provisioned/keys/${payload.id}`,
      headers: adminHeaders
    });
    expect(revoked.statusCode).toBe(204);

    const afterRevoke = await app.inject({
      method: 'GET',
      url: '/v1/runs',
      headers: { authorization: `Bearer ${payload.secret}` }
    });
    expect(afterRevoke.statusCode).toBe(401);
  });

  test('a cap raised through the admin api takes effect on the next claim', async () => {
    const adminHeaders = { authorization: `Bearer ${ADMIN_KEY}` };
    await createTenant(fixture.pool, {
      id: 'ramped',
      name: 'Ramped',
      maxConcurrentTasks: 1
    });
    for (let index = 0; index < 4; index += 1) {
      await seedRun('ramped', { email: `ramped-${index}@example.com` });
    }

    const before = await claimTasks(fixture.pool, {
      workerId: 'w1',
      limit: 10,
      leaseMs: 60_000
    });
    expect(before.tasks).toHaveLength(1);

    const raised = await app.inject({
      method: 'PATCH',
      url: '/v1/admin/tenants/ramped',
      headers: adminHeaders,
      payload: { maxConcurrentTasks: 3 }
    });
    expect(raised.statusCode).toBe(200);

    const after = await claimTasks(fixture.pool, {
      workerId: 'w2',
      limit: 10,
      leaseMs: 60_000
    });
    expect(after.tasks).toHaveLength(2);
    expect(await tenantRunningTasks(fixture.pool, 'ramped')).toBe(3);
  });

  test('an updated tenant name is persisted', async () => {
    await updateTenant(fixture.pool, 'default', { name: 'Primary' });
    const tenant = await getTenant(fixture.pool, 'default');
    expect(tenant?.name).toBe('Primary');
  });
});

describe.sequential('required api key mode', () => {
  test('an unauthenticated request is refused when keys are required', async () => {
    const strict = await createServer({
      databaseUrl: fixture.databaseUrl,
      requireApiKey: true
    });
    await strict.ready();
    try {
      const refused = await strict.inject({
        method: 'GET',
        url: '/v1/runs'
      });
      expect(refused.statusCode).toBe(401);

      const key = (
        await issueApiKey(fixture.pool, { tenantId: 'default', name: 'strict' })
      ).secret;
      const allowed = await strict.inject({
        method: 'GET',
        url: '/v1/runs',
        headers: { authorization: `Bearer ${key}` }
      });
      expect(allowed.statusCode).toBe(200);

      const health = await strict.inject({ method: 'GET', url: '/healthz' });
      expect(health.statusCode).toBe(200);
    } finally {
      await strict.close();
    }
  });
});

describe.sequential('worker fairness under real processes', () => {
  test('a backlog for one tenant does not block another tenant', async () => {
    await createTenant(fixture.pool, { id: 'busy', name: 'Busy' });
    await createTenant(fixture.pool, { id: 'quiet', name: 'Quiet' });
    for (let index = 0; index < 25; index += 1) {
      await seedRun('busy', { email: `busy-${index}@example.com` });
    }
    const quietRun = await seedRun('quiet', { email: 'quiet@example.com' });

    const { spawnWorkerProcess, stopWorkerProcess } =
      await import('./harness.js');
    const workers = ['fw1', 'fw2'].map((workerId) =>
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId,
        env: {
          DURABLY_WORKER_CONCURRENCY: '2',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '2000',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    try {
      const completed = await waitForCondition(
        async () => {
          const result = await fixture.pool.query<{ status: string }>(
            'SELECT status FROM runs WHERE id = $1',
            [quietRun]
          );
          return result.rows[0]?.status === 'completed'
            ? 'completed'
            : undefined;
        },
        (value) => value === 'completed',
        60000
      );
      expect(completed).toBe('completed');
    } catch (error) {
      const failures = await fixture.pool.query<{
        step_key: string;
        status: string;
        attempts: number;
        last_error: unknown;
      }>(
        `SELECT s.step_key, s.status, s.attempts, s.last_error
         FROM steps s JOIN runs r ON r.id = s.run_id
         WHERE r.tenant_id = 'busy' AND s.status <> 'completed'
         LIMIT 5`
      );
      throw new Error(
        `${String(error)}\n${JSON.stringify(failures.rows, null, 2)}\n${workers
          .map((worker) => worker.recentOutput())
          .join('\n')}`
      );
    } finally {
      for (const worker of workers) {
        await stopWorkerProcess(worker);
      }
    }
  }, 120000);
});

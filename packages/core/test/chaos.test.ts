import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';
import { Client } from 'pg';

import {
  createRun,
  createSchedule,
  createTenant,
  getRunWithSteps
} from '@durably/core';
import { createServer } from '@durably/server';

import {
  killWorkerProcess,
  spawnWorkerProcess,
  startPostgres,
  startToxiproxy,
  stopPostgres,
  stopWorkerProcess,
  truncateAll,
  waitForCondition
} from './harness.js';
import type {
  PostgresFixture,
  ToxiproxyFixture,
  WorkerProcess
} from './harness.js';

const CHAOS_STEPS = ['first', 'slow', 'last'] as const;

let fixture: PostgresFixture;
let toxiproxy: ToxiproxyFixture | null = null;
const workers: WorkerProcess[] = [];

function track(worker: WorkerProcess): WorkerProcess {
  workers.push(worker);
  return worker;
}

function chaosEnv(extra: Record<string, string> = {}): Record<string, string> {
  return {
    DURABLY_WORKER_CONCURRENCY: '1',
    DURABLY_WORKER_POLL_INTERVAL_MS: '50',
    DURABLY_WORKER_LEASE_MS: '2000',
    DURABLY_WORKER_REAPER_INTERVAL_MS: '200',
    DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
    DURABLY_WORKER_CRON_INTERVAL_MS: '200',
    ONBOARD_USER_FAILURE_RATE: '0',
    ...extra
  };
}

async function waitForCompletion(runId: string, timeoutMs: number) {
  return waitForCondition(
    async () => {
      const current = await getRunWithSteps(fixture.pool, runId);
      const status = current.run?.status;
      return status === 'completed' || status === 'failed'
        ? current
        : undefined;
    },
    (result) => result.run?.status === 'completed',
    timeoutMs,
    100
  );
}

async function runDiagnostics(runId: string): Promise<string> {
  const run = await fixture.pool.query<Record<string, unknown>>(
    'SELECT * FROM runs WHERE id = $1',
    [runId]
  );
  const tasks = await fixture.pool.query<Record<string, unknown>>(
    'SELECT id, status, attempts, run_at, locked_by, lease_expires_at, last_error FROM tasks WHERE run_id = $1 ORDER BY run_at',
    [runId]
  );
  const steps = await fixture.pool.query<Record<string, unknown>>(
    'SELECT step_key, status, attempts, last_error FROM steps WHERE run_id = $1 ORDER BY step_key',
    [runId]
  );
  return JSON.stringify(
    { run: run.rows, tasks: tasks.rows, steps: steps.rows },
    null,
    2
  );
}

async function executionCounts(runId: string): Promise<Record<string, number>> {
  const result = await fixture.pool.query<{ step_key: string; count: number }>(
    `SELECT step_key, count(*)::int AS count
     FROM example_side_effects
     WHERE run_id = $1 AND idempotency_key LIKE $2
     GROUP BY step_key`,
    [runId, `${runId}:%:exec:%`]
  );
  return Object.fromEntries(
    result.rows.map((row) => [row.step_key, row.count])
  );
}

async function stepAttempts(runId: string): Promise<Record<string, number>> {
  const result = await fixture.pool.query<{
    step_key: string;
    attempts: number;
    status: string;
  }>('SELECT step_key, attempts, status FROM steps WHERE run_id = $1', [runId]);
  const records: Record<string, number> = {};
  for (const row of result.rows) {
    expect(row.status, `${row.step_key} must be completed`).toBe('completed');
    records[row.step_key] = row.attempts;
  }
  return records;
}

/**
 * A step records `attempts` only when its outcome is written. An execution lost
 * to a process kill or a severed database connection is never recorded, which is
 * exactly the at-least-once case. So executions may exceed recorded attempts by
 * at most one per injected fault, and a step that completed before the fault
 * must never execute again.
 */
async function assertNoCompletedStepReexecuted(
  runId: string,
  options: {
    singleExecution: ReadonlyArray<(typeof CHAOS_STEPS)[number]>;
    unrecordedExecutions: number;
  }
): Promise<void> {
  const executions = await executionCounts(runId);
  const attempts = await stepAttempts(runId);

  for (const stepKey of CHAOS_STEPS) {
    const count = executions[stepKey] ?? 0;
    const recorded = attempts[stepKey] ?? 0;
    expect(count, `${stepKey} must have executed`).toBeGreaterThan(0);
    expect(
      count,
      `${stepKey} executions must not exceed recorded attempts plus the unrecorded ones`
    ).toBeLessThanOrEqual(recorded + options.unrecordedExecutions);
    expect(
      count,
      `${stepKey} recorded attempts must not exceed real executions`
    ).toBeGreaterThanOrEqual(recorded);
  }

  for (const stepKey of options.singleExecution) {
    expect(
      executions[stepKey],
      `${stepKey} completed before the fault and must never run again`
    ).toBe(1);
  }
}

beforeAll(async () => {
  fixture = await startPostgres();
});

beforeEach(async () => {
  await truncateAll(fixture.pool);
  await toxiproxy?.setLatency(0);
  await toxiproxy?.cutConnection(false);
});

afterEach(async () => {
  await toxiproxy?.setLatency(0);
  await toxiproxy?.cutConnection(false);
  for (const worker of workers.splice(0)) {
    await stopWorkerProcess(worker);
  }
});

afterAll(async () => {
  await toxiproxy?.stop();
  await stopPostgres(fixture);
});

describe.sequential('chaos: worker process faults', () => {
  test('killing a worker mid step does not re-execute a completed step and the run still finishes', async () => {
    const run = await createRun(fixture.pool, {
      workflow: 'chaos',
      input: {}
    });

    const victim = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'chaos-victim',
        env: chaosEnv({ DURABLY_CHAOS_STEP_DELAY_MS: '20000' })
      })
    );

    const startedSlow = await waitForCondition(
      async () => {
        const steps = await fixture.pool.query<{ count: number }>(
          `SELECT count(*)::int AS count
           FROM example_side_effects
           WHERE run_id = $1 AND step_key = 'slow'`,
          [run.id]
        );
        return (steps.rows[0]?.count ?? 0) > 0 ? 'started' : undefined;
      },
      (value) => value === 'started',
      30000,
      50
    );
    expect(startedSlow).toBe('started');

    const firstExecutions = await executionCounts(run.id);
    expect(firstExecutions.first).toBe(1);
    expect(firstExecutions.slow).toBe(1);

    await killWorkerProcess(victim);

    const survivor = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'chaos-survivor',
        env: chaosEnv()
      })
    );
    expect(survivor.workerId).toBe('chaos-survivor');

    const finished = await waitForCompletion(run.id, 120000);
    expect(finished.run?.status).toBe('completed');
    expect(finished.steps).toHaveLength(CHAOS_STEPS.length);

    await assertNoCompletedStepReexecuted(run.id, {
      singleExecution: ['first', 'last'],
      unrecordedExecutions: 1
    });
  }, 200000);

  test('killing the leader during a cron tick never produces a duplicate run', async () => {
    const schedule = await createSchedule(fixture.pool, {
      workflow: 'chaos',
      cron: '* * * * *',
      input: {},
      catchup: 'latest'
    });
    await fixture.pool.query(
      "UPDATE schedules SET created_at = now() - interval '70 minutes', last_fire_time = NULL WHERE id = $1",
      [schedule.id]
    );

    const cronWorkers = ['cw-a', 'cw-b', 'cw-c'].map((workerId) =>
      track(
        spawnWorkerProcess({
          databaseUrl: fixture.databaseUrl,
          workerId,
          env: chaosEnv()
        })
      )
    );

    const first = await waitForCondition(
      async () => {
        const runs = await fixture.pool.query<{ id: string }>(
          'SELECT id FROM runs WHERE idempotency_key LIKE $1 ORDER BY id LIMIT 1',
          [`${schedule.id}:%`]
        );
        return runs.rows[0]?.id;
      },
      (value) => value !== undefined,
      150000,
      100
    );
    expect(first).toBeDefined();

    const leaderBeforeKill = cronWorkers.find((worker) =>
      worker.leaderEvents.some((event) => event.event === 'leader_acquired')
    );
    expect(leaderBeforeKill).toBeDefined();
    await killWorkerProcess(leaderBeforeKill as WorkerProcess);

    const keys = await waitForCondition(
      async () => {
        const runs = await fixture.pool.query<{ idempotency_key: string }>(
          'SELECT idempotency_key FROM runs WHERE idempotency_key LIKE $1',
          [`${schedule.id}:%`]
        );
        return runs.rows.length >= 2
          ? runs.rows.map((row) => row.idempotency_key)
          : undefined;
      },
      (found) => new Set(found).size === found.length && found.length >= 2,
      150000,
      200
    );

    expect(new Set(keys).size).toBe(keys.length);
    for (const key of keys) {
      expect(key.startsWith(`${schedule.id}:`)).toBe(true);
    }
  }, 400000);
});

describe.sequential('chaos: database faults through toxiproxy', () => {
  test('a database connection cut mid step does not lose the run or repeat a completed step', async () => {
    toxiproxy = await startToxiproxy(fixture, 8667);
    await truncateAll(fixture.pool);

    const run = await createRun(fixture.pool, {
      workflow: 'chaos',
      input: {}
    });

    const worker = track(
      spawnWorkerProcess({
        databaseUrl: toxiproxy.databaseUrl,
        workerId: 'chaos-cut',
        env: chaosEnv({ DURABLY_CHAOS_STEP_DELAY_MS: '8000' })
      })
    );
    expect(worker.workerId).toBe('chaos-cut');

    const inFlight = await waitForCondition(
      async () => {
        const steps = await fixture.pool.query<{ count: number }>(
          `SELECT count(*)::int AS count
           FROM example_side_effects
           WHERE run_id = $1 AND step_key = 'slow'`,
          [run.id]
        );
        return (steps.rows[0]?.count ?? 0) > 0 ? 'in-flight' : undefined;
      },
      (value) => value === 'in-flight',
      30000,
      50
    );
    expect(inFlight).toBe('in-flight');

    await toxiproxy.cutConnection(true);
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await toxiproxy.cutConnection(false);

    let finished;
    try {
      finished = await waitForCompletion(run.id, 60000);
    } catch (error) {
      let probe = 'probe not run';
      try {
        const client = new Client({ connectionString: toxiproxy.databaseUrl });
        await client.connect();
        const result = await client.query<{ ok: number }>('SELECT 1 AS ok');
        probe = `proxy reachable: ${JSON.stringify(result.rows)}`;
        await client.end();
      } catch (probeError) {
        probe = `proxy unreachable: ${String(probeError)}`;
      }
      const state = await fetch(
        `${toxiproxy.controlUrl}/proxies/${toxiproxy.proxyName}`
      ).then((response) => response.text());
      const logs = (await toxiproxy.container.getLogs()).slice(-15).join('\n');
      throw new Error(
        `${String(error)}\n${probe}\nproxy state: ${state}\nproxy logs:\n${logs}\nworker exited: ${worker.exited()}\n${await runDiagnostics(run.id)}`
      );
    }
    expect(finished.run?.status).toBe('completed');
    await assertNoCompletedStepReexecuted(run.id, {
      singleExecution: ['first', 'last'],
      unrecordedExecutions: 1
    });
  }, 300000);

  test('sustained database latency still completes every run', async () => {
    toxiproxy = await startToxiproxy(fixture, 8668);
    await truncateAll(fixture.pool);

    const run = await createRun(fixture.pool, {
      workflow: 'chaos',
      input: {}
    });

    const worker = track(
      spawnWorkerProcess({
        databaseUrl: toxiproxy.databaseUrl,
        workerId: 'chaos-latency',
        env: chaosEnv({ DURABLY_WORKER_LEASE_MS: '4000' })
      })
    );
    expect(worker.workerId).toBe('chaos-latency');

    await toxiproxy.setLatency(120);

    const finished = await waitForCompletion(run.id, 180000);
    expect(finished.run?.status).toBe('completed');
    await assertNoCompletedStepReexecuted(run.id, {
      singleExecution: ['first', 'slow', 'last'],
      unrecordedExecutions: 0
    });
  }, 300000);
});

describe.sequential('chaos: tenancy invariants hold under load', () => {
  test('many concurrent runs across tenants all reach a terminal state', async () => {
    await createTenant(fixture.pool, { id: 'chaos-a', name: 'Chaos A' });
    await createTenant(fixture.pool, { id: 'chaos-b', name: 'Chaos B' });

    const runIds: string[] = [];
    for (let index = 0; index < 12; index += 1) {
      const run = await createRun(fixture.pool, {
        tenantId: index % 2 === 0 ? 'chaos-a' : 'chaos-b',
        workflow: 'chaos',
        input: {}
      });
      runIds.push(run.id);
    }

    for (const workerId of ['load-1', 'load-2']) {
      track(
        spawnWorkerProcess({
          databaseUrl: fixture.databaseUrl,
          workerId,
          env: chaosEnv({ DURABLY_WORKER_CONCURRENCY: '3' })
        })
      );
    }

    await waitForCondition(
      async () => {
        const result = await fixture.pool.query<{ count: number }>(
          `SELECT count(*)::int AS count
           FROM runs
           WHERE id = ANY($1::uuid[])
             AND status IN ('completed', 'failed', 'cancelled')`,
          [runIds]
        );
        return result.rows[0]?.count === runIds.length ? 'done' : undefined;
      },
      (value) => value === 'done',
      180000,
      200
    );

    const statuses = await fixture.pool.query<{
      status: string;
      count: number;
    }>(
      `SELECT status, count(*)::int AS count
       FROM runs WHERE id = ANY($1::uuid[]) GROUP BY status`,
      [runIds]
    );
    for (const row of statuses.rows) {
      expect(row.status).toBe('completed');
    }

    for (const runId of runIds) {
      await assertNoCompletedStepReexecuted(runId, {
        singleExecution: ['first', 'last'],
        unrecordedExecutions: 0
      });
    }
  }, 300000);

  test('the api keeps serving while a worker is being killed', async () => {
    const app = await createServer({ databaseUrl: fixture.databaseUrl });
    await app.ready();
    const victim = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'api-victim',
        env: chaosEnv({ DURABLY_CHAOS_STEP_DELAY_MS: '20000' })
      })
    );
    try {
      const run = await createRun(fixture.pool, {
        workflow: 'chaos',
        input: {}
      });
      await waitForCondition(
        async () => {
          const response = await app.inject({
            method: 'GET',
            url: `/v1/runs/${run.id}`
          });
          const body = response.json() as {
            steps?: Array<{ step_key: string }>;
          };
          return (body.steps?.length ?? 0) >= 1 ? 'running' : undefined;
        },
        (value) => value === 'running',
        30000,
        100
      );

      await killWorkerProcess(victim);

      const health = await app.inject({ method: 'GET', url: '/healthz' });
      expect(health.statusCode).toBe(200);

      const metrics = await app.inject({ method: 'GET', url: '/metrics' });
      expect(metrics.statusCode).toBe(200);
      expect(metrics.body).toContain('durably_queue_depth');
    } finally {
      await app.close();
    }
  }, 120000);
});

import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';

import {
  claimTasks,
  createRun,
  getRunWithSteps,
  parseDurationMs
} from '@durably/core';

import {
  killWorkerProcess,
  spawnWorkerProcess,
  startPostgres,
  stopPostgres,
  stopWorkerProcess,
  truncateAll,
  waitForCondition
} from './harness.js';
import type { PostgresFixture, WorkerProcess } from './harness.js';

let fixture: PostgresFixture;
const workerProcesses: WorkerProcess[] = [];

function track(worker: WorkerProcess): WorkerProcess {
  workerProcesses.push(worker);
  return worker;
}

beforeAll(async () => {
  fixture = await startPostgres();
});

beforeEach(async () => {
  await truncateAll(fixture.pool);
});

afterEach(async () => {
  for (const worker of workerProcesses.splice(0)) {
    await stopWorkerProcess(worker);
  }
});

describe.sequential('duration parsing', () => {
  test('parses second, minute, day, hour, week and millisecond units', () => {
    expect(parseDurationMs('30s')).toBe(30_000);
    expect(parseDurationMs('5m')).toBe(300_000);
    expect(parseDurationMs('3d')).toBe(259_200_000);
    expect(parseDurationMs('2h')).toBe(7_200_000);
    expect(parseDurationMs('1w')).toBe(604_800_000);
    expect(parseDurationMs('250ms')).toBe(250);
    expect(parseDurationMs(' 1.5m ')).toBe(90_000);
    expect(parseDurationMs(1500)).toBe(1500);
  });

  test('rejects malformed durations', () => {
    expect(() => parseDurationMs('soon')).toThrow(/invalid duration/);
    expect(() => parseDurationMs('30')).toThrow(/invalid duration/);
    expect(() => parseDurationMs('m')).toThrow(/invalid duration/);
    expect(() => parseDurationMs('30y')).toThrow(/invalid duration/);
    expect(() => parseDurationMs(Number.NaN)).toThrow(/invalid duration/);
  });
});

describe.sequential('sleep and timers', () => {
  test('a sleeping run wakes on time, releases the worker slot, and keeps the wake time across a crash', async () => {
    const worker = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'sleep-1',
        env: {
          DURABLY_WORKER_CONCURRENCY: '1',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '1000',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    const sleeper = await createRun(fixture.pool, {
      workflow: 'delayed-approval',
      input: { email: 'sleeper@example.com', delay: '6s' },
      idempotencyKey: 'sleep-sleeper'
    });
    const fast = await createRun(fixture.pool, {
      workflow: 'onboard-user',
      input: { email: 'fast@example.com', name: 'Fast', plan: 'pro' },
      idempotencyKey: 'sleep-fast'
    });

    const sleeping = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, sleeper.id);
        return details.run?.status === 'sleeping' ? details : undefined;
      },
      (value) => value.run?.status === 'sleeping',
      20000
    );

    const sleepStep = sleeping.steps.find(
      (step) => step.step_key === 'wait-for-approval'
    );
    expect(sleepStep?.status).toBe('completed');
    expect(sleepStep?.wake_at).not.toBeNull();

    const readyTask = await fixture.pool.query<{ run_at: Date }>(
      `SELECT run_at FROM tasks WHERE run_id = $1 AND status = 'ready'`,
      [sleeper.id]
    );
    expect(readyTask.rowCount).toBe(1);
    expect(readyTask.rows[0]?.run_at.getTime()).toBe(
      sleepStep?.wake_at?.getTime() ?? 0
    );

    const fastStatus = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, fast.id);
        return details.run?.status === 'completed'
          ? details.run.status
          : undefined;
      },
      (value) => value === 'completed',
      30000
    );
    expect(fastStatus).toBe('completed');

    const sleepStartedAt = Date.now();
    await killWorkerProcess(worker);
    expect(worker.exited()).toBe(true);

    track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'sleep-2',
        env: {
          DURABLY_WORKER_CONCURRENCY: '1',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '1000',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    const completed = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, sleeper.id);
        return details.run?.status === 'completed' ? details : undefined;
      },
      (value) => value.run?.status === 'completed',
      30000
    );

    expect(completed.run?.completed_at).not.toBeNull();
    const wokeAt = completed.run?.completed_at?.getTime() ?? 0;
    const wakeAt = sleepStep?.wake_at?.getTime() ?? 0;
    expect(wokeAt).toBeGreaterThanOrEqual(wakeAt);
    expect(wokeAt - wakeAt).toBeLessThan(3000);
    expect(wokeAt - sleepStartedAt).toBeGreaterThan(0);

    const preservedWake = await fixture.pool.query<{ wake_at: Date }>(
      `SELECT wake_at FROM steps
       WHERE run_id = $1 AND step_key = 'wait-for-approval'`,
      [sleeper.id]
    );
    expect(preservedWake.rows[0]?.wake_at.getTime()).toBe(wakeAt);

    const queueStep = completed.steps.find(
      (step) => step.step_key === 'queue-request'
    );
    expect(queueStep?.attempts).toBe(1);
    expect(queueStep?.wake_at).toBeNull();
  }, 120000);

  test('a sleeping run does not consume worker concurrency', async () => {
    const workers = ['conc-1', 'conc-2'].map((workerId) =>
      track(
        spawnWorkerProcess({
          databaseUrl: fixture.databaseUrl,
          workerId,
          env: {
            DURABLY_WORKER_CONCURRENCY: '1',
            DURABLY_WORKER_POLL_INTERVAL_MS: '50',
            DURABLY_WORKER_LEASE_MS: '1000',
            DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
            DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
          }
        })
      )
    );

    const sleepers = await Promise.all(
      [0, 1, 2, 3, 4].map((index) =>
        createRun(fixture.pool, {
          workflow: 'delayed-approval',
          input: { email: `sleep-${index}@example.com`, delay: '20s' },
          idempotencyKey: `sleep-concurrency-${index}`
        })
      )
    );

    await waitForCondition(
      async () => {
        const result = await fixture.pool.query<{ count: number }>(
          `SELECT count(*)::int AS count FROM runs WHERE status = 'sleeping'`
        );
        return (result.rows[0]?.count ?? 0) >= 5
          ? result.rows[0]?.count
          : undefined;
      },
      (value) => value >= 5,
      30000
    );

    const leasedDuringSleep = await fixture.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM tasks WHERE status = 'leased'`
    );
    expect(leasedDuringSleep.rows[0]?.count).toBe(0);

    const follower = await createRun(fixture.pool, {
      workflow: 'onboard-user',
      input: { email: 'after-sleep@example.com', name: 'After', plan: 'pro' },
      idempotencyKey: 'sleep-concurrency-follower'
    });

    const status = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, follower.id);
        return details.run?.status === 'completed'
          ? details.run.status
          : undefined;
      },
      (value) => value === 'completed',
      30000
    );
    expect(status).toBe('completed');

    const stillSleeping = await fixture.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM runs WHERE status = 'sleeping'`
    );
    expect(stillSleeping.rows[0]?.count).toBe(5);

    const sleepersLeased = await fixture.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM tasks
         WHERE status = 'leased' AND run_id = ANY($1::uuid[])`,
      [sleepers.map((run) => run.id)]
    );
    expect(sleepersLeased.rows[0]?.count).toBe(0);
    expect(workers).toHaveLength(2);
  }, 120000);

  test('a sleep step whose wake time has passed returns immediately on replay', async () => {
    const run = await createRun(fixture.pool, {
      workflow: 'delayed-approval',
      input: { email: 'past@example.com', delay: '1s' },
      idempotencyKey: 'sleep-replay'
    });

    const claimed = await claimTasks(fixture.pool, {
      workerId: 'replay-worker',
      limit: 1,
      leaseMs: 1000
    });
    const task = claimed.tasks[0];
    if (!task) {
      throw new Error('expected a claimed task');
    }

    const pastWake = new Date(Date.now() - 60_000);
    await fixture.pool.query(
      `INSERT INTO steps (
         run_id, step_key, status, output, attempts, last_error, started_at, finished_at, wake_at
       ) VALUES ($1, 'queue-request', 'completed', $2, 1, NULL, now(), now(), NULL)`,
      [run.id, { email: 'past@example.com', queued: true }]
    );
    await fixture.pool.query(
      `INSERT INTO steps (
         run_id, step_key, status, output, attempts, last_error, started_at, finished_at, wake_at
       ) VALUES ($1, 'wait-for-approval', 'completed', $2, 1, NULL, now(), now(), $3)`,
      [run.id, { wake_at: pastWake.toISOString() }, pastWake]
    );
    await fixture.pool.query(`UPDATE tasks SET run_at = $2 WHERE id = $1`, [
      task.id,
      pastWake
    ]);

    const worker = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'replay-1',
        env: {
          DURABLY_WORKER_CONCURRENCY: '1',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '1000',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    const status = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, run.id);
        return details.run?.status === 'completed' ? details : undefined;
      },
      (value) => value.run?.status === 'completed',
      30000
    );

    expect(status.steps.map((step) => step.step_key)).toEqual([
      'queue-request',
      'wait-for-approval',
      'record-decision'
    ]);
    const sleepStep = status.steps.find(
      (step) => step.step_key === 'wait-for-approval'
    );
    expect(sleepStep?.wake_at?.toISOString()).toBe(pastWake.toISOString());
    expect(sleepStep?.attempts).toBe(1);
  });

  test('sleepUntil wakes the run at the requested instant', async () => {
    const wakeAt = new Date(Date.now() + 2500);
    const run = await createRun(fixture.pool, {
      workflow: 'delayed-approval',
      input: {
        email: 'until@example.com',
        delay: '60s',
        wakeAt: wakeAt.toISOString()
      },
      idempotencyKey: 'sleep-until'
    });

    const worker = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'until-1',
        env: {
          DURABLY_WORKER_CONCURRENCY: '2',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '1000',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    const sleeping = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, run.id);
        return details.run?.status === 'sleeping' ? details : undefined;
      },
      (value) => value.run?.status === 'sleeping',
      20000
    );
    const sleepStep = sleeping.steps.find(
      (step) => step.step_key === 'wait-for-approval'
    );
    expect(sleepStep?.wake_at?.toISOString()).toBe(wakeAt.toISOString());

    const status = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(fixture.pool, run.id);
        return details.run?.status === 'completed'
          ? details.run.status
          : undefined;
      },
      (value) => value === 'completed',
      30000
    );
    expect(status).toBe('completed');
    expect(worker.exited()).toBe(false);
  });

  test('a sleep step wake time is decided once and never extended by a retry', async () => {
    const run = await createRun(fixture.pool, {
      workflow: 'delayed-approval',
      input: { email: 'stable@example.com', delay: '10s' },
      idempotencyKey: 'sleep-stable'
    });

    const worker = track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'stable-1',
        env: {
          DURABLY_WORKER_CONCURRENCY: '1',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '500',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    const firstWake = await waitForCondition(
      async () => {
        const result = await fixture.pool.query<{ wake_at: Date }>(
          `SELECT wake_at FROM steps
           WHERE run_id = $1 AND step_key = 'wait-for-approval'`,
          [run.id]
        );
        return result.rows[0]?.wake_at ?? undefined;
      },
      (value) => value instanceof Date,
      20000
    );

    await killWorkerProcess(worker);

    track(
      spawnWorkerProcess({
        databaseUrl: fixture.databaseUrl,
        workerId: 'stable-2',
        env: {
          DURABLY_WORKER_CONCURRENCY: '1',
          DURABLY_WORKER_POLL_INTERVAL_MS: '50',
          DURABLY_WORKER_LEASE_MS: '500',
          DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
          DURABLY_WORKER_REAPER_INTERVAL_MS: '200'
        }
      })
    );

    const forcedRewake = new Date(firstWake.getTime() + 5000);
    await fixture.pool.query(
      `UPDATE tasks SET status = 'ready', run_at = now(), locked_by = NULL,
              lease_token = NULL, lease_expires_at = NULL
       WHERE run_id = $1`,
      [run.id]
    );

    const attemptsAfter = await waitForCondition(
      async () => {
        const result = await fixture.pool.query<{ attempts: number }>(
          `SELECT attempts FROM steps
           WHERE run_id = $1 AND step_key = 'record-decision'`,
          [run.id]
        );
        return result.rows[0]?.attempts !== undefined
          ? result.rows[0]?.attempts
          : undefined;
      },
      () => true,
      20000
    );
    expect(attemptsAfter).toBe(1);

    const finalWake = await fixture.pool.query<{ wake_at: Date }>(
      `SELECT wake_at FROM steps
       WHERE run_id = $1 AND step_key = 'wait-for-approval'`,
      [run.id]
    );
    expect(finalWake.rows[0]?.wake_at.getTime()).toBe(firstWake.getTime());
    expect(forcedRewake.getTime()).toBeGreaterThan(firstWake.getTime());
  });
});

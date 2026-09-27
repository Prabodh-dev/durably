import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';

import { GenericContainer, Wait } from 'testcontainers';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';

import {
  claimTasks,
  completeRunAndTask,
  computeBackoffDelayMs,
  createDatabasePool,
  createRetryPolicy,
  createRun,
  failRunAndDeadLetter,
  getRunWithSteps,
  listDeadLetters,
  reapExpiredTasks,
  recordStepFailure,
  recordStepSuccess,
  releaseWorkerLeases,
  replayDeadLetter,
  rescheduleRunAndTask,
  runMigrations
} from '@durably/core';

import type { Pool } from 'pg';

const migrationsDir = resolve(process.cwd(), 'migrations');
const workerMain = resolve(process.cwd(), 'packages/worker/dist/main.js');

type TestContext = {
  container: Awaited<ReturnType<typeof startPostgresContainer>>;
  pool: Pool;
  databaseUrl: string;
};

async function startPostgresContainer() {
  const container = await new GenericContainer('postgres:16-alpine')
    .withEnvironment({
      POSTGRES_DB: 'durably',
      POSTGRES_USER: 'durably',
      POSTGRES_PASSWORD: 'durably'
    })
    .withExposedPorts(5432)
    .withWaitStrategy(
      Wait.forLogMessage('database system is ready to accept connections')
    )
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(5432);
  const databaseUrl = `postgres://durably:durably@${host}:${port}/durably`;
  return { container, databaseUrl };
}

async function waitForCondition<T>(
  callback: () => Promise<T | undefined | null>,
  predicate: (value: T) => boolean,
  timeoutMs = 30000,
  intervalMs = 100
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await callback();
    if (value !== undefined && value !== null && predicate(value)) {
      return value;
    }
    await sleep(intervalMs);
  }
  throw new Error('condition timed out');
}

async function waitForProcessExit(
  child: ChildProcessWithoutNullStreams,
  timeoutMs = 10000
): Promise<number | null> {
  return await new Promise<number | null>((resolvePromise, rejectPromise) => {
    const timeout = setTimeout(() => {
      rejectPromise(new Error('process did not exit in time'));
    }, timeoutMs);

    child.once('exit', (code) => {
      clearTimeout(timeout);
      resolvePromise(code);
    });
  });
}

function spawnWorker(
  databaseUrl: string,
  failureRate = '0'
): ChildProcessWithoutNullStreams {
  if (!existsSync(workerMain)) {
    throw new Error(`worker build output not found at ${workerMain}`);
  }

  return spawn(process.execPath, [workerMain], {
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      DURABLY_WORKER_CONCURRENCY: '1',
      DURABLY_WORKER_LEASE_MS: '500',
      ONBOARD_USER_FAILURE_RATE: failureRate
    },
    stdio: ['ignore', 'ignore', 'ignore']
  });
}

async function resetDatabase(pool: Pool): Promise<void> {
  await pool.query(
    `TRUNCATE TABLE dead_letters, example_side_effects, steps, tasks, runs RESTART IDENTITY CASCADE`
  );
}

async function seedTasks(pool: Pool, count: number): Promise<string> {
  const runId = randomUUID();
  await pool.query(
    `INSERT INTO runs (id, tenant_id, workflow, input, status, idempotency_key, created_at, updated_at)
     VALUES ($1, 'default', 'bulk-claim', '{}'::jsonb, 'pending', NULL, now(), now())`,
    [runId]
  );

  const values: string[] = [];
  const params: Array<string> = [];
  for (let index = 0; index < count; index += 1) {
    params.push(randomUUID(), runId);
    values.push(
      `($${params.length - 1}, $${params.length}, 'default', now(), 'ready', 0, 10)`
    );
  }

  await pool.query(
    `INSERT INTO tasks (id, run_id, tenant_id, run_at, status, attempts, max_attempts)
     VALUES ${values.join(', ')}`,
    params
  );
  return runId;
}

let context: TestContext;

async function initializeDatabase(pool: Pool): Promise<void> {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await pool.query('SELECT 1');
      await runMigrations(pool, migrationsDir);
      return;
    } catch (error) {
      if (attempt === 4) {
        throw error;
      }
      await sleep(1000);
    }
  }
}

beforeAll(async () => {
  const started = await startPostgresContainer();
  const pool = await createDatabasePool(started.databaseUrl);
  await initializeDatabase(pool);
  context = {
    container: started.container,
    pool,
    databaseUrl: started.databaseUrl
  };
});

beforeEach(async () => {
  await resetDatabase(context.pool);
});

afterAll(async () => {
  if (context) {
    await context.pool.end();
    await context.container.stop();
  }
});

describe.sequential('durably core', () => {
  test('8 concurrent claimers over 2000 tasks claim each task exactly once', async () => {
    await seedTasks(context.pool, 2000);

    const claimedIds = new Set<string>();
    const claimers = Array.from({ length: 8 }, async (_, workerIndex) => {
      for (;;) {
        const claimed = await claimTasks(context.pool, {
          workerId: `claimer-${workerIndex}`,
          limit: 1,
          leaseMs: 1000
        });

        if (claimed.tasks.length === 0) {
          return;
        }

        for (const task of claimed.tasks) {
          expect(claimedIds.has(task.id)).toBe(false);
          claimedIds.add(task.id);
        }
      }
    });

    await Promise.all(claimers);
    expect(claimedIds.size).toBe(2000);
  });

  test('idempotent run creation returns the same run for the same key', async () => {
    const first = await createRun(context.pool, {
      workflow: 'onboard-user',
      input: { email: 'same@example.com' },
      idempotencyKey: 'idempotent-key'
    });

    const second = await createRun(context.pool, {
      workflow: 'onboard-user',
      input: { email: 'same@example.com' },
      idempotencyKey: 'idempotent-key'
    });

    expect(second.id).toBe(first.id);
    expect(second.workflow).toBe('onboard-user');
  });

  test('lease expiry lets another worker pick up a task after the leased worker is killed', async () => {
    const run = await createRun(context.pool, {
      workflow: 'onboard-user',
      input: {
        email: 'lease@example.com',
        name: 'Lease Test',
        plan: 'pro'
      },
      idempotencyKey: 'lease-expiry'
    });

    const worker = spawnWorker(context.databaseUrl, '0');
    await waitForCondition(
      async () => {
        const steps = await getRunWithSteps(context.pool, run.id);
        return steps.steps.length >= 1 ? steps.steps.length : undefined;
      },
      (value) => value >= 1
    );

    worker.kill('SIGKILL');
    await waitForProcessExit(worker);

    const secondWorker = spawnWorker(context.databaseUrl, '0');
    const completedRun = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(context.pool, run.id);
        return details.run?.status === 'completed' ? details.run : undefined;
      },
      (value) => value.status === 'completed',
      30000
    );

    expect(completedRun.status).toBe('completed');
    const sideEffects = await context.pool.query(
      'SELECT count(*)::int AS count FROM example_side_effects WHERE idempotency_key = $1',
      [`${run.id}:create-profile`]
    );
    expect(sideEffects.rows[0].count).toBe(1);

    secondWorker.kill('SIGKILL');
    await waitForProcessExit(secondWorker);
  });

  test('crash resume keeps the first two steps single-executed and completes the run', async () => {
    const run = await createRun(context.pool, {
      workflow: 'onboard-user',
      input: {
        email: 'crash@example.com',
        name: 'Crash Test',
        plan: 'pro'
      },
      idempotencyKey: 'crash-resume'
    });

    const worker = spawnWorker(context.databaseUrl, '0');
    await waitForCondition(
      async () => {
        const details = await getRunWithSteps(context.pool, run.id);
        const stepKeys = new Set(details.steps.map((step) => step.step_key));
        return stepKeys.has('create-profile') && stepKeys.has('enrich-profile')
          ? details.steps.length
          : undefined;
      },
      (value) => value >= 2
    );

    worker.kill('SIGKILL');
    await waitForProcessExit(worker);

    const replacementWorker = spawnWorker(context.databaseUrl, '0');
    const completedRun = await waitForCondition(
      async () => {
        const details = await getRunWithSteps(context.pool, run.id);
        return details.run?.status === 'completed' ? details.run : undefined;
      },
      (value) => value.status === 'completed',
      30000
    );

    expect(completedRun.status).toBe('completed');

    const steps = await getRunWithSteps(context.pool, run.id);
    const stepCounts = new Map(
      steps.steps.map((step) => [step.step_key, step.attempts])
    );
    expect(stepCounts.get('create-profile')).toBe(1);
    expect(stepCounts.get('enrich-profile')).toBe(1);

    const sideEffects = await context.pool.query(
      'SELECT count(*)::int AS count FROM example_side_effects WHERE idempotency_key = $1',
      [`${run.id}:create-profile`]
    );
    expect(sideEffects.rows[0].count).toBe(1);

    replacementWorker.kill('SIGKILL');
    await waitForProcessExit(replacementWorker);
  });

  test('retry bounds, dead letters, and replay work together', async () => {
    const policy = createRetryPolicy({
      maxAttempts: 3,
      baseDelayMs: 200,
      maxDelayMs: 1500
    });
    const delay = computeBackoffDelayMs(2, policy, () => 0.5);
    expect(delay).toBeGreaterThanOrEqual(0);
    expect(delay).toBeLessThanOrEqual(1500);

    const run = await createRun(context.pool, {
      workflow: 'onboard-user',
      input: { email: 'retry@example.com', name: 'Retry Test', plan: 'pro' },
      idempotencyKey: 'retry-run'
    });

    const claimed = await claimTasks(context.pool, {
      workerId: 'retry-worker',
      limit: 1,
      leaseMs: 1000
    });
    const task = claimed.tasks[0];
    expect(task).toBeDefined();
    if (!task) {
      throw new Error('expected a task to claim');
    }

    await context.pool.query(
      'UPDATE tasks SET attempts = max_attempts WHERE id = $1',
      [task.id]
    );
    const didDeadLetter = await failRunAndDeadLetter(
      context.pool,
      task,
      task.lease_token ?? '',
      'task_exhausted',
      { message: 'boom' },
      task
    );
    expect(didDeadLetter).toBe(true);

    const deadLetters = await listDeadLetters(context.pool, {
      tenantId: 'default'
    });
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0].run_id).toBe(run.id);

    const replayed = await replayDeadLetter(context.pool, deadLetters[0].id);
    expect(replayed).toBe(true);

    const afterReplay = await getRunWithSteps(context.pool, run.id);
    expect(afterReplay.run?.status).toBe('pending');
    const replayTasks = await context.pool.query(
      'SELECT count(*)::int AS count FROM tasks WHERE run_id = $1 AND status = $2',
      [run.id, 'ready']
    );
    expect(replayTasks.rows[0].count).toBe(1);
  });

  test('a stale worker token cannot complete a reassigned task', async () => {
    await createRun(context.pool, {
      workflow: 'onboard-user',
      input: { email: 'fence@example.com', name: 'Fence Test', plan: 'pro' },
      idempotencyKey: 'fence-run'
    });

    const claimed = await claimTasks(context.pool, {
      workerId: 'worker-a',
      limit: 1,
      leaseMs: 1000
    });
    const task = claimed.tasks[0];
    expect(task).toBeDefined();
    if (!task || !task.lease_token) {
      throw new Error('expected leased task');
    }

    const reassignedToken = randomUUID();
    await context.pool.query(
      `UPDATE tasks
       SET lease_token = $2, locked_by = 'worker-b'
       WHERE id = $1`,
      [task.id, reassignedToken]
    );

    const staleCompletion = await completeRunAndTask(
      context.pool,
      task,
      task.lease_token,
      { accepted: true }
    );
    expect(staleCompletion).toBe(false);

    const status = await getRunWithSteps(context.pool, task.run_id);
    expect(status.run?.status).not.toBe('completed');
  });

  test('a task held by an expired lease is reaped back to ready', async () => {
    const run = await createRun(context.pool, {
      workflow: 'onboard-user',
      input: { email: 'reap@example.com', name: 'Reap Test', plan: 'pro' },
      idempotencyKey: 'reap-run'
    });

    const leaseToken = randomUUID();
    const leasedTaskResult = await context.pool.query(
      `UPDATE tasks
       SET status = 'leased',
           locked_by = 'worker-reap',
           lease_token = $2,
           lease_expires_at = now() + interval '1 millisecond'
       WHERE run_id = $1
       RETURNING *`,
      [run.id, leaseToken]
    );
    const task = leasedTaskResult.rows[0];
    expect(task).toBeDefined();
    if (!task) {
      throw new Error('expected leased task');
    }

    await sleep(10);
    const reaped = await reapExpiredTasks(
      context.pool,
      1,
      new Date(Date.now() + 1000)
    );
    expect(reaped).toBe(1);

    const readyTasks = await context.pool.query(
      'SELECT count(*)::int AS count FROM tasks WHERE run_id = $1 AND status = $2',
      [run.id, 'ready']
    );
    expect(readyTasks.rows[0].count).toBe(1);
  });
});

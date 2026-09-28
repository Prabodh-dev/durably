import {
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';

import {
  createLeaderDuties,
  createLeaderElector,
  createMetrics,
  createRun,
  repairStuckRuns
} from '@durably/core';
import type { LeaderHandle } from '@durably/core';

import {
  createSilentLogger,
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
const electors: LeaderHandle[] = [];

function track(worker: WorkerProcess): WorkerProcess {
  workerProcesses.push(worker);
  return worker;
}

function currentLeaderFromLogs(workers: WorkerProcess[]): WorkerProcess | null {
  const timeline = workers
    .flatMap((worker) => worker.leaderEvents)
    .sort((a, b) => a.at - b.at);
  const holders = new Set<string>();
  for (const entry of timeline) {
    if (entry.event === 'leader_acquired') {
      holders.add(entry.workerId);
    } else {
      holders.delete(entry.workerId);
    }
  }
  const [leaderId] = [...holders];
  if (leaderId === undefined) {
    return null;
  }
  return workers.find((worker) => worker.workerId === leaderId) ?? null;
}

function acquisitionCount(workers: WorkerProcess[]): number {
  return workers.reduce(
    (total, worker) =>
      total +
      worker.leaderEvents.filter((event) => event.event === 'leader_acquired')
        .length,
    0
  );
}

beforeAll(async () => {
  fixture = await startPostgres();
});

beforeEach(async () => {
  await truncateAll(fixture.pool);
});

afterEach(async () => {
  for (const elector of electors.splice(0)) {
    await elector.stop();
  }
  for (const worker of workerProcesses.splice(0)) {
    await stopWorkerProcess(worker);
  }
});

describe.sequential('leader election', () => {
  test('three electors in one process contend and exactly one holds the lock', async () => {
    const metrics = createMetrics();
    const leaders = ['a', 'b', 'c'].map((workerId) => {
      const elector = createLeaderElector({
        databaseUrl: fixture.databaseUrl,
        workerId,
        intervalMs: 100,
        logger: createSilentLogger({ worker_id: workerId }),
        metrics
      });
      electors.push(elector);
      return elector;
    });

    for (const elector of leaders) {
      await elector.start();
    }

    await waitForCondition(
      () => {
        const count = leaders.filter((leader) => leader.isLeader()).length;
        return count > 0 ? count : undefined;
      },
      (count) => count === 1,
      15000
    );

    const holder = leaders.find((leader) => leader.isLeader());
    if (!holder) {
      throw new Error('expected a leader');
    }
    const backendPid = holder.leaderBackendPid();
    expect(backendPid).not.toBeNull();

    const lockHolders = await fixture.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM pg_locks WHERE locktype = 'advisory' AND granted`
    );
    expect(lockHolders.rows[0]?.count).toBe(1);
  });

  test('leadership moves to a survivor when the leader connection is terminated', async () => {
    const metrics = createMetrics();
    const sweeps = new Map<string, number>();
    const leaders = ['a', 'b', 'c'].map((workerId) => {
      const elector = createLeaderElector({
        databaseUrl: fixture.databaseUrl,
        workerId,
        intervalMs: 100,
        logger: createSilentLogger({ worker_id: workerId }),
        metrics
      });
      electors.push(elector);
      return { workerId, elector };
    });

    const dutyHandles = leaders.map(({ workerId, elector }) => {
      const duties = createLeaderDuties({
        pool: fixture.pool,
        workerId,
        logger: createSilentLogger({ worker_id: workerId }),
        metrics,
        reaperIntervalMs: 100,
        stuckRunIntervalMs: 100,
        stuckRunGraceMs: 1000,
        onSweep: (kind) => {
          if (kind !== 'lease_reaper') {
            return;
          }
          const key = `${workerId}`;
          sweeps.set(key, (sweeps.get(key) ?? 0) + 1);
        }
      });
      elector.onChange((isLeader) => {
        if (isLeader) {
          duties.start();
          return;
        }
        void duties.stop();
      });
      return duties;
    });

    for (const { elector } of leaders) {
      await elector.start();
    }

    await waitForCondition(
      () => {
        const count = leaders.filter((entry) =>
          entry.elector.isLeader()
        ).length;
        return count > 0 ? count : undefined;
      },
      (count) => count === 1,
      15000
    );

    const holder = leaders.find((entry) => entry.elector.isLeader());
    if (!holder) {
      throw new Error('expected a leader');
    }
    const backendPid = holder.elector.leaderBackendPid();
    if (backendPid === null) {
      throw new Error('expected a backend pid for the leader');
    }

    await waitForCondition(
      () => ((sweeps.get(holder.workerId) ?? 0) > 0 ? sweeps.size : undefined),
      (size) => true,
      10000
    );

    for (const entry of leaders) {
      if (entry.workerId === holder.workerId) {
        expect(sweeps.get(entry.workerId) ?? 0).toBeGreaterThan(0);
        continue;
      }
      expect(sweeps.get(entry.workerId) ?? 0).toBe(0);
    }

    await fixture.pool.query('SELECT pg_terminate_backend($1)', [backendPid]);

    const survivors = leaders.filter((entry) => entry !== holder);
    const takeover = await waitForCondition(
      () => {
        const count = survivors.filter((entry) =>
          entry.elector.isLeader()
        ).length;
        const next = survivors.find((entry) => entry.elector.isLeader());
        return next ? { workerId: next.workerId, at: Date.now() } : undefined;
      },
      (value) => value.workerId.length > 0,
      15000,
      25
    );

    expect(holder.elector.isLeader()).toBe(false);
    expect(takeover.workerId).not.toBe(holder.workerId);
    expect(survivors.filter((entry) => entry.elector.isLeader()).length).toBe(
      1
    );

    await waitForCondition(
      () => ((sweeps.get(takeover.workerId) ?? 0) > 0 ? true : undefined),
      (value) => value,
      10000
    );

    const oldLeaderSweepsAtTakeover = sweeps.get(holder.workerId) ?? 0;
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(sweeps.get(holder.workerId) ?? 0).toBe(oldLeaderSweepsAtTakeover);

    for (const handle of dutyHandles) {
      await handle.stop();
    }
  });

  test('three worker processes elect one leader and a survivor takes over after a real kill', async () => {
    const workers = ['w1', 'w2', 'w3'].map((workerId) =>
      track(
        spawnWorkerProcess({
          databaseUrl: fixture.databaseUrl,
          workerId,
          env: {
            DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
            DURABLY_WORKER_REAPER_INTERVAL_MS: '200',
            DURABLY_WORKER_POLL_INTERVAL_MS: '100',
            DURABLY_WORKER_LEASE_MS: '1000',
            DURABLY_WORKER_CONCURRENCY: '2'
          }
        })
      )
    );

    await waitForCondition(
      () => (currentLeaderFromLogs(workers) ? 1 : undefined),
      (value) => value === 1,
      30000
    );

    await waitForCondition(
      () =>
        acquisitionCount(workers) >= 1 ? acquisitionCount(workers) : undefined,
      (count) => count === 1,
      2000,
      200
    );

    const leader = currentLeaderFromLogs(workers);
    if (!leader) {
      throw new Error('expected a leader');
    }

    const survivors = workers.filter(
      (worker) => worker.workerId !== leader.workerId
    );
    const killedAt = Date.now();
    await killWorkerProcess(leader);

    await waitForCondition(
      () => {
        const next = currentLeaderFromLogs(survivors);
        return next ? { workerId: next.workerId, at: Date.now() } : undefined;
      },
      (value) => value.workerId.length > 0,
      15000,
      25
    );

    expect(Date.now() - killedAt).toBeLessThan(10000);
    expect(acquisitionCount(workers)).toBe(2);
    expect(acquisitionCount(survivors)).toBe(1);
  }, 90000);

  test('the lease reaper reclaims a lease left behind by a killed process', async () => {
    const workers = ['r1', 'r2'].map((workerId) =>
      track(
        spawnWorkerProcess({
          databaseUrl: fixture.databaseUrl,
          workerId,
          env: {
            DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
            DURABLY_WORKER_REAPER_INTERVAL_MS: '200',
            DURABLY_WORKER_POLL_INTERVAL_MS: '100',
            DURABLY_WORKER_LEASE_MS: '1000',
            DURABLY_WORKER_CONCURRENCY: '1'
          }
        })
      )
    );

    await waitForCondition(
      () => (currentLeaderFromLogs(workers) ? 1 : undefined),
      (value) => value === 1,
      30000
    );

    const run = await createRun(fixture.pool, {
      workflow: 'onboard-user',
      input: { email: 'reaper@example.com', name: 'Reaper', plan: 'pro' },
      idempotencyKey: 'leader-reaper'
    });

    await fixture.pool.query(
      `UPDATE tasks
         SET status = 'leased',
             locked_by = 'ghost-worker',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second'
         WHERE run_id = $1`,
      [run.id]
    );

    const reclaimed = await waitForCondition(
      async () => {
        const result = await fixture.pool.query<{ count: number }>(
          `SELECT count(*)::int AS count FROM tasks
             WHERE run_id = $1 AND locked_by = 'ghost-worker'`,
          [run.id]
        );
        return (result.rows[0]?.count ?? 0) === 0
          ? result.rows[0]?.count
          : undefined;
      },
      (value) => value === 0,
      20000
    );
    expect(reclaimed).toBe(0);

    const completed = await waitForCondition(
      async () => {
        const result = await fixture.pool.query<{ status: string }>(
          'SELECT status FROM runs WHERE id = $1',
          [run.id]
        );
        return result.rows[0]?.status === 'completed'
          ? result.rows[0]?.status
          : undefined;
      },
      (value) => value === 'completed',
      30000
    );
    expect(completed).toBe('completed');
  }, 90000);

  test('a run marked running with no ready or leased task is repaired into a resumable task', async () => {
    const run = await createRun(fixture.pool, {
      workflow: 'onboard-user',
      input: { email: 'stuck@example.com', name: 'Stuck', plan: 'pro' },
      idempotencyKey: 'stuck-run'
    });

    await fixture.pool.query(
      `UPDATE tasks SET status = 'done' WHERE run_id = $1 AND status = 'ready'`,
      [run.id]
    );
    await fixture.pool.query(
      `UPDATE runs
       SET status = 'running', updated_at = now() - interval '10 minutes'
       WHERE id = $1`,
      [run.id]
    );

    const repaired = await repairStuckRuns(
      fixture.pool,
      10,
      new Date(Date.now() - 30000)
    );
    expect(repaired).toEqual([run.id]);

    const runRow = await fixture.pool.query<{ status: string }>(
      'SELECT status FROM runs WHERE id = $1',
      [run.id]
    );
    expect(runRow.rows[0]?.status).toBe('pending');

    const ready = await fixture.pool.query<{ count: number; priority: number }>(
      `SELECT count(*)::int AS count, max(priority)::int AS priority
       FROM tasks WHERE run_id = $1 AND status = 'ready'`,
      [run.id]
    );
    expect(ready.rows[0]?.count).toBe(1);
    expect(ready.rows[0]?.priority).toBe(0);
  });

  test('a run that still owns a ready task is not treated as stuck', async () => {
    const run = await createRun(fixture.pool, {
      workflow: 'onboard-user',
      input: { email: 'healthy@example.com', name: 'Healthy', plan: 'pro' },
      idempotencyKey: 'healthy-run'
    });

    await fixture.pool.query(
      `UPDATE runs
       SET status = 'running', updated_at = now() - interval '10 minutes'
       WHERE id = $1`,
      [run.id]
    );

    const repaired = await repairStuckRuns(
      fixture.pool,
      10,
      new Date(Date.now() - 30000)
    );
    expect(repaired).toEqual([]);

    const ready = await fixture.pool.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM tasks WHERE run_id = $1 AND status = 'ready'`,
      [run.id]
    );
    expect(ready.rows[0]?.count).toBe(1);
  });
});

import {
  afterAll,
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
  createSchedule,
  createTenant,
  getSchedule,
  latestOccurrenceAtOrBefore,
  listSchedules,
  scheduleFireKey,
  selectFireTime,
  tickSchedules,
  updateSchedule
} from '@durably/core';
import type { LeaderHandle, ScheduleRecord } from '@durably/core';

import {
  createSilentLogger,
  createTestClock,
  spawnWorkerProcess,
  startPostgres,
  stopPostgres,
  stopWorkerProcess,
  truncateAll,
  waitForCondition
} from './harness.js';
import type { PostgresFixture, WorkerProcess, TestClock } from './harness.js';

let fixture: PostgresFixture;
const workerProcesses: WorkerProcess[] = [];
const electors: LeaderHandle[] = [];

function track(worker: WorkerProcess): WorkerProcess {
  workerProcesses.push(worker);
  return worker;
}

async function backdateSchedule(
  scheduleId: string,
  createdAt: Date
): Promise<void> {
  await fixture.pool.query(
    `UPDATE schedules SET created_at = $2, last_fire_time = NULL WHERE id = $1`,
    [scheduleId, createdAt]
  );
}

async function runsForSchedule(scheduleId: string): Promise<string[]> {
  const result = await fixture.pool.query<{ idempotency_key: string }>(
    'SELECT idempotency_key FROM runs WHERE idempotency_key LIKE $1 ORDER BY idempotency_key',
    [`${scheduleId}:%`]
  );
  return result.rows.map((row) => row.idempotency_key ?? '');
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

afterAll(async () => {
  await stopPostgres(fixture);
});

describe.sequential('cron occurrence selection with an injected clock', () => {
  const from = new Date('2026-01-01T00:00:00.000Z');

  test('a UTC daily schedule fires at the same instant every day', () => {
    const occurrences = [
      latestOccurrenceAtOrBefore(
        '0 9 * * *',
        'UTC',
        new Date('2026-06-15T09:30:00.000Z')
      ),
      latestOccurrenceAtOrBefore(
        '0 9 * * *',
        'UTC',
        new Date('2026-06-16T09:00:00.000Z')
      )
    ];
    expect(occurrences.map((date) => date.toISOString())).toEqual([
      '2026-06-15T09:00:00.000Z',
      '2026-06-16T09:00:00.000Z'
    ]);
  });

  test('a zoned daily schedule keeps its local wall clock across a DST change', () => {
    const winter = latestOccurrenceAtOrBefore(
      '0 9 * * *',
      'America/New_York',
      new Date('2026-01-15T15:00:00.000Z')
    );
    const summer = latestOccurrenceAtOrBefore(
      '0 9 * * *',
      'America/New_York',
      new Date('2026-06-15T15:00:00.000Z')
    );

    expect(winter.toISOString()).toBe('2026-01-15T14:00:00.000Z');
    expect(summer.toISOString()).toBe('2026-06-15T13:00:00.000Z');
  });

  test('a half-hour offset zone is honoured', () => {
    const occurrence = latestOccurrenceAtOrBefore(
      '0 9 * * *',
      'Asia/Kolkata',
      new Date('2026-06-15T12:00:00.000Z')
    );
    expect(occurrence.toISOString()).toBe('2026-06-15T03:30:00.000Z');
  });

  test('a local time that does not exist on spring forward still fires once, shifted past the gap', () => {
    const selection = selectFireTime(
      '30 2 * * *',
      'America/New_York',
      new Date('2026-03-07T07:30:00.000Z'),
      new Date('2026-03-08T12:00:00.000Z'),
      'latest'
    );
    expect(selection.fireTime?.toISOString()).toBe('2026-03-08T07:30:00.000Z');
    expect(selection.missed).toBe(1);
  });

  test('a local time that repeats on fall back fires once, on the first pass', () => {
    const selection = selectFireTime(
      '30 1 * * *',
      'America/New_York',
      new Date('2026-10-31T05:30:00.000Z'),
      new Date('2026-11-01T12:00:00.000Z'),
      'latest'
    );
    expect(selection.fireTime?.toISOString()).toBe('2026-11-01T05:30:00.000Z');
    expect(selection.missed).toBe(1);
  });

  test('catch-up none skips a backlog and only fires when exactly one occurrence is due', () => {
    const to = new Date('2026-06-15T12:00:00.000Z');

    const backlogged = selectFireTime(
      '0 * * * *',
      'UTC',
      new Date('2026-06-15T09:05:00.000Z'),
      to,
      'none'
    );
    expect(backlogged.fireTime).toBeNull();
    expect(backlogged.missed).toBe(3);

    const single = selectFireTime(
      '0 * * * *',
      'UTC',
      new Date('2026-06-15T11:05:00.000Z'),
      to,
      'none'
    );
    expect(single.fireTime?.toISOString()).toBe('2026-06-15T12:00:00.000Z');
    expect(single.missed).toBe(1);
  });

  test('catch-up latest fires once on the newest missed occurrence', () => {
    const selection = selectFireTime(
      '0 * * * *',
      'UTC',
      from,
      new Date('2026-06-15T12:00:00.000Z'),
      'latest'
    );
    expect(selection.fireTime?.toISOString()).toBe('2026-06-15T12:00:00.000Z');
  });

  test('a schedule created after its cron window has no due occurrence', () => {
    const selection = selectFireTime(
      '0 9 * * *',
      'UTC',
      new Date('2026-06-15T08:00:00.000Z'),
      new Date('2026-06-15T08:59:59.000Z'),
      'latest'
    );
    expect(selection.fireTime).toBeNull();
    expect(selection.missed).toBe(0);
  });
});

describe.sequential('cron ticking against the database', () => {
  test('a tick creates exactly one run keyed by schedule id and fire time', async () => {
    const clock = createTestClock(new Date('2026-06-15T09:05:00.000Z'));
    const schedule = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 * * * *',
      input: { email: 'cron@example.com', name: 'Cron', plan: 'pro' },
      catchup: 'latest'
    });
    await backdateSchedule(schedule.id, clock.now());

    clock.set(new Date('2026-06-15T12:00:00.000Z'));
    const firstTick = await tickSchedules(fixture.pool, clock.now());
    expect(firstTick).toHaveLength(1);
    expect(firstTick[0]?.firedAt?.toISOString()).toBe(
      '2026-06-15T12:00:00.000Z'
    );

    const secondTick = await tickSchedules(fixture.pool, clock.now());
    expect(secondTick).toHaveLength(0);

    const keys = await runsForSchedule(schedule.id);
    expect(keys).toEqual([
      scheduleFireKey(schedule.id, new Date('2026-06-15T12:00:00.000Z'))
    ]);

    const stored = await getSchedule(fixture.pool, schedule.id);
    expect(stored?.last_fire_time?.toISOString()).toBe(
      '2026-06-15T12:00:00.000Z'
    );
  });

  test('a disabled schedule never fires and can be re-enabled through an update', async () => {
    const clock = createTestClock(new Date('2026-06-15T09:05:00.000Z'));
    const schedule = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 * * * *',
      input: { email: 'disabled@example.com', name: 'Off', plan: 'pro' },
      enabled: false,
      catchup: 'latest'
    });
    await backdateSchedule(schedule.id, clock.now());

    clock.set(new Date('2026-06-15T12:00:00.000Z'));
    expect(await tickSchedules(fixture.pool, clock.now())).toHaveLength(0);
    expect(await runsForSchedule(schedule.id)).toEqual([]);

    const enabled = await updateSchedule(fixture.pool, schedule.id, 'default', {
      enabled: true
    });
    expect(enabled?.enabled).toBe(true);

    const fired = await tickSchedules(fixture.pool, clock.now());
    expect(fired).toHaveLength(1);
    expect(fired[0]?.missed).toBe(3);
  });

  test('catch-up none records the missed window without starting a run', async () => {
    const clock = createTestClock(new Date('2026-06-15T09:05:00.000Z'));
    const schedule = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 * * * *',
      input: { email: 'norecatchup@example.com', name: 'None', plan: 'pro' },
      catchup: 'none'
    });
    await backdateSchedule(schedule.id, clock.now());

    clock.set(new Date('2026-06-15T12:00:00.000Z'));
    const skipped = await tickSchedules(fixture.pool, clock.now());
    expect(skipped).toHaveLength(1);
    expect(skipped[0]?.firedAt).toBeNull();
    expect(skipped[0]?.missed).toBe(3);
    expect(await runsForSchedule(schedule.id)).toEqual([]);

    const stored = await getSchedule(fixture.pool, schedule.id);
    expect(stored?.last_fire_time?.toISOString()).toBe(
      '2026-06-15T12:00:00.000Z'
    );

    clock.set(new Date('2026-06-15T13:00:00.000Z'));
    const fired = await tickSchedules(fixture.pool, clock.now());
    expect(fired).toHaveLength(1);
    expect(fired[0]?.firedAt?.toISOString()).toBe('2026-06-15T13:00:00.000Z');
  });

  test('a zoned schedule fires at the instant its timezone implies', async () => {
    const clock = createTestClock(new Date('2026-01-15T00:00:00.000Z'));
    const schedule = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 9 * * *',
      timezone: 'America/New_York',
      input: { email: 'ny@example.com', name: 'NY', plan: 'pro' },
      catchup: 'latest'
    });
    await backdateSchedule(schedule.id, clock.now());

    clock.set(new Date('2026-01-15T14:30:00.000Z'));
    const winter = await tickSchedules(fixture.pool, clock.now());
    expect(winter[0]?.firedAt?.toISOString()).toBe('2026-01-15T14:00:00.000Z');

    clock.set(new Date('2026-01-16T14:30:00.000Z'));
    const nextDay = await tickSchedules(fixture.pool, clock.now());
    expect(nextDay[0]?.firedAt?.toISOString()).toBe('2026-01-16T14:00:00.000Z');
  });

  test('an invalid cron expression or timezone is rejected at creation', async () => {
    await expect(
      createSchedule(fixture.pool, {
        workflow: 'onboard-user',
        cron: 'not a cron',
        input: {}
      })
    ).rejects.toThrow(/invalid cron expression/);

    await expect(
      createSchedule(fixture.pool, {
        workflow: 'onboard-user',
        cron: '0 9 * * *',
        timezone: 'Mars/Olympus',
        input: {}
      })
    ).rejects.toThrow(/invalid timezone/);
  });

  test('schedules are listed and scoped by tenant', async () => {
    await createTenant(fixture.pool, { id: 'tenant-b', name: 'Tenant B' });
    await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 9 * * *',
      input: { label: 'a' }
    });
    await createSchedule(fixture.pool, {
      tenantId: 'tenant-b',
      workflow: 'onboard-user',
      cron: '0 9 * * *',
      input: { label: 'b' }
    });

    const all = await listSchedules(fixture.pool);
    expect(all).toHaveLength(2);

    const scoped = await listSchedules(fixture.pool, { tenantId: 'tenant-b' });
    expect(scoped).toHaveLength(1);
    expect(scoped[0]?.tenant_id).toBe('tenant-b');

    const hidden = await getSchedule(
      fixture.pool,
      scoped[0]?.id ?? '',
      'default'
    );
    expect(hidden).toBeNull();
  });
});

describe.sequential('cron ticking across leader failover', () => {
  test('a fire already produced is never produced again by the next leader', async () => {
    const clock = createTestClock(new Date('2026-06-15T11:05:00.000Z'));
    const metrics = createMetrics();
    const schedule: ScheduleRecord = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '0 * * * *',
      input: { email: 'failover@example.com', name: 'Failover', plan: 'pro' },
      catchup: 'latest'
    });
    await backdateSchedule(schedule.id, clock.now());

    const leaders = ['c1', 'c2', 'c3'].map((workerId) => {
      const elector = createLeaderElector({
        databaseUrl: fixture.databaseUrl,
        workerId,
        intervalMs: 100,
        clock,
        logger: createSilentLogger({ worker_id: workerId }),
        metrics
      });
      electors.push(elector);
      const duties = createLeaderDuties({
        pool: fixture.pool,
        workerId,
        logger: createSilentLogger({ worker_id: workerId }),
        metrics,
        clock,
        reaperIntervalMs: 60_000,
        stuckRunIntervalMs: 60_000,
        cronIntervalMs: 60_000
      });
      return { workerId, elector, duties };
    });

    for (const entry of leaders) {
      await entry.elector.start();
    }

    const holder = await waitForCondition(
      () => leaders.find((entry) => entry.elector.isLeader()),
      (entry): boolean => entry !== undefined,
      15000,
      25
    );
    if (!holder) {
      throw new Error('expected a leader');
    }

    clock.set(new Date('2026-06-15T12:00:00.000Z'));
    await holder.duties.sweep('cron_ticker');

    const fireKey = scheduleFireKey(
      schedule.id,
      new Date('2026-06-15T12:00:00.000Z')
    );
    expect(await runsForSchedule(schedule.id)).toEqual([fireKey]);

    const backendPid = holder.elector.leaderBackendPid();
    if (backendPid === null) {
      throw new Error('expected a backend pid for the leader');
    }
    await fixture.pool.query('SELECT pg_terminate_backend($1)', [backendPid]);

    const successor = await waitForCondition(
      () => {
        const next = leaders.find(
          (entry) => entry !== holder && entry.elector.isLeader() === true
        );
        return next ? next : undefined;
      },
      (entry): boolean => entry !== undefined,
      15000,
      25
    );
    if (!successor) {
      throw new Error('expected a successor leader');
    }

    await successor.duties.sweep('cron_ticker');
    expect(await runsForSchedule(schedule.id)).toEqual([fireKey]);

    for (const entry of leaders) {
      await entry.duties.stop();
    }
  }, 60000);

  test('three worker processes produce one run per cron tick', async () => {
    const workers = ['cw1', 'cw2', 'cw3'].map((workerId) =>
      track(
        spawnWorkerProcess({
          databaseUrl: fixture.databaseUrl,
          workerId,
          env: {
            DURABLY_WORKER_LEADER_INTERVAL_MS: '200',
            DURABLY_WORKER_CRON_INTERVAL_MS: '200',
            DURABLY_WORKER_REAPER_INTERVAL_MS: '60',
            DURABLY_WORKER_POLL_INTERVAL_MS: '100',
            DURABLY_WORKER_LEASE_MS: '2000',
            DURABLY_WORKER_CONCURRENCY: '2'
          }
        })
      )
    );

    const schedule = await createSchedule(fixture.pool, {
      workflow: 'onboard-user',
      cron: '* * * * *',
      input: { email: 'crowd@example.com', name: 'Cron Crowd', plan: 'pro' },
      catchup: 'latest'
    });

    let keys: string[];
    try {
      keys = await waitForCondition(
        async () => {
          const found = await runsForSchedule(schedule.id);
          return found.length > 0 ? found : undefined;
        },
        (found) => found.length >= 2,
        240000,
        200
      );
    } catch (error) {
      throw new Error(
        `${String(error)}\n${workers.map((worker) => worker.recentOutput()).join('\n\n')}`
      );
    }

    const boundaryCounts = new Map<string, number>();
    for (const key of keys) {
      boundaryCounts.set(key, (boundaryCounts.get(key) ?? 0) + 1);
    }
    for (const count of boundaryCounts.values()) {
      expect(count).toBe(1);
    }

    const runs = await fixture.pool.query<{ idempotency_key: string }>(
      'SELECT idempotency_key FROM runs WHERE idempotency_key LIKE $1',
      [`${schedule.id}:%`]
    );
    expect(runs.rowCount).toBe(keys.length);
  }, 300000);
});

import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics
} from 'prom-client';
import type { Pool } from 'pg';

export const TASK_STATUSES = ['ready', 'leased', 'done'] as const;
export const DEAD_LETTER_REASONS = [
  'step_exhausted',
  'task_exhausted',
  'workflow_failed',
  'lease_lost'
] as const;

export type DurablyMetrics = {
  registry: Registry;
  leader: Gauge<'worker_id'>;
  reaperSweep: Gauge<'worker_id'>;
  claimLatency: Histogram<'worker_id'>;
  taskDuration: Histogram<'workflow' | 'outcome'>;
  stepAttempts: Counter<'workflow'>;
  stepRetries: Counter<'workflow'>;
  deadLetters: Counter<'reason'>;
  leaseExpirations: Counter;
  queueDepth: Gauge<'status'>;
  runningTasks: Gauge<'tenant_id'>;
  cronFires: Counter<'outcome'>;
};

export type CreateMetricsOptions = {
  pool?: Pool;
};

export function createMetrics(
  options: CreateMetricsOptions = {}
): DurablyMetrics {
  const pool = options.pool ?? null;
  const registry = new Registry();
  collectDefaultMetrics({ register: registry, prefix: 'durably_process_' });

  const queueDepth = new Gauge({
    name: 'durably_queue_depth',
    help: 'Tasks in the queue by status',
    labelNames: ['status'],
    registers: [registry],
    collect: async (): Promise<void> => {
      if (!pool) {
        return;
      }
      const result = await pool.query<{ status: string; count: number }>(
        'SELECT status, count(*)::int AS count FROM tasks GROUP BY status'
      );
      const seen = new Set<string>();
      for (const row of result.rows) {
        queueDepth.set({ status: row.status }, row.count);
        seen.add(row.status);
      }
      for (const status of TASK_STATUSES) {
        if (!seen.has(status)) {
          queueDepth.set({ status }, 0);
        }
      }
    }
  });

  const runningTasks = new Gauge({
    name: 'durably_running_tasks',
    help: 'Leased tasks per tenant',
    labelNames: ['tenant_id'],
    registers: [registry],
    collect: async (): Promise<void> => {
      if (!pool) {
        return;
      }
      const result = await pool.query<{ tenant_id: string; count: number }>(
        `SELECT task.tenant_id, count(*)::int AS count
         FROM tasks task
         JOIN tenants tenant ON tenant.id = task.tenant_id
         WHERE task.status = 'leased'
         GROUP BY task.tenant_id`
      );
      for (const row of result.rows) {
        runningTasks.set({ tenant_id: row.tenant_id }, row.count);
      }
    }
  });

  return {
    registry,
    leader: new Gauge({
      name: 'durably_leader',
      help: '1 when this process holds the durably leader advisory lock',
      labelNames: ['worker_id'],
      registers: [registry]
    }),
    reaperSweep: new Gauge({
      name: 'durably_leader_sweep_timestamp_seconds',
      help: 'Unix time of the last completed leader duty sweep',
      labelNames: ['worker_id'],
      registers: [registry]
    }),
    claimLatency: new Histogram({
      name: 'durably_claim_latency_seconds',
      help: 'Time a claim call took, from issuing the query to receiving rows',
      labelNames: ['worker_id'],
      buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
      registers: [registry]
    }),
    taskDuration: new Histogram({
      name: 'durably_task_duration_seconds',
      help: 'Wall clock time a worker spent on one task attempt',
      labelNames: ['workflow', 'outcome'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30],
      registers: [registry]
    }),
    stepAttempts: new Counter({
      name: 'durably_step_attempts_total',
      help: 'Step executions started, labelled by workflow',
      labelNames: ['workflow'],
      registers: [registry]
    }),
    stepRetries: new Counter({
      name: 'durably_step_retries_total',
      help: 'Step attempts that failed and were rescheduled',
      labelNames: ['workflow'],
      registers: [registry]
    }),
    deadLetters: new Counter({
      name: 'durably_dead_letters_total',
      help: 'Dead letters written, labelled by reason',
      labelNames: ['reason'],
      registers: [registry]
    }),
    leaseExpirations: new Counter({
      name: 'durably_lease_expirations_total',
      help: 'Leases that expired and were reclaimed by the leader reaper',
      registers: [registry]
    }),
    queueDepth,
    runningTasks,
    cronFires: new Counter({
      name: 'durably_cron_ticks_total',
      help: 'Schedule ticks processed by the leader, labelled by outcome',
      labelNames: ['outcome'],
      registers: [registry]
    })
  };
}

let singleton: DurablyMetrics | null = null;

export function getMetrics(): DurablyMetrics {
  if (!singleton) {
    singleton = createMetrics();
  }
  return singleton;
}

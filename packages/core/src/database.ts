import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';

import type {
  DeadLetterRecord,
  RunStatus,
  TaskRecord,
  WorkflowRunRecord,
  WorkflowStepRecord
} from './types.js';
import { createRetryPolicy, serializeError, systemClock } from './utils.js';
import { createLogger } from './logging.js';
import { DEFAULT_TENANT_ID, UnknownTenantError } from './tenancy.js';

export type Database = {
  pool: Pool;
};

export type CreateRunInput = {
  tenantId?: string;
  workflow: string;
  input: unknown;
  idempotencyKey?: string | null;
  priority?: number;
  taskMaxAttempts?: number;
};

export type RunFilter = {
  tenantId?: string;
  status?: RunStatus;
  workflow?: string;
  limit?: number;
  offset?: number;
};

export type DeadLetterFilter = {
  tenantId?: string;
  limit?: number;
  offset?: number;
};

export type ClaimTasksInput = {
  workerId: string;
  limit: number;
  leaseMs: number;
  now?: Date;
};

export type ClaimResult = {
  tasks: TaskRecord[];
};

export type TaskDecision =
  | { kind: 'success'; output: unknown }
  | { kind: 'retry'; error: unknown; delayMs: number }
  | { kind: 'dead-letter'; error: unknown; reason: string };

export type CreateDatabasePoolOptions = {
  statementTimeoutMs?: number;
};

const DEFAULT_STATEMENT_TIMEOUT_MS = 30000;

function defaultStatementTimeoutMs(): number {
  const configured = Number(process.env.DURABLY_STATEMENT_TIMEOUT_MS);
  return Number.isFinite(configured) && configured > 0
    ? configured
    : DEFAULT_STATEMENT_TIMEOUT_MS;
}

export async function createDatabasePool(
  databaseUrl: string,
  options?: CreateDatabasePoolOptions
): Promise<Pool> {
  const { Pool } = await import('pg');
  const statementTimeoutMs =
    options?.statementTimeoutMs ?? defaultStatementTimeoutMs();
  const pool = new Pool({
    connectionString: databaseUrl,
    // A database that accepts a query and never answers would otherwise hang a
    // worker forever, which is the one failure the reaper cannot rescue.
    options: `-c statement_timeout=${statementTimeoutMs}`
  });
  // pg emits 'error' on the pool when an idle connection drops. Without a
  // listener that emit is an unhandled error event and kills the process, so a
  // database blip would take down a worker instead of letting it reconnect.
  pool.on('error', (error) => {
    createLogger().error(
      { event: 'database_pool_error', error: serializeError(error) },
      'idle database connection failed'
    );
  });
  return pool;
}

async function withClient<T>(
  pool: Pool,
  callback: (..._args: [PoolClient]) => Promise<T>
): Promise<T> {
  const client = await pool.connect();
  // pg attaches no 'error' listener to a checked-out client, so a severed
  // connection surfaces as an unhandled error event and takes the process
  // down. The pending query already rejects with the same error, which is what
  // the caller needs to see.
  const swallowError = (): void => undefined;
  client.on('error', swallowError);
  try {
    return await callback(client);
  } finally {
    client.off('error', swallowError);
    client.release();
  }
}

export async function createRun(
  pool: Pool,
  input: CreateRunInput
): Promise<WorkflowRunRecord> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const run = await createRunInTransaction(client, input);
      await client.query('COMMIT');
      return run;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function createRunInTransaction(
  client: PoolClient,
  input: CreateRunInput
): Promise<WorkflowRunRecord> {
  const tenantId = input.tenantId ?? DEFAULT_TENANT_ID;
  const taskMaxAttempts = input.taskMaxAttempts ?? 10;
  const priority = input.priority ?? 0;

  const tenant = await client.query<{ id: string }>(
    'SELECT id FROM tenants WHERE id = $1 LIMIT 1',
    [tenantId]
  );
  if ((tenant.rowCount ?? 0) === 0) {
    throw new UnknownTenantError(tenantId);
  }

  if (input.idempotencyKey) {
    const existing = await client.query<WorkflowRunRecord>(
      'SELECT * FROM runs WHERE tenant_id = $1 AND idempotency_key = $2 LIMIT 1',
      [tenantId, input.idempotencyKey]
    );

    if ((existing.rowCount ?? 0) > 0) {
      return existing.rows[0] as WorkflowRunRecord;
    }
  }

  const runId = randomUUID();
  const runResult = await client.query<WorkflowRunRecord>(
    `INSERT INTO runs (
      id, tenant_id, workflow, input, status, idempotency_key, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, 'pending', $5, now(), now())
    ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    RETURNING *`,
    [runId, tenantId, input.workflow, input.input, input.idempotencyKey ?? null]
  );

  if ((runResult.rowCount ?? 0) === 0) {
    const existing = await client.query<WorkflowRunRecord>(
      'SELECT * FROM runs WHERE tenant_id = $1 AND idempotency_key = $2 LIMIT 1',
      [tenantId, input.idempotencyKey]
    );
    return existing.rows[0] as WorkflowRunRecord;
  }

  await client.query(
    `INSERT INTO tasks (
      id, run_id, tenant_id, run_at, status, priority, attempts, max_attempts
    ) VALUES ($1, $2, $3, now(), 'ready', $4, 0, $5)`,
    [randomUUID(), runId, tenantId, priority, taskMaxAttempts]
  );
  await client.query("NOTIFY task_ready, 'created'");
  return runResult.rows[0] as WorkflowRunRecord;
}

export async function getRun(
  pool: Pool,
  runId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<WorkflowRunRecord | null> {
  const result = await pool.query<WorkflowRunRecord>(
    'SELECT * FROM runs WHERE id = $1 AND tenant_id = $2 LIMIT 1',
    [runId, tenantId]
  );
  return result.rows[0] ?? null;
}

export async function getRunWithSteps(
  pool: Pool,
  runId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<{ run: WorkflowRunRecord | null; steps: WorkflowStepRecord[] }> {
  const runResult = await pool.query<WorkflowRunRecord>(
    'SELECT * FROM runs WHERE id = $1 AND tenant_id = $2 LIMIT 1',
    [runId, tenantId]
  );
  const stepsResult = await pool.query<WorkflowStepRecord>(
    'SELECT * FROM steps WHERE run_id = $1 ORDER BY started_at ASC',
    [runId]
  );
  return { run: runResult.rows[0] ?? null, steps: stepsResult.rows };
}

export async function listRuns(
  pool: Pool,
  filter: RunFilter = {}
): Promise<WorkflowRunRecord[]> {
  const clauses: string[] = [];
  const params: Array<string | number> = [];

  if (filter.tenantId) {
    params.push(filter.tenantId);
    clauses.push(`tenant_id = $${params.length}`);
  }
  if (filter.status) {
    params.push(filter.status);
    clauses.push(`status = $${params.length}`);
  }
  if (filter.workflow) {
    params.push(filter.workflow);
    clauses.push(`workflow = $${params.length}`);
  }

  const limit = filter.limit ?? 50;
  const offset = filter.offset ?? 0;
  params.push(limit, offset);
  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const result = await pool.query<WorkflowRunRecord>(
    `SELECT * FROM runs ${where} ORDER BY created_at DESC LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params
  );
  return result.rows;
}

export async function listDeadLetters(
  pool: Pool,
  filter: DeadLetterFilter = {}
): Promise<DeadLetterRecord[]> {
  const clauses: string[] = [];
  const params: Array<string | number> = [];

  if (filter.tenantId) {
    params.push(filter.tenantId);
    clauses.push(`tenant_id = $${params.length}`);
  }

  const limit = filter.limit ?? 50;
  const offset = filter.offset ?? 0;
  params.push(limit, offset);
  const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
  const result = await pool.query<DeadLetterRecord>(
    `SELECT * FROM dead_letters ${where} ORDER BY created_at DESC LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params
  );
  return result.rows;
}

export async function cancelRun(
  pool: Pool,
  runId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<WorkflowRunRecord | null> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const result = await client.query<WorkflowRunRecord>(
        `UPDATE runs
         SET status = 'cancelled', error = jsonb_build_object('reason', 'cancelled'), updated_at = now(), completed_at = now()
         WHERE id = $1 AND tenant_id = $2
         RETURNING *`,
        [runId, tenantId]
      );

      if ((result.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        return null;
      }

      await client.query(
        `UPDATE tasks
         SET status = 'done', last_error = jsonb_build_object('reason', 'cancelled')
         WHERE run_id = $1 AND tenant_id = $2 AND status <> 'done'`,
        [runId, tenantId]
      );
      await client.query('COMMIT');
      return result.rows[0] as WorkflowRunRecord;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function markRunRunning(
  pool: Pool,
  runId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<void> {
  await pool.query(
    `UPDATE runs
     SET status = 'running', updated_at = now()
     WHERE id = $1 AND tenant_id = $2 AND status IN ('pending', 'sleeping')`,
    [runId, tenantId]
  );
}

export async function completeRunAndTask(
  pool: Pool,
  task: TaskRecord,
  leaseToken: string,
  output: unknown
): Promise<boolean> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const taskResult = await client.query(
        `UPDATE tasks
         SET status = 'done',
             locked_by = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             last_error = NULL
         WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
        [task.id, leaseToken]
      );

      if ((taskResult.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query(
        `UPDATE runs
         SET status = 'completed',
             output = $2,
             error = NULL,
             updated_at = now(),
             completed_at = now()
         WHERE id = $1`,
        [task.run_id, output]
      );
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function rescheduleRunAndTask(
  pool: Pool,
  task: TaskRecord,
  leaseToken: string,
  delayMs: number,
  error: unknown,
  now: Date = systemClock.now()
): Promise<boolean> {
  const runAt = new Date(now.getTime() + delayMs);
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const taskResult = await client.query(
        `UPDATE tasks
         SET status = 'ready',
             run_at = $3,
             locked_by = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             last_error = $4
         WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
        [task.id, leaseToken, runAt, serializeError(error)]
      );

      if ((taskResult.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query(
        `UPDATE runs
         SET status = 'sleeping',
             error = $2,
             updated_at = now()
         WHERE id = $1`,
        [task.run_id, serializeError(error)]
      );
      await client.query("NOTIFY task_ready, 'rescheduled'");
      await client.query('COMMIT');
      return true;
    } catch (innerError) {
      await client.query('ROLLBACK');
      throw innerError;
    }
  });
}

export async function failRunAndDeadLetter(
  pool: Pool,
  task: TaskRecord,
  leaseToken: string,
  reason: string,
  error: unknown,
  payload: unknown
): Promise<boolean> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const taskResult = await client.query(
        `UPDATE tasks
         SET status = 'done',
             locked_by = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             last_error = $3
         WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
        [task.id, leaseToken, serializeError(error)]
      );

      if ((taskResult.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query(
        `UPDATE runs
         SET status = 'failed',
             error = $2,
             updated_at = now(),
             completed_at = now()
         WHERE id = $1`,
        [task.run_id, serializeError(error)]
      );
      await client.query(
        `INSERT INTO dead_letters (
          id, run_id, task_id, tenant_id, reason, error, payload, created_at, replayed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), NULL)`,
        [
          randomUUID(),
          task.run_id,
          task.id,
          task.tenant_id,
          reason,
          serializeError(error),
          payload
        ]
      );
      await client.query('COMMIT');
      return true;
    } catch (innerError) {
      await client.query('ROLLBACK');
      throw innerError;
    }
  });
}

export async function replayDeadLetter(
  pool: Pool,
  deadLetterId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<boolean> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const deadLetterResult = await client.query<DeadLetterRecord>(
        `SELECT * FROM dead_letters WHERE id = $1 AND tenant_id = $2 LIMIT 1 FOR UPDATE`,
        [deadLetterId, tenantId]
      );

      const deadLetter = deadLetterResult.rows[0];
      if (!deadLetter || deadLetter.replayed_at) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query(
        'UPDATE dead_letters SET replayed_at = now() WHERE id = $1',
        [deadLetterId]
      );
      await client.query(
        `UPDATE runs
         SET status = 'pending',
             error = NULL,
             completed_at = NULL,
             updated_at = now()
         WHERE id = $1`,
        [deadLetter.run_id]
      );
      await client.query(
        `INSERT INTO tasks (
          id, run_id, tenant_id, run_at, status, priority, attempts, max_attempts
        ) VALUES ($1, $2, $3, now(), 'ready', 0, 0, 10)`,
        [randomUUID(), deadLetter.run_id, deadLetter.tenant_id]
      );
      await client.query("NOTIFY task_ready, 'replay'");
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export type ReapResult = {
  requeued: number;
  deadLettered: number;
};

export async function reapExpiredTasks(
  pool: Pool,
  limit: number,
  now: Date = systemClock.now()
): Promise<ReapResult> {
  if (limit <= 0) {
    return { requeued: 0, deadLettered: 0 };
  }

  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const result = await client.query<TaskRecord>(
        `SELECT * FROM tasks
         WHERE status = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at < $1
         ORDER BY lease_expires_at ASC
         LIMIT $2
         FOR UPDATE SKIP LOCKED`,
        [now, limit]
      );

      const reaped: ReapResult = { requeued: 0, deadLettered: 0 };
      for (const task of result.rows) {
        if (task.attempts >= task.max_attempts) {
          const deadLetterResult = await client.query(
            `UPDATE tasks
             SET status = 'done',
                 locked_by = NULL,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 last_error = jsonb_build_object('reason', 'lease_expired')
             WHERE id = $1 AND status = 'leased'`,
            [task.id]
          );
          if ((deadLetterResult.rowCount ?? 0) > 0) {
            await client.query(
              `UPDATE runs
               SET status = 'failed', error = jsonb_build_object('reason', 'lease_expired'), updated_at = now(), completed_at = now()
               WHERE id = $1`,
              [task.run_id]
            );
            await client.query(
              `INSERT INTO dead_letters (
                id, run_id, task_id, tenant_id, reason, error, payload, created_at, replayed_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), NULL)`,
              [
                randomUUID(),
                task.run_id,
                task.id,
                task.tenant_id,
                'lease_lost',
                { reason: 'lease_expired' },
                task
              ]
            );
            reaped.deadLettered += 1;
          }
          continue;
        }

        const readyResult = await client.query(
          `UPDATE tasks
           SET status = 'ready',
               locked_by = NULL,
               lease_token = NULL,
               lease_expires_at = NULL,
               last_error = jsonb_build_object('reason', 'lease_expired')
           WHERE id = $1 AND status = 'leased'`,
          [task.id]
        );
        if ((readyResult.rowCount ?? 0) > 0) {
          await client.query(
            `UPDATE runs
             SET status = 'sleeping', error = jsonb_build_object('reason', 'lease_expired'), updated_at = now()
             WHERE id = $1`,
            [task.run_id]
          );
          await client.query("NOTIFY task_ready, 'reaped'");
          reaped.requeued += 1;
        }
      }

      await client.query('COMMIT');
      return reaped;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function releaseWorkerLeases(
  pool: Pool,
  workerId: string
): Promise<number> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const result = await client.query<TaskRecord>(
        `UPDATE tasks
         SET status = 'ready',
             locked_by = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             last_error = jsonb_build_object('reason', 'worker_shutdown')
         WHERE locked_by = $1 AND status = 'leased'
         RETURNING *`,
        [workerId]
      );

      for (const task of result.rows) {
        await client.query(
          `UPDATE runs
           SET status = 'sleeping', error = jsonb_build_object('reason', 'worker_shutdown'), updated_at = now()
           WHERE id = $1`,
          [task.run_id]
        );
      }

      await client.query('COMMIT');
      return result.rowCount ?? 0;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function repairStuckRuns(
  pool: Pool,
  limit: number,
  staleBefore: Date = systemClock.now()
): Promise<string[]> {
  if (limit <= 0) {
    return [];
  }

  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const repaired = await client.query<{ id: string; tenant_id: string }>(
        `WITH stuck AS (
           SELECT r.id AS run_id,
                  r.tenant_id,
                  COALESCE((SELECT max(t.priority) FROM tasks t WHERE t.run_id = r.id), 0) AS priority
           FROM runs r
           WHERE r.status IN ('pending', 'running', 'sleeping')
             AND r.updated_at < $1
             AND NOT EXISTS (
               SELECT 1 FROM tasks t
               WHERE t.run_id = r.id AND t.status IN ('ready', 'leased')
             )
           ORDER BY r.updated_at ASC
           LIMIT $2
           FOR UPDATE OF r SKIP LOCKED
         )
         UPDATE runs r
         SET status = 'pending',
             error = jsonb_build_object('reason', 'stuck_run_repaired'),
             updated_at = now()
         FROM stuck s
         WHERE r.id = s.run_id
         RETURNING r.id, r.tenant_id`,
        [staleBefore, limit]
      );

      for (const row of repaired.rows) {
        const priorityResult = await client.query<{ priority: number }>(
          'SELECT COALESCE(max(priority), 0)::int AS priority FROM tasks WHERE run_id = $1',
          [row.id]
        );
        await client.query(
          `INSERT INTO tasks (
             id, run_id, tenant_id, run_at, status, priority, attempts, max_attempts
           ) VALUES ($1, $2, $3, now(), 'ready', $4, 0, 10)`,
          [
            randomUUID(),
            row.id,
            row.tenant_id,
            priorityResult.rows[0]?.priority ?? 0
          ]
        );
      }

      if (repaired.rows.length > 0) {
        await client.query("NOTIFY task_ready, 'repaired'");
      }
      await client.query('COMMIT');
      return repaired.rows.map((row) => row.id);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function sleepRunAndTask(
  pool: Pool,
  task: TaskRecord,
  leaseToken: string,
  wakeAt: Date,
  now: Date = systemClock.now()
): Promise<boolean> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const taskResult = await client.query(
        `UPDATE tasks
         SET status = 'ready',
             run_at = $3,
             locked_by = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             last_error = NULL
         WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
        [task.id, leaseToken, wakeAt]
      );

      if ((taskResult.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      const runResult = await client.query(
        `UPDATE runs
         SET status = 'sleeping',
             error = NULL,
             updated_at = $2
         WHERE id = $1 AND status IN ('pending', 'running', 'sleeping')`,
        [task.run_id, now]
      );

      if ((runResult.rowCount ?? 0) === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function recordSleepStep(
  pool: Pool,
  runId: string,
  stepKey: string,
  wakeAt: Date,
  decidedAt: Date
): Promise<{ wake_at: Date; created: boolean }> {
  const inserted = await pool.query<{ wake_at: Date }>(
    `INSERT INTO steps (
       run_id, step_key, status, output, attempts, last_error,
       started_at, finished_at, wake_at
     ) VALUES ($1, $2, 'completed', $3, 1, NULL, $4, $4, $5)
     ON CONFLICT (run_id, step_key) DO NOTHING
     RETURNING wake_at`,
    [runId, stepKey, { wake_at: wakeAt.toISOString() }, decidedAt, wakeAt]
  );

  if ((inserted.rowCount ?? 0) > 0 && inserted.rows[0]) {
    return { wake_at: inserted.rows[0].wake_at, created: true };
  }

  const existing = await pool.query<{ wake_at: Date | null }>(
    'SELECT wake_at FROM steps WHERE run_id = $1 AND step_key = $2 LIMIT 1',
    [runId, stepKey]
  );
  const stored = existing.rows[0]?.wake_at;
  if (!stored) {
    throw new Error(`sleep step ${runId}:${stepKey} has no wake time`);
  }
  return { wake_at: stored, created: false };
}

export async function ensureRunTraceparent(
  pool: Pool,
  runId: string,
  generate: () => string
): Promise<string | null> {
  const claimed = await pool.query<{ traceparent: string }>(
    `UPDATE runs
     SET traceparent = $2, updated_at = now()
     WHERE id = $1 AND traceparent IS NULL
     RETURNING traceparent`,
    [runId, generate()]
  );

  const returned = claimed.rows[0]?.traceparent;
  if (returned) {
    return returned;
  }

  const existing = await pool.query<{ traceparent: string | null }>(
    'SELECT traceparent FROM runs WHERE id = $1 LIMIT 1',
    [runId]
  );
  return existing.rows[0]?.traceparent ?? null;
}

export async function claimTasks(
  pool: Pool,
  input: ClaimTasksInput
): Promise<ClaimResult> {
  if (input.limit <= 0) {
    return { tasks: [] };
  }

  const now = input.now ?? systemClock.now();
  const leaseExpiresAt = new Date(now.getTime() + input.leaseMs);

  const result = await pool.query<TaskRecord>(
    `WITH running AS (
       SELECT tenant_id, count(*)::int AS running
       FROM tasks
       WHERE status = 'leased'
       GROUP BY tenant_id
     ),
     serving AS (
       SELECT tenant.id AS tenant_id,
              tenant.last_claim_at,
              tenant.max_concurrent_tasks,
              COALESCE(running.running, 0) AS running
       FROM tenants tenant
       LEFT JOIN running ON running.tenant_id = tenant.id
       WHERE (tenant.max_concurrent_tasks = 0
              OR COALESCE(running.running, 0) < tenant.max_concurrent_tasks)
         AND EXISTS (
           SELECT 1 FROM tasks ready
           WHERE ready.tenant_id = tenant.id
             AND ready.status = 'ready'
             AND ready.run_at <= $2
         )
       ORDER BY tenant.last_claim_at ASC NULLS FIRST, tenant.id ASC
       FOR UPDATE OF tenant SKIP LOCKED
       LIMIT $3
     ),
     budget AS (
       SELECT tenant_id,
              last_claim_at,
              LEAST(
                CASE WHEN max_concurrent_tasks = 0 THEN $3::int
                     ELSE GREATEST(0, max_concurrent_tasks - running) END,
                GREATEST(1, $3::int / GREATEST((SELECT count(*)::int FROM serving), 1))
              )::int AS share
       FROM serving
     ),
     picked AS (
       SELECT candidate.id
       FROM budget
       CROSS JOIN LATERAL (
         SELECT task.id, task.priority, task.run_at,
                budget.last_claim_at AS last_claim_at, budget.tenant_id AS tenant_id
         FROM tasks task
         WHERE task.tenant_id = budget.tenant_id
           AND task.status = 'ready'
           AND task.run_at <= $2
         ORDER BY task.priority DESC, task.run_at ASC, task.id ASC
         LIMIT budget.share
         FOR UPDATE SKIP LOCKED
       ) candidate
       ORDER BY candidate.last_claim_at ASC NULLS FIRST,
                candidate.tenant_id ASC,
                candidate.priority DESC,
                candidate.run_at ASC
       LIMIT $3
     ),
     served AS (
       UPDATE tenants tenant
       SET last_claim_at = $2
       WHERE tenant.id IN (SELECT tenant_id FROM serving)
       RETURNING tenant.id
     )
     UPDATE tasks
     SET status = 'leased',
         locked_by = $1,
         lease_token = gen_random_uuid(),
         lease_expires_at = $4,
         claimed_at = $2,
         attempts = tasks.attempts + 1
     FROM picked
     WHERE tasks.id = picked.id
     RETURNING tasks.*`,
    [input.workerId, now, input.limit, leaseExpiresAt]
  );

  return { tasks: result.rows };
}

export async function extendLease(
  pool: Pool,
  taskId: string,
  leaseToken: string,
  leaseMs: number,
  now: Date = systemClock.now()
): Promise<boolean> {
  const leaseExpiresAt = new Date(now.getTime() + leaseMs);
  const result = await pool.query(
    `UPDATE tasks
     SET lease_expires_at = $3
     WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
    [taskId, leaseToken, leaseExpiresAt]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function markTaskDone(
  pool: Pool,
  taskId: string,
  leaseToken: string
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE tasks
     SET status = 'done', lease_expires_at = NULL, locked_by = NULL
     WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
    [taskId, leaseToken]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function rescheduleTask(
  pool: Pool,
  taskId: string,
  leaseToken: string,
  delayMs: number,
  error: unknown,
  now: Date = systemClock.now()
): Promise<boolean> {
  const nextRunAt = new Date(now.getTime() + delayMs);
  const result = await pool.query(
    `UPDATE tasks
     SET status = 'ready',
         run_at = $3,
         locked_by = NULL,
         lease_token = NULL,
         lease_expires_at = NULL,
         last_error = $4
     WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
    [taskId, leaseToken, nextRunAt, serializeError(error)]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function deadLetterTask(
  pool: Pool,
  task: TaskRecord,
  reason: string,
  error: unknown,
  payload: unknown,
  leaseToken: string
): Promise<boolean> {
  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      const result = await client.query(
        `UPDATE tasks
         SET status = 'done',
             locked_by = NULL,
             lease_token = NULL,
             lease_expires_at = NULL,
             last_error = $3
         WHERE id = $1 AND lease_token = $2 AND status = 'leased'`,
        [task.id, leaseToken, serializeError(error)]
      );

      if (result.rowCount === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      await client.query(
        `INSERT INTO dead_letters (
          id, run_id, task_id, tenant_id, reason, error, payload, created_at, replayed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, now(), NULL)`,
        [
          randomUUID(),
          task.run_id,
          task.id,
          task.tenant_id,
          reason,
          serializeError(error),
          payload
        ]
      );
      await client.query('COMMIT');
      return true;
    } catch (innerError) {
      await client.query('ROLLBACK');
      throw innerError;
    }
  });
}

export async function recordStepFailure(
  pool: Pool,
  runId: string,
  stepKey: string,
  attempts: number,
  error: unknown,
  startedAt: Date,
  finishedAt: Date
): Promise<void> {
  await pool.query(
    `INSERT INTO steps (
      run_id, step_key, status, output, attempts, last_error, started_at, finished_at
    ) VALUES ($1, $2, 'failed', NULL, $3, $4, $5, $6)
    ON CONFLICT (run_id, step_key) DO UPDATE SET
      status = EXCLUDED.status,
      attempts = EXCLUDED.attempts,
      last_error = EXCLUDED.last_error,
      started_at = EXCLUDED.started_at,
      finished_at = EXCLUDED.finished_at
    WHERE steps.status <> 'completed'`,
    [runId, stepKey, attempts, serializeError(error), startedAt, finishedAt]
  );
}

export async function recordStepSuccess<T>(
  pool: Pool,
  runId: string,
  stepKey: string,
  attempts: number,
  output: T,
  startedAt: Date,
  finishedAt: Date
): Promise<T> {
  const result = await pool.query<WorkflowStepRecord>(
    `INSERT INTO steps (
      run_id, step_key, status, output, attempts, last_error, started_at, finished_at
    ) VALUES ($1, $2, 'completed', $3, $4, NULL, $5, $6)
    ON CONFLICT (run_id, step_key) DO UPDATE SET
      status = 'completed',
      output = EXCLUDED.output,
      attempts = EXCLUDED.attempts,
      last_error = NULL,
      started_at = EXCLUDED.started_at,
      finished_at = EXCLUDED.finished_at
    WHERE steps.status <> 'completed'
    RETURNING *`,
    [runId, stepKey, output, attempts, startedAt, finishedAt]
  );
  if ((result.rowCount ?? 0) > 0) {
    return (result.rows[0]?.output as T) ?? output;
  }
  const existing = await pool.query<WorkflowStepRecord>(
    'SELECT output FROM steps WHERE run_id = $1 AND step_key = $2 LIMIT 1',
    [runId, stepKey]
  );
  return (existing.rows[0]?.output as T) ?? output;
}

export async function getStep(
  pool: Pool,
  runId: string,
  stepKey: string
): Promise<WorkflowStepRecord | null> {
  const result = await pool.query<WorkflowStepRecord>(
    'SELECT * FROM steps WHERE run_id = $1 AND step_key = $2 LIMIT 1',
    [runId, stepKey]
  );
  return result.rows[0] ?? null;
}

export async function listSteps(
  pool: Pool,
  runId: string
): Promise<WorkflowStepRecord[]> {
  const result = await pool.query<WorkflowStepRecord>(
    'SELECT * FROM steps WHERE run_id = $1 ORDER BY started_at ASC',
    [runId]
  );
  return result.rows;
}

export function createTaskDecisionFromFailure(
  attempts: number,
  policy: ReturnType<typeof createRetryPolicy>,
  error: unknown,
  random: () => number = Math.random
): TaskDecision {
  if (attempts >= policy.maxAttempts) {
    return { kind: 'dead-letter', error, reason: 'step_exhausted' };
  }

  const delayMs = Math.floor(
    random() * Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** attempts)
  );
  return { kind: 'retry', error, delayMs };
}

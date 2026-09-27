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

export async function createDatabasePool(databaseUrl: string): Promise<Pool> {
  const { Pool } = await import('pg');
  return new Pool({ connectionString: databaseUrl });
}

async function withClient<T>(
  pool: Pool,
  callback: (..._args: [PoolClient]) => Promise<T>
): Promise<T> {
  const client = await pool.connect();
  try {
    return await callback(client);
  } finally {
    client.release();
  }
}

export async function createRun(
  pool: Pool,
  input: CreateRunInput
): Promise<WorkflowRunRecord> {
  const tenantId = input.tenantId ?? 'default';
  const taskMaxAttempts = input.taskMaxAttempts ?? 10;
  const priority = input.priority ?? 0;

  return withClient(pool, async (client) => {
    await client.query('BEGIN');
    try {
      if (input.idempotencyKey) {
        const existing = await client.query<WorkflowRunRecord>(
          'SELECT * FROM runs WHERE tenant_id = $1 AND idempotency_key = $2 LIMIT 1',
          [tenantId, input.idempotencyKey]
        );

        if ((existing.rowCount ?? 0) > 0) {
          await client.query('COMMIT');
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
        [
          runId,
          tenantId,
          input.workflow,
          input.input,
          input.idempotencyKey ?? null
        ]
      );

      if ((runResult.rowCount ?? 0) === 0) {
        const existing = await client.query<WorkflowRunRecord>(
          'SELECT * FROM runs WHERE tenant_id = $1 AND idempotency_key = $2 LIMIT 1',
          [tenantId, input.idempotencyKey]
        );
        await client.query('COMMIT');
        return existing.rows[0] as WorkflowRunRecord;
      }

      const taskId = randomUUID();
      await client.query(
        `INSERT INTO tasks (
          id, run_id, tenant_id, run_at, status, priority, attempts, max_attempts
        ) VALUES ($1, $2, $3, now(), 'ready', $4, 0, $5)`,
        [taskId, runId, tenantId, priority, taskMaxAttempts]
      );
      await client.query("NOTIFY task_ready, 'created'");
      await client.query('COMMIT');
      return runResult.rows[0] as WorkflowRunRecord;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });
}

export async function getRun(
  pool: Pool,
  runId: string,
  tenantId = 'default'
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
  tenantId = 'default'
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
  tenantId = 'default'
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
  tenantId = 'default'
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
  tenantId = 'default'
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

export async function reapExpiredTasks(
  pool: Pool,
  limit: number,
  now: Date = systemClock.now()
): Promise<number> {
  if (limit <= 0) {
    return 0;
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

      let touched = 0;
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
            touched += 1;
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
          touched += 1;
        }
      }

      await client.query('COMMIT');
      return touched;
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
    `WITH picked AS (
      SELECT id
      FROM tasks
      WHERE status = 'ready' AND run_at <= $2
      ORDER BY priority DESC, run_at ASC
      LIMIT $3
      FOR UPDATE SKIP LOCKED
    )
    UPDATE tasks
    SET status = 'leased',
        locked_by = $1,
        lease_token = gen_random_uuid(),
        lease_expires_at = $4,
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

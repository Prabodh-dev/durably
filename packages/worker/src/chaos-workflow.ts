import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';

import { createDatabasePool, defineWorkflow } from '@durably/sdk';

type ChaosInput = Record<string, never>;

type ChaosOutput = {
  runId: string;
  value: number;
};

let poolPromise: Promise<Pool> | null = null;

async function getPool(): Promise<Pool> {
  if (!poolPromise) {
    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
      throw new Error('DATABASE_URL is required for the chaos workflow');
    }
    poolPromise = createDatabasePool(databaseUrl);
  }
  return poolPromise;
}

export async function recordStepExecution(
  runId: string,
  stepKey: string,
  idempotencyKey: string,
  note: string
): Promise<void> {
  const pool = await getPool();
  const attempt = await pool.query(
    `INSERT INTO example_side_effects (run_id, step_key, idempotency_key, note)
     VALUES ($1, $2, $3, $4)`,
    [runId, stepKey, `${idempotencyKey}:exec:${randomUUID()}`, note]
  );
  if ((attempt.rowCount ?? 0) === 0) {
    throw new Error('execution record was not written');
  }

  await pool.query(
    `INSERT INTO example_side_effects (run_id, step_key, idempotency_key, note)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (idempotency_key) DO NOTHING`,
    [runId, stepKey, idempotencyKey, note]
  );
}

export async function countStepExecutions(
  runId: string,
  stepKey: string
): Promise<number> {
  const pool = await getPool();
  const result = await pool.query<{ count: number }>(
    `SELECT count(*)::int AS count
     FROM example_side_effects
     WHERE run_id = $1 AND step_key = $2 AND idempotency_key LIKE $3`,
    [runId, stepKey, `${runId}:${stepKey}:exec:%`]
  );
  return result.rows[0]?.count ?? 0;
}

export const chaosWorkflow = defineWorkflow<ChaosInput, ChaosOutput>(
  {
    id: 'chaos',
    retry: {
      maxAttempts: 20,
      baseDelayMs: 200,
      maxDelayMs: 2000
    }
  },
  async ({ runId, step }) => {
    const first = await step.run('first', async (idempotencyKey) => {
      await recordStepExecution(runId, 'first', idempotencyKey, 'first');
      return { value: 1 };
    });

    const slow = await step.run('slow', async (idempotencyKey) => {
      await recordStepExecution(runId, 'slow', idempotencyKey, 'slow');
      const delayMs = Number(process.env.DURABLY_CHAOS_STEP_DELAY_MS ?? '0');
      if (delayMs > 0) {
        await new Promise<void>((resolve) => {
          setTimeout(resolve, delayMs);
        });
      }
      return { value: first.value + 1 };
    });

    const last = await step.run('last', async (idempotencyKey) => {
      await recordStepExecution(runId, 'last', idempotencyKey, 'last');
      return { value: slow.value + 1 };
    });

    return { runId, value: last.value };
  }
);

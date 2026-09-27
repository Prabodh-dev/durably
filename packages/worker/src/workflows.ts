import { createDatabasePool, defineWorkflow } from '@durably/sdk';

let poolPromise: Promise<
  Awaited<ReturnType<typeof createDatabasePool>>
> | null = null;

type OnboardUserInput = {
  email: string;
  name: string;
  plan: string;
};

type OnboardUserOutput = {
  welcomeMessage: string;
  approved: boolean;
  runId: string;
};

async function getPool(): Promise<
  Awaited<ReturnType<typeof createDatabasePool>>
> {
  if (!poolPromise) {
    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
      throw new Error('DATABASE_URL is required for the example workflow');
    }
    poolPromise = createDatabasePool(databaseUrl);
  }
  return poolPromise;
}

async function insertSideEffect(
  runId: string,
  stepKey: string,
  idempotencyKey: string,
  note: string
) {
  const pool = await getPool();
  const inserted = await pool.query(
    `INSERT INTO example_side_effects (run_id, step_key, idempotency_key, note)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING *`,
    [runId, stepKey, idempotencyKey, note]
  );

  if ((inserted.rowCount ?? 0) > 0) {
    return inserted.rows[0];
  }

  const existing = await pool.query(
    'SELECT * FROM example_side_effects WHERE idempotency_key = $1 LIMIT 1',
    [idempotencyKey]
  );
  return existing.rows[0];
}

export const onboardUserWorkflow = defineWorkflow<
  OnboardUserInput,
  OnboardUserOutput
>(
  {
    id: 'onboard-user',
    retry: {
      maxAttempts: 6,
      baseDelayMs: 250,
      maxDelayMs: 2000
    }
  },
  async ({ input, runId, step }) => {
    const profile = await step.run('create-profile', async (idempotencyKey) => {
      const row = await insertSideEffect(
        runId,
        'create-profile',
        idempotencyKey,
        `profile:${input.email}`
      );
      return {
        sideEffectId: row.id,
        email: input.email,
        plan: input.plan
      };
    });

    const enriched = await step.run('enrich-profile', async () => {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 1000);
      });
      return {
        ...profile,
        displayName: input.name.trim().toUpperCase()
      };
    });

    const approval = await step.run(
      'maybe-fail',
      async () => {
        const failureRate = Number(
          process.env.ONBOARD_USER_FAILURE_RATE ?? '0.35'
        );
        if (Math.random() < failureRate) {
          throw new Error('transient onboarding failure');
        }
        return {
          approved: true,
          riskScore: Math.floor(Math.random() * 100)
        };
      },
      {
        maxAttempts: 5,
        baseDelayMs: 150,
        maxDelayMs: 1500
      }
    );

    const finalized = await step.run('finalize', async () => {
      return {
        welcomeMessage: `Welcome, ${enriched.displayName}`,
        approved: approval.approved,
        runId
      };
    });

    return finalized;
  }
);

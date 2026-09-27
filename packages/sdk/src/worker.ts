import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';

import type { PoolClient } from 'pg';
import type {
  Clock,
  RetryPolicy,
  StepFn,
  StepOptions,
  StepRunner,
  TaskRecord,
  WorkflowDefinition,
  WorkflowStepRecord
} from '@durably/core';
import {
  claimTasks,
  completeRunAndTask,
  computeBackoffDelayMs,
  createDatabasePool,
  createRetryPolicy,
  extendLease,
  failRunAndDeadLetter,
  getRunWithSteps,
  markRunRunning,
  recordStepFailure,
  recordStepSuccess,
  reapExpiredTasks,
  releaseWorkerLeases,
  rescheduleRunAndTask,
  runMigrations,
  systemClock
} from '@durably/core';

export type WorkerOptions = {
  databaseUrl: string;
  workflows: ReadonlyArray<WorkflowDefinition<unknown, unknown>>;
  concurrency?: number;
  leaseMs?: number;
  claimBatchSize?: number;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  shutdownTimeoutMs?: number;
  reaperIntervalMs?: number;
  migrationsDir?: string;
  workerId?: string;
  random?: () => number;
  clock?: Clock;
};

export type WorkerHandle = {
  start(): Promise<void>;
  stop(): Promise<void>;
  readonly workerId: string;
};

class StepFailureError extends Error {
  public readonly stepKey: string;
  public readonly attempt: number;
  public readonly policy: RetryPolicy;
  public readonly cause: unknown;

  public constructor(
    stepKey: string,
    attempt: number,
    policy: RetryPolicy,
    cause: unknown
  ) {
    super(`step ${stepKey} failed`);
    this.name = 'StepFailureError';
    this.stepKey = stepKey;
    this.attempt = attempt;
    this.policy = policy;
    this.cause = cause;
  }
}

export function createWorker(options: WorkerOptions): WorkerHandle {
  const workerId =
    options.workerId ??
    process.env.DURABLY_WORKER_ID ??
    process.env.HOSTNAME ??
    os.hostname();
  const concurrency = options.concurrency ?? 4;
  const leaseMs = options.leaseMs ?? 15000;
  const claimBatchSize = options.claimBatchSize ?? concurrency;
  const pollIntervalMs = options.pollIntervalMs ?? 1000;
  const heartbeatIntervalMs =
    options.heartbeatIntervalMs ?? Math.max(1000, Math.floor(leaseMs / 3));
  const shutdownTimeoutMs = options.shutdownTimeoutMs ?? 15000;
  const reaperIntervalMs = options.reaperIntervalMs ?? 5000;
  const clock = options.clock ?? systemClock;
  const random = options.random ?? Math.random;
  const workflows = new Map<string, WorkflowDefinition<unknown, unknown>>(
    options.workflows.map((workflow) => [workflow.id, workflow])
  );

  let stopping = false;
  let started = false;
  let activeClaims = false;
  let inflight = 0;
  let pool: Awaited<ReturnType<typeof createDatabasePool>> | null = null;
  let listenerPool: Awaited<ReturnType<typeof createDatabasePool>> | null =
    null;
  let listenerClient: PoolClient | null = null;
  let pollTimer: NodeJS.Timeout | null = null;
  let reaperTimer: NodeJS.Timeout | null = null;
  const heartbeatTimers = new Map<string, NodeJS.Timeout>();
  let stopResolver: (() => void) | null = null;
  const stopPromise = new Promise<void>((resolve) => {
    stopResolver = resolve;
  });

  function signalPump(): void {
    if (!stopping) {
      void pump();
    }
  }

  async function processTask(task: TaskRecord): Promise<void> {
    const dbPool = pool;
    if (!dbPool || !task.lease_token) {
      return;
    }

    const runWithSteps = await getRunWithSteps(
      dbPool,
      task.run_id,
      task.tenant_id
    );
    const run = runWithSteps.run;
    if (!run) {
      await completeRunAndTask(dbPool, task, task.lease_token, null);
      return;
    }

    if (
      run.status === 'cancelled' ||
      run.status === 'completed' ||
      run.status === 'failed'
    ) {
      await completeRunAndTask(
        dbPool,
        task,
        task.lease_token,
        run.output ?? null
      );
      return;
    }

    await markRunRunning(dbPool, run.id, run.tenant_id);

    const workflow = workflows.get(run.workflow);
    if (!workflow) {
      await failRunAndDeadLetter(
        dbPool,
        task,
        task.lease_token,
        'workflow_failed',
        { message: `unknown workflow ${run.workflow}` },
        task
      );
      return;
    }

    const completedSteps = new Map<string, WorkflowStepRecord>(
      runWithSteps.steps.map((step) => [step.step_key, step])
    );
    const seenKeys = new Set<string>();
    const controller = new AbortController();

    const heartbeatTimer = setInterval(() => {
      if (!dbPool || !task.lease_token) {
        return;
      }
      void extendLease(
        dbPool,
        task.id,
        task.lease_token,
        leaseMs,
        clock.now()
      ).then((extended) => {
        if (!extended) {
          controller.abort();
        }
      });
    }, heartbeatIntervalMs);
    heartbeatTimers.set(task.id, heartbeatTimer);

    const stepRunner: StepRunner = {
      async run<T>(
        key: string,
        fn: StepFn<T>,
        options?: StepOptions
      ): Promise<T> {
        if (controller.signal.aborted) {
          throw new Error('lease lost');
        }
        if (seenKeys.has(key)) {
          throw new Error(`duplicate step key: ${key}`);
        }
        seenKeys.add(key);

        const existingStep = completedSteps.get(key);
        if (existingStep?.status === 'completed') {
          return existingStep.output as T;
        }

        const policy = createRetryPolicy({
          ...workflow.retry,
          ...options
        });
        const attempts = (existingStep?.attempts ?? 0) + 1;
        const startedAt = clock.now();
        try {
          const value = await fn(`${run.id}:${key}`);
          const finishedAt = clock.now();
          const stored = await recordStepSuccess(
            dbPool,
            run.id,
            key,
            attempts,
            value,
            startedAt,
            finishedAt
          );
          completedSteps.set(key, {
            run_id: run.id,
            step_key: key,
            status: 'completed',
            output: stored,
            attempts,
            last_error: null,
            started_at: startedAt,
            finished_at: finishedAt
          });
          return stored;
        } catch (error) {
          const finishedAt = clock.now();
          await recordStepFailure(
            dbPool,
            run.id,
            key,
            attempts,
            error,
            startedAt,
            finishedAt
          );
          throw new StepFailureError(key, attempts, policy, error);
        }
      }
    };

    try {
      const output = await workflow.handler({
        input: run.input,
        runId: run.id,
        step: stepRunner,
        signal: controller.signal
      });

      if (controller.signal.aborted) {
        throw new Error('lease lost');
      }

      const completed = await completeRunAndTask(
        dbPool,
        task,
        task.lease_token,
        output
      );
      if (!completed) {
        throw new Error('stale lease');
      }
    } catch (error) {
      if (
        controller.signal.aborted ||
        (error instanceof Error &&
          (error.message === 'lease lost' || error.message === 'stale lease'))
      ) {
        return;
      }

      if (error instanceof StepFailureError) {
        if (error.attempt >= error.policy.maxAttempts) {
          await failRunAndDeadLetter(
            dbPool,
            task,
            task.lease_token,
            'step_exhausted',
            error.cause,
            {
              runId: run.id,
              stepKey: error.stepKey,
              attempts: error.attempt
            }
          );
        } else {
          const delayMs = computeBackoffDelayMs(
            error.attempt,
            error.policy,
            random
          );
          await rescheduleRunAndTask(
            dbPool,
            task,
            task.lease_token,
            delayMs,
            error.cause,
            clock.now()
          );
        }
        return;
      }

      if (task.attempts >= task.max_attempts) {
        await failRunAndDeadLetter(
          dbPool,
          task,
          task.lease_token,
          'task_exhausted',
          error,
          task
        );
        return;
      }

      const policy = createRetryPolicy(workflow.retry);
      const delayMs = computeBackoffDelayMs(task.attempts, policy, random);
      await rescheduleRunAndTask(
        dbPool,
        task,
        task.lease_token,
        delayMs,
        error,
        clock.now()
      );
    } finally {
      clearInterval(heartbeatTimer);
      heartbeatTimers.delete(task.id);
    }
  }

  async function pump(): Promise<void> {
    const dbPool = pool;
    if (!dbPool || stopping || activeClaims) {
      return;
    }

    activeClaims = true;
    try {
      while (!stopping && inflight < concurrency) {
        const batch = Math.min(claimBatchSize, concurrency - inflight);
        const claimed = await claimTasks(dbPool, {
          workerId,
          limit: batch,
          leaseMs,
          now: clock.now()
        });
        if (claimed.tasks.length === 0) {
          break;
        }

        for (const task of claimed.tasks) {
          inflight += 1;
          void processTask(task)
            .catch(() => undefined)
            .finally(() => {
              inflight -= 1;
              signalPump();
            });
        }
      }
    } finally {
      activeClaims = false;
    }
  }

  async function stop(): Promise<void> {
    if (stopping) {
      return;
    }
    stopping = true;

    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    if (reaperTimer) {
      clearInterval(reaperTimer);
      reaperTimer = null;
    }
    for (const timer of heartbeatTimers.values()) {
      clearInterval(timer);
    }
    heartbeatTimers.clear();

    const startedAt = clock.now().getTime();
    while (
      inflight > 0 &&
      clock.now().getTime() - startedAt < shutdownTimeoutMs
    ) {
      await sleep(100);
    }

    if (pool) {
      await releaseWorkerLeases(pool, workerId);
    }

    if (listenerClient) {
      try {
        await listenerClient.query('UNLISTEN *');
      } finally {
        listenerClient.release();
      }
      listenerClient = null;
    }

    if (listenerPool) {
      await listenerPool.end();
      listenerPool = null;
    }

    if (pool) {
      await pool.end();
      pool = null;
    }

    stopResolver?.();
    stopResolver = null;
  }

  async function start(): Promise<void> {
    if (started) {
      await stopPromise;
      return;
    }
    started = true;
    pool = await createDatabasePool(options.databaseUrl);
    await runMigrations(
      pool,
      options.migrationsDir ??
        fileURLToPath(new URL('../../../migrations', import.meta.url))
    );
    listenerPool = await createDatabasePool(options.databaseUrl);
    listenerClient = await listenerPool.connect();
    await listenerClient.query('LISTEN task_ready');
    listenerClient.on('notification', signalPump);

    pollTimer = setInterval(() => {
      signalPump();
    }, pollIntervalMs);

    reaperTimer = setInterval(() => {
      const dbPool = pool;
      if (!dbPool) {
        return;
      }
      void reapExpiredTasks(dbPool, concurrency, clock.now()).then((count) => {
        if (count > 0) {
          signalPump();
        }
      });
    }, reaperIntervalMs);

    signalPump();
    await stopPromise;
  }

  return {
    workerId,
    start,
    stop
  };
}

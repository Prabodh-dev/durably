import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';

import type { PoolClient } from 'pg';
import { context as otelContext, trace as otelTrace } from '@opentelemetry/api';
import type {
  Clock,
  DurablyMetrics,
  LeaderHandle,
  Logger,
  RetryPolicy,
  SleepDuration,
  StepFn,
  StepOptions,
  StepRunner,
  TaskOutcome,
  TaskRecord,
  TelemetryHandle,
  WorkflowDefinition,
  WorkflowStepRecord
} from '@durably/core';
import {
  claimTasks,
  completeRunAndTask,
  computeBackoffDelayMs,
  createDatabasePool,
  createLeaderDuties,
  createLeaderElector,
  createLogger,
  createRetryPolicy,
  createRunTraceparent,
  endSpan,
  ensureRunTraceparent,
  extendLease,
  extractTraceContext,
  failRunAndDeadLetter,
  getMetrics,
  getRunWithSteps,
  markRunRunning,
  NOOP_TELEMETRY,
  parseDurationMs,
  recordSleepStep,
  recordStepFailure,
  recordStepSuccess,
  releaseWorkerLeases,
  rescheduleRunAndTask,
  runMigrations,
  sleepRunAndTask,
  startRunSpan,
  startStepSpan,
  systemClock,
  toDate
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
  stuckRunIntervalMs?: number;
  stuckRunGraceMs?: number;
  leaderIntervalMs?: number;
  cronIntervalMs?: number;
  leader?: boolean;
  migrationsDir?: string;
  workerId?: string;
  random?: () => number;
  clock?: Clock;
  metrics?: DurablyMetrics;
  telemetry?: TelemetryHandle;
  logger?: Logger;
  onTaskReady?: () => void;
};

export type WorkerHandle = {
  start(): Promise<void>;
  stop(): Promise<void>;
  readonly workerId: string;
  isLeader(): boolean;
  inflightTasks(): number;
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

export class SleepDeferredError extends Error {
  public readonly stepKey: string;
  public readonly wakeAt: Date;

  public constructor(stepKey: string, wakeAt: Date) {
    super(`sleeping at step ${stepKey} until ${wakeAt.toISOString()}`);
    this.name = 'SleepDeferredError';
    this.stepKey = stepKey;
    this.wakeAt = wakeAt;
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
  const reaperIntervalMs = options.reaperIntervalMs ?? 1000;
  const leaderIntervalMs = options.leaderIntervalMs ?? 1000;
  const stuckRunIntervalMs = options.stuckRunIntervalMs ?? 10000;
  const stuckRunGraceMs = options.stuckRunGraceMs ?? 30000;
  const cronIntervalMs = options.cronIntervalMs ?? 1000;
  const participateInLeaderElection = options.leader ?? true;
  const clock = options.clock ?? systemClock;
  const random = options.random ?? Math.random;
  const metrics = options.metrics ?? getMetrics();
  const telemetry = options.telemetry ?? NOOP_TELEMETRY;
  const logger =
    options.logger ??
    createLogger({ component: 'worker', worker_id: workerId }, {});
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
  let listenerReconnectTimer: NodeJS.Timeout | null = null;
  let listenerConnecting = false;
  let pollTimer: NodeJS.Timeout | null = null;
  let leader: LeaderHandle | null = null;
  let duties: ReturnType<typeof createLeaderDuties> | null = null;
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

    const taskLogger = logger.child({
      worker_id: workerId,
      task_id: task.id,
      run_id: task.run_id,
      tenant_id: task.tenant_id
    });
    const startedAtMs = clock.now().getTime();

    const runWithSteps = await getRunWithSteps(
      dbPool,
      task.run_id,
      task.tenant_id
    );
    const run = runWithSteps.run;
    if (!run) {
      await completeRunAndTask(dbPool, task, task.lease_token, null);
      observeTask('abandoned', 'error', null, undefined);
      return;
    }
    const runId = run.id;
    const workflowName = run.workflow;

    function observeTask(
      label: TaskOutcome,
      workflow: string | null,
      span: ReturnType<typeof startRunSpan> | null,
      error: unknown
    ): void {
      taskLogger.info(
        {
          event: 'task_finished',
          outcome: label,
          duration_ms: clock.now().getTime() - startedAtMs
        },
        'task finished'
      );
      if (workflow !== null) {
        metrics.taskDuration.observe(
          { workflow, outcome: label },
          (clock.now().getTime() - startedAtMs) / 1000
        );
      }
      if (span) {
        endSpan(span, error);
      }
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
      observeTask('abandoned', workflowName, null, undefined);
      return;
    }

    const traceparent = telemetry.enabled
      ? await ensureRunTraceparent(dbPool, runId, createRunTraceparent)
      : null;
    const runSpan =
      traceparent === null
        ? null
        : startRunSpan(telemetry, traceparent, {
            'durably.run_id': runId,
            'durably.tenant_id': run.tenant_id,
            'durably.workflow': run.workflow,
            'durably.attempt': task.attempts
          });
    const runContext =
      traceparent === null
        ? otelContext.active()
        : otelTrace.setSpan(extractTraceContext(traceparent), runSpan as never);
    taskLogger.debug(
      { event: 'task_started', attempt: task.attempts, traceparent },
      'task started'
    );

    await markRunRunning(dbPool, run.id, run.tenant_id);

    const workflow = workflows.get(run.workflow);
    if (!workflow) {
      const failure = { message: `unknown workflow ${run.workflow}` };
      await failRunAndDeadLetter(
        dbPool,
        task,
        task.lease_token,
        'workflow_failed',
        failure,
        task
      );
      metrics.deadLetters.inc({ reason: 'workflow_failed' });
      taskLogger.error(
        { event: 'dead_lettered', reason: 'workflow_failed', ...failure },
        'dead letter written'
      );
      observeTask('dead_lettered', workflowName, runSpan, failure);
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
      // A database that cannot be reached means the lease can no longer be
      // proven, so the task is abandoned and the reaper hands it to someone
      // else. Letting the rejection escape would kill the worker.
      void extendLease(dbPool, task.id, task.lease_token, leaseMs, clock.now())
        .then((extended) => {
          if (!extended) {
            controller.abort();
          }
        })
        .catch((error: unknown) => {
          logger.warn(
            {
              event: 'lease_heartbeat_failed',
              run_id: runId,
              task_id: task.id,
              error: String(error)
            },
            'could not extend the lease, abandoning the task'
          );
          controller.abort();
        });
    }, heartbeatIntervalMs);
    heartbeatTimers.set(task.id, heartbeatTimer);

    async function deferSleep(key: string, wakeAt: Date): Promise<void> {
      if (!dbPool || !task.lease_token || controller.signal.aborted) {
        throw new Error('lease lost');
      }
      if (seenKeys.has(key)) {
        throw new Error(`duplicate step key: ${key}`);
      }
      seenKeys.add(key);

      const decidedAt = clock.now();
      const recorded = await recordSleepStep(
        dbPool,
        runId,
        key,
        wakeAt,
        decidedAt
      );
      completedSteps.set(key, {
        run_id: runId,
        step_key: key,
        status: 'completed',
        output: { wake_at: recorded.wake_at.toISOString() },
        attempts: 1,
        last_error: null,
        started_at: decidedAt,
        finished_at: decidedAt,
        wake_at: recorded.wake_at
      });

      if (recorded.wake_at.getTime() <= decidedAt.getTime()) {
        return;
      }
      throw new SleepDeferredError(key, recorded.wake_at);
    }

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
        const stepSpan = startStepSpan(telemetry.tracer, runContext, key, {
          'durably.run_id': run.id,
          'durably.step_key': key,
          'durably.attempt': attempts
        });
        const stepLogger = taskLogger.child({
          step_key: key,
          attempt: attempts
        });
        metrics.stepAttempts.inc({ workflow: workflowName });
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
            finished_at: finishedAt,
            wake_at: null
          });
          stepLogger.info(
            {
              event: 'step_completed',
              duration_ms: finishedAt.getTime() - startedAt.getTime()
            },
            'step completed'
          );
          endSpan(stepSpan);
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
          metrics.stepRetries.inc({ workflow: workflowName });
          stepLogger.warn(
            {
              event: 'step_failed',
              attempt: attempts,
              max_attempts: policy.maxAttempts,
              duration_ms: finishedAt.getTime() - startedAt.getTime(),
              err: error
            },
            'step failed'
          );
          endSpan(stepSpan, error);
          throw new StepFailureError(key, attempts, policy, error);
        }
      },
      async sleep(key: string, duration: SleepDuration): Promise<void> {
        const wakeAt = new Date(
          clock.now().getTime() + parseDurationMs(duration)
        );
        await deferSleep(key, wakeAt);
      },
      async sleepUntil(
        key: string,
        target: Date | string | number
      ): Promise<void> {
        await deferSleep(key, toDate(target));
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
      observeTask('completed', workflowName, runSpan, undefined);
      return;
    } catch (error) {
      if (
        controller.signal.aborted ||
        (error instanceof Error &&
          (error.message === 'lease lost' || error.message === 'stale lease'))
      ) {
        taskLogger.warn(
          { event: 'lease_lost', task_id: task.id, run_id: run.id },
          'lease lost, leaving the task to the reaper'
        );
        observeTask('abandoned', workflowName, runSpan, error);
        return;
      }

      if (error instanceof SleepDeferredError) {
        await sleepRunAndTask(
          dbPool,
          task,
          task.lease_token,
          error.wakeAt,
          clock.now()
        );
        taskLogger.info(
          {
            event: 'run_sleeping',
            step_key: error.stepKey,
            wake_at: error.wakeAt
          },
          'run sleeping until its wake time'
        );
        observeTask('slept', workflowName, runSpan, undefined);
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
          metrics.deadLetters.inc({ reason: 'step_exhausted' });
          taskLogger.error(
            {
              event: 'dead_lettered',
              reason: 'step_exhausted',
              step_key: error.stepKey,
              attempts: error.attempt,
              err: error.cause
            },
            'dead letter written'
          );
          observeTask('dead_lettered', workflowName, runSpan, error.cause);
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
          taskLogger.warn(
            {
              event: 'task_rescheduled',
              step_key: error.stepKey,
              attempt: error.attempt,
              delay_ms: delayMs
            },
            'task rescheduled with backoff'
          );
          observeTask('retried', workflowName, runSpan, error.cause);
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
        metrics.deadLetters.inc({ reason: 'task_exhausted' });
        taskLogger.error(
          { event: 'dead_lettered', reason: 'task_exhausted', err: error },
          'dead letter written'
        );
        observeTask('dead_lettered', workflowName, runSpan, error);
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
      observeTask('retried', workflowName, runSpan, error);
      return;
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
        const claimStartedAt = process.hrtime.bigint();
        const claimed = await claimTasks(dbPool, {
          workerId,
          limit: batch,
          leaseMs,
          now: clock.now()
        });
        metrics.claimLatency.observe(
          { worker_id: workerId },
          Number(process.hrtime.bigint() - claimStartedAt) / 1e9
        );
        if (claimed.tasks.length === 0) {
          break;
        }

        for (const task of claimed.tasks) {
          inflight += 1;
          void processTask(task)
            .catch((error: unknown) => {
              logger.error(
                { err: error, run_id: task.run_id, task_id: task.id },
                'task processing failed'
              );
            })
            .then(() => {
              inflight -= 1;
              signalPump();
            });
        }
      }
    } catch (error) {
      // A claim that cannot reach the database is expected during a database
      // outage. The poll timer keeps retrying, so this must stay a warning
      // rather than an unhandled rejection.
      logger.warn(
        { err: error, event: 'claim_failed' },
        'could not claim tasks, retrying on the next poll'
      );
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
    if (duties) {
      await duties.stop();
      duties = null;
    }
    if (leader) {
      await leader.stop();
      leader = null;
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
      const client = listenerClient;
      listenerClient = null;
      try {
        await client.query('UNLISTEN *');
      } catch (error) {
        logger.warn({ event: 'listener_shutdown_failed', error: String(error) });
      } finally {
        client.release(true);
      }
    }

    if (listenerReconnectTimer) {
      clearTimeout(listenerReconnectTimer);
      listenerReconnectTimer = null;
    }

    if (listenerPool) {
      await listenerPool.end();
      listenerPool = null;
    }

    if (pool) {
      await pool.end();
      pool = null;
    }

    logger.info({ event: 'worker_stopped', worker_id: workerId });
    stopResolver?.();
    stopResolver = null;
  }

  /**
   * A checked-out pg client has no 'error' listener, so a dropped LISTEN
   * connection raises an unhandled error event. Losing LISTEN is survivable:
   * the poll timer still claims tasks, so the listener is reopened in the
   * background and the worker keeps its lease on work already claimed.
   */
  async function openListener(): Promise<void> {
    if (stopping || listenerConnecting || listenerClient !== null) {
      return;
    }
    if (listenerPool === null) {
      listenerPool = await createDatabasePool(options.databaseUrl);
    }
    listenerConnecting = true;
    let client: PoolClient | null = null;
    let released = false;
    const releaseClient = (): void => {
      if (client === null || released) {
        return;
      }
      released = true;
      client.release(true);
    };
    const onClientError = (error: Error): void => {
      logger.warn(
        { event: 'listener_connection_lost', error: String(error) },
        'lost the notification connection, polling continues'
      );
      if (listenerClient === client) {
        listenerClient = null;
      }
      releaseClient();
      scheduleListenerReconnect();
    };
    try {
      client = await listenerPool.connect();
      // The handler has to exist before the query: a connection that drops
      // during it would otherwise raise an unhandled error event.
      client.on('error', onClientError);
      await client.query('LISTEN task_ready');
      client.on('notification', signalPump);
      listenerClient = client;
    } catch (error) {
      client?.on('error', () => undefined);
      releaseClient();
      logger.warn(
        { event: 'listener_connect_failed', error: String(error) },
        'could not open the notification connection'
      );
      scheduleListenerReconnect();
    } finally {
      listenerConnecting = false;
    }
  }

  function scheduleListenerReconnect(): void {
    if (stopping || listenerReconnectTimer !== null) {
      return;
    }
    listenerReconnectTimer = setTimeout(() => {
      listenerReconnectTimer = null;
      void openListener();
    }, 250);
    listenerReconnectTimer.unref();
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
    await openListener();
    pollTimer = setInterval(() => {
      signalPump();
    }, pollIntervalMs);

    if (participateInLeaderElection) {
      leader = createLeaderElector({
        databaseUrl: options.databaseUrl,
        workerId,
        intervalMs: leaderIntervalMs,
        clock,
        logger,
        metrics
      });
      duties = createLeaderDuties({
        pool,
        workerId,
        logger,
        metrics,
        clock,
        reaperIntervalMs,
        reaperBatchSize: Math.max(50, concurrency * 4),
        stuckRunIntervalMs,
        stuckRunGraceMs,
        cronIntervalMs,
        onTaskReady: () => {
          signalPump();
          options.onTaskReady?.();
        }
      });
      leader.onChange((isLeader) => {
        if (isLeader) {
          duties?.start();
        } else {
          void duties?.stop();
        }
      });
      await leader.start();
    }

    logger.info({
      event: 'worker_started',
      worker_id: workerId,
      concurrency,
      lease_ms: leaseMs,
      workflows: [...workflows.keys()]
    });

    signalPump();
    await stopPromise;
  }

  return {
    workerId,
    start,
    stop,
    isLeader: () => leader?.isLeader() ?? false,
    inflightTasks: () => inflight
  };
}

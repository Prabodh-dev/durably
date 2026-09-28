import type { Pool } from 'pg';

import { reapExpiredTasks, repairStuckRuns } from './database.js';
import type { Logger } from './logging.js';
import type { DurablyMetrics } from './metrics.js';
import { tickSchedules } from './schedules.js';
import type { ScheduleTickResult } from './schedules.js';
import { systemClock } from './utils.js';
import type { Clock } from './types.js';

export type LeaderDutyKind =
  'lease_reaper' | 'stuck_run_detector' | 'cron_ticker';

export type LeaderDutiesOptions = {
  pool: Pool;
  workerId: string;
  logger: Logger;
  metrics: DurablyMetrics;
  clock?: Clock;
  reaperIntervalMs?: number;
  reaperBatchSize?: number;
  stuckRunIntervalMs?: number;
  stuckRunGraceMs?: number;
  stuckRunBatchSize?: number;
  cronIntervalMs?: number;
  onTaskReady?: () => void;
  onSweep?: (..._args: [LeaderDutyKind]) => void;
  onCronFire?: (..._args: [ScheduleTickResult]) => void;
};

export type LeaderDutiesHandle = {
  start(): void;
  stop(): Promise<void>;
  sweep(..._args: [LeaderDutyKind]): Promise<boolean>;
};

type Duty = () => Promise<boolean>;

export function createLeaderDuties(
  options: LeaderDutiesOptions
): LeaderDutiesHandle {
  const clock = options.clock ?? systemClock;
  const logger = options.logger.child({ component: 'leader_duties' });
  const reaperIntervalMs = options.reaperIntervalMs ?? 1000;
  const reaperBatchSize = options.reaperBatchSize ?? 50;
  const stuckRunIntervalMs = options.stuckRunIntervalMs ?? 10000;
  const stuckRunGraceMs = options.stuckRunGraceMs ?? 30000;
  const stuckRunBatchSize = options.stuckRunBatchSize ?? 50;
  const cronIntervalMs = options.cronIntervalMs ?? 1000;

  let timers: NodeJS.Timeout[] = [];
  let active = false;
  let inflight = 0;
  let drainResolvers: Array<() => void> = [];

  async function sweepLeaseReaper(): Promise<boolean> {
    const reaped = await reapExpiredTasks(
      options.pool,
      reaperBatchSize,
      clock.now()
    );
    if (reaped.requeued > 0) {
      options.metrics.leaseExpirations.inc(reaped.requeued);
      logger.info({ event: 'lease_reaper_swept', requeued: reaped.requeued });
    }
    if (reaped.deadLettered > 0) {
      options.metrics.leaseExpirations.inc(reaped.deadLettered);
      options.metrics.deadLetters.inc(
        { reason: 'lease_lost' },
        reaped.deadLettered
      );
      logger.warn({
        event: 'lease_reaper_dead_lettered',
        dead_lettered: reaped.deadLettered
      });
    }
    if (reaped.requeued > 0) {
      options.onTaskReady?.();
    }
    return reaped.requeued > 0;
  }

  async function sweepStuckRuns(): Promise<boolean> {
    const staleBefore = new Date(clock.now().getTime() - stuckRunGraceMs);
    const repaired = await repairStuckRuns(
      options.pool,
      stuckRunBatchSize,
      staleBefore
    );
    if (repaired.length > 0) {
      logger.info({ event: 'stuck_runs_repaired', run_ids: repaired });
      options.onTaskReady?.();
    }
    return repaired.length > 0;
  }

  async function sweepCronTicker(): Promise<boolean> {
    const outcomes = await tickSchedules(options.pool, clock.now());
    const fired = outcomes.filter((outcome) => outcome.firedAt !== null);
    for (const outcome of outcomes) {
      options.onCronFire?.(outcome);
      options.metrics.cronFires.inc({
        outcome: outcome.firedAt === null ? 'skipped' : 'fired'
      });
      logger.info({
        event: 'cron_tick',
        schedule_id: outcome.scheduleId,
        tenant_id: outcome.tenantId,
        fired_at: outcome.firedAt,
        missed: outcome.missed
      });
    }
    if (fired.length > 0) {
      options.onTaskReady?.();
    }
    return fired.length > 0;
  }

  const duties: Record<LeaderDutyKind, Duty> = {
    lease_reaper: sweepLeaseReaper,
    stuck_run_detector: sweepStuckRuns,
    cron_ticker: sweepCronTicker
  };

  const intervals: Record<LeaderDutyKind, number> = {
    lease_reaper: reaperIntervalMs,
    stuck_run_detector: stuckRunIntervalMs,
    cron_ticker: cronIntervalMs
  };

  function run(kind: LeaderDutyKind): void {
    if (!active) {
      return;
    }
    inflight += 1;
    options.onSweep?.(kind);
    void duties[kind]()
      .catch((error: unknown) => {
        logger.error({ err: error, duty: kind, event: 'leader_duty_failed' });
        return false;
      })
      .then(() => {
        inflight -= 1;
        if (inflight === 0) {
          for (const resolveDrain of drainResolvers) {
            resolveDrain();
          }
          drainResolvers = [];
        }
      });
  }

  async function waitForDrain(): Promise<void> {
    if (inflight === 0) {
      return;
    }
    await new Promise<void>((resolveDrain) => {
      drainResolvers.push(resolveDrain);
    });
  }

  return {
    start(): void {
      if (active) {
        return;
      }
      active = true;
      timers = (Object.keys(duties) as LeaderDutyKind[]).map((kind) => {
        const timer = setInterval(() => {
          run(kind);
        }, intervals[kind]);
        timer.unref();
        return timer;
      });
      logger.info({
        event: 'leader_duties_started',
        duties: Object.keys(duties)
      });
    },
    async stop(): Promise<void> {
      active = false;
      for (const timer of timers) {
        clearInterval(timer);
      }
      timers = [];
      await waitForDrain();
      logger.info({ event: 'leader_duties_stopped' });
    },
    async sweep(kind: LeaderDutyKind): Promise<boolean> {
      return duties[kind]();
    }
  };
}

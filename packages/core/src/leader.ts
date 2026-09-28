import { Client } from 'pg';
import { setTimeout as sleep } from 'node:timers/promises';

import type { Logger } from './logging.js';
import type { DurablyMetrics } from './metrics.js';
import { systemClock } from './utils.js';
import type { Clock } from './types.js';

export const LEADER_ADVISORY_LOCK_KEY = '4269203771';

const LEADER_CONNECT_TIMEOUT_MS = 5000;

export type LeaderElectorOptions = {
  databaseUrl: string;
  workerId: string;
  intervalMs?: number;
  clock?: Clock;
  logger: Logger;
  metrics: DurablyMetrics;
};

export type LeaderHandle = {
  readonly workerId: string;
  isLeader(): boolean;
  leaderBackendPid(): number | null;
  onChange(..._args: [(isLeader: boolean) => void]): () => void;
  start(): Promise<void>;
  stop(): Promise<void>;
};

type LeadershipListener = (isLeader: boolean) => void;

export function createLeaderElector(
  options: LeaderElectorOptions
): LeaderHandle {
  const intervalMs = options.intervalMs ?? 1000;
  const clock = options.clock ?? systemClock;
  const logger = options.logger.child({ component: 'leader' });
  const { leader: leaderGauge } = options.metrics;

  let client: Client | null = null;
  let backendPid: number | null = null;
  let leading = false;
  let stopping = false;
  let loop: Promise<void> | null = null;
  const listeners = new Set<LeadershipListener>();

  leaderGauge.set({ worker_id: options.workerId }, 0);

  function setLeadership(next: boolean): void {
    if (leading === next) {
      return;
    }
    leading = next;
    leaderGauge.set({ worker_id: options.workerId }, next ? 1 : 0);
    logger.info(
      { event: next ? 'leader_acquired' : 'leader_lost', at: clock.now() },
      next ? 'acquired leadership' : 'lost leadership'
    );
    for (const listener of listeners) {
      try {
        listener(next);
      } catch (error) {
        logger.error({ err: error, event: 'leader_listener_failed' });
      }
    }
  }

  async function endClient(target: Client): Promise<void> {
    try {
      await target.end();
    } catch (error) {
      logger.debug({ err: error, event: 'leader_client_end_failed' });
    }
  }

  async function runSession(target: Client): Promise<boolean> {
    let broken = false;
    const markBroken = (): void => {
      broken = true;
    };
    target.on('error', markBroken);
    target.on('end', markBroken);

    try {
      await target.connect();
    } catch (error) {
      logger.warn({ err: error, event: 'leader_connect_failed' });
      return false;
    }
    while (!stopping && !broken) {
      try {
        const lockResult = await target.query<{ locked: boolean; pid: number }>(
          'SELECT pg_try_advisory_lock($1) AS locked, pg_backend_pid()::int AS pid',
          [LEADER_ADVISORY_LOCK_KEY]
        );
        backendPid = lockResult.rows[0]?.pid ?? null;
        setLeadership(lockResult.rows[0]?.locked === true);
      } catch (error) {
        logger.warn({ err: error, event: 'leader_lock_query_failed' });
        break;
      }

      if (stopping || broken) {
        break;
      }

      try {
        await target.query('SELECT 1');
      } catch (error) {
        logger.debug({ err: error, event: 'leader_keepalive_failed' });
        break;
      }

      await sleep(intervalMs);
    }

    return broken;
  }

  async function electionLoop(): Promise<void> {
    while (!stopping) {
      const target = new Client({
        connectionString: options.databaseUrl,
        // Without a bound a proxy that accepts the socket and never answers
        // parks the elector forever, so no reaper or cron tick ever runs again.
        connectionTimeoutMillis: LEADER_CONNECT_TIMEOUT_MS
      });
      client = target;
      await runSession(target);
      setLeadership(false);
      backendPid = null;
      logger.warn({ event: 'leader_session_end' });
      await endClient(target);
      logger.warn({ event: 'leader_client_end', stopping });
      if (client === target) {
        client = null;
      }
      if (!stopping) {
        // Without this pause a database that is unreachable turns election into
        // a hot loop of failed connections.
        await sleep(intervalMs);
      }
    }
  }

  return {
    workerId: options.workerId,
    isLeader: () => leading,
    leaderBackendPid: () => backendPid,
    onChange(listener: LeadershipListener): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    async start(): Promise<void> {
      loop = electionLoop();
    },
    async stop(): Promise<void> {
      stopping = true;
      setLeadership(false);
      const active = client;
      if (active) {
        await endClient(active);
      }
      if (loop) {
        await loop.catch(() => undefined);
        loop = null;
      }
      client = null;
    }
  };
}

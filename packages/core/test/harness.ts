import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import type { ChildProcessWithoutNullStreams } from 'node:child_process';

import { GenericContainer, Wait } from 'testcontainers';
import type { StartedTestContainer } from 'testcontainers';

import {
  createDatabasePool,
  createLogger,
  createRun,
  getRunWithSteps,
  runMigrations
} from '@durably/core';
import type { Clock, Logger, Pool } from '@durably/core';

const migrationsDir = resolve(process.cwd(), 'migrations');
const workerMain = resolve(process.cwd(), 'packages/worker/dist/main.js');

export type PostgresFixture = {
  container: StartedTestContainer;
  pool: Pool;
  databaseUrl: string;
  host: string;
  port: number;
};

export async function startPostgres(): Promise<PostgresFixture> {
  // The suite starts several containers at once and Docker occasionally stalls
  // one of them. A single retry keeps that infrastructure noise out of results.
  let lastError: unknown;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      return await launchPostgres();
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

async function launchPostgres(): Promise<PostgresFixture> {
  const container = await new GenericContainer('postgres:16-alpine')
    .withEnvironment({
      POSTGRES_DB: 'durably',
      POSTGRES_USER: 'durably',
      POSTGRES_PASSWORD: 'durably'
    })
    .withExposedPorts(5432)
    .withWaitStrategy(
      Wait.forLogMessage('database system is ready to accept connections')
    )
    .start();

  const host = container.getHost();
  const port = container.getMappedPort(5432);
  const databaseUrl = `postgres://durably:durably@${host}:${port}/durably`;
  const pool = await createDatabasePool(databaseUrl);
  await runMigrationsWithRetry(pool, migrationsDir);

  return { container, pool, databaseUrl, host, port };
}

export type ToxiproxyFixture = {
  container: StartedTestContainer;
  databaseUrl: string;
  cutConnection(enabled: boolean): Promise<void>;
  setLatency(latencyMs: number): Promise<void>;
  stop(): Promise<void>;
};

export async function startToxiproxy(
  fixture: PostgresFixture,
  listenPort: number
): Promise<ToxiproxyFixture> {
  const container = await new GenericContainer(
    'ghcr.io/shopify/toxiproxy:2.9.0'
  )
    .withExposedPorts(8474, listenPort)
    .withWaitStrategy(Wait.forHttp('/version', 8474).forStatusCode(200))
    .start();

  const controlUrl = `http://${container.getHost()}:${container.getMappedPort(8474)}`;
  const listenHostPort = container.getMappedPort(listenPort);
  const proxyName = `durably-${listenPort}`;

  await fetch(`${controlUrl}/proxies`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      name: proxyName,
      listen: `0.0.0.0:${listenPort}`,
      upstream: `host.docker.internal:${fixture.port}`,
      enabled: true
    })
  });

  const databaseUrl = `postgres://durably:durably@${container.getHost()}:${listenHostPort}/durably`;
  const toxicPath = `${controlUrl}/proxies/${proxyName}/toxics`;

  const removeToxic = async (name: string): Promise<void> => {
    const response = await fetch(`${toxicPath}/${encodeURIComponent(name)}`, {
      method: 'DELETE'
    });
    if (!response.ok && response.status !== 404) {
      throw new Error(
        `failed to remove the ${name} toxic: ${await response.text()}`
      );
    }
  };

  const addToxic = async (toxic: {
    name: string;
    type: string;
    stream: string;
    toxicity: number;
    attributes: Record<string, number>;
  }): Promise<void> => {
    const response = await fetch(toxicPath, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(toxic)
    });
    if (!response.ok) {
      throw new Error(`failed to add ${toxic.name}: ${await response.text()}`);
    }
  };

  const removeLatency = async (): Promise<void> => {
    await removeToxic('chaos-latency');
  };

  return {
    container,
    databaseUrl,
    async cutConnection(enabled: boolean): Promise<void> {
      if (!enabled) {
        await removeToxic('chaos-cut');
        return;
      }
      await addToxic({
        name: 'chaos-cut',
        type: 'timeout',
        stream: 'downstream',
        toxicity: 1,
        attributes: { timeout: 0 }
      });
    },
    async setLatency(latencyMs: number): Promise<void> {
      await removeLatency();
      if (latencyMs <= 0) {
        return;
      }
      const response = await fetch(toxicPath, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          name: 'chaos-latency',
          type: 'latency',
          stream: 'downstream',
          toxicity: 1,
          attributes: { latency: latencyMs, jitter: 0 }
        })
      });
      if (!response.ok) {
        throw new Error(`failed to add latency: ${await response.text()}`);
      }
    },
    async stop(): Promise<void> {
      await container.stop();
    }
  };
}

async function runMigrationsWithRetry(
  pool: Pool,
  targetDir: string
): Promise<void> {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    try {
      await pool.query('SELECT 1');
      await runMigrations(pool, targetDir);
      return;
    } catch (error) {
      if (attempt === 9) {
        throw error;
      }
      await sleep(500);
    }
  }
}

export async function stopPostgres(fixture: PostgresFixture): Promise<void> {
  await fixture.pool.end();
  await fixture.container.stop();
}

export async function truncateAll(pool: Pool): Promise<void> {
  await pool.query(
    'TRUNCATE TABLE schedules, dead_letters, example_side_effects, steps, tasks, runs RESTART IDENTITY CASCADE'
  );
  await pool.query("DELETE FROM api_keys WHERE tenant_id <> 'default'");
  await pool.query("DELETE FROM tenants WHERE id <> 'default'");
  await pool.query(
    "UPDATE tenants SET last_claim_at = NULL WHERE id = 'default'"
  );
}

export type TestClock = Clock & {
  set(next: Date): void;
  advance(ms: number): void;
};

export function createTestClock(start: Date): TestClock {
  let current = new Date(start.getTime());
  return {
    now: () => new Date(current.getTime()),
    set(next: Date): void {
      current = new Date(next.getTime());
    },
    advance(ms: number): void {
      current = new Date(current.getTime() + ms);
    }
  };
}

export async function waitForCondition<T>(
  callback: () => Promise<T | undefined | null>,
  predicate: (value: T) => boolean,
  timeoutMs = 30000,
  intervalMs = 50
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = null;
  while (Date.now() < deadline) {
    try {
      const value = await callback();
      if (value !== undefined && value !== null && predicate(value)) {
        return value;
      }
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }
  throw new Error(
    `condition timed out after ${timeoutMs}ms${lastError === null ? '' : `: ${String(lastError)}`}`
  );
}

export async function waitForProcessExit(
  child: ChildProcessWithoutNullStreams,
  timeoutMs = 20000
): Promise<number | null> {
  return await new Promise<number | null>((resolvePromise, rejectPromise) => {
    const timeout = setTimeout(() => {
      rejectPromise(new Error('process did not exit in time'));
    }, timeoutMs);
    child.once('exit', (code) => {
      clearTimeout(timeout);
      resolvePromise(code);
    });
  });
}

export type LeaderEvent = { at: number; event: string; workerId: string };

export type WorkerProcess = {
  child: ChildProcessWithoutNullStreams;
  workerId: string;
  leaderEvents: LeaderEvent[];
  exited(): boolean;
  waitForExit(): Promise<number | null>;
  recentOutput(): string;
};

const OUTPUT_BUFFER_LINES = 400;

export type SpawnWorkerOptions = {
  databaseUrl: string;
  workerId: string;
  env?: Record<string, string>;
  captureOutput?: boolean;
};

function assertWorkerBuild(): void {
  if (!existsSync(workerMain)) {
    throw new Error(`worker build output not found at ${workerMain}`);
  }
}

export function spawnWorkerProcess(options: SpawnWorkerOptions): WorkerProcess {
  assertWorkerBuild();

  const child = spawn(process.execPath, [workerMain], {
    env: {
      ...process.env,
      DATABASE_URL: options.databaseUrl,
      DURABLY_WORKER_ID: options.workerId,
      ONBOARD_USER_FAILURE_RATE: '0',
      ...options.env
    },
    stdio:
      options.captureOutput === false ? 'ignore' : ['ignore', 'pipe', 'pipe']
  });

  const leaderEvents: LeaderEvent[] = [];
  const outputLines: string[] = [];
  const record = (chunk: Buffer | string): void => {
    for (const line of chunk.toString().split(/\r?\n/)) {
      if (line.trim().length > 0) {
        outputLines.push(line);
      }
      if (outputLines.length > OUTPUT_BUFFER_LINES) {
        outputLines.shift();
      }
      for (const event of ['leader_acquired', 'leader_lost']) {
        if (line.includes(`"${event}"`)) {
          leaderEvents.push({
            at: Date.now(),
            event,
            workerId: options.workerId
          });
        }
      }
    }
  };

  child.stdout?.on('data', record);
  child.stderr?.on('data', record);

  let exitPromise: Promise<number | null> | null = null;
  let hasExited = false;
  let exitCode: number | null = null;
  child.once('exit', (code) => {
    hasExited = true;
    exitCode = code;
  });

  return {
    child,
    workerId: options.workerId,
    leaderEvents,
    exited: () => hasExited,
    recentOutput: () =>
      `worker ${options.workerId} output:\n${outputLines.join('\n')}`,
    waitForExit(): Promise<number | null> {
      if (!exitPromise) {
        exitPromise = new Promise<number | null>((resolvePromise) => {
          if (hasExited) {
            resolvePromise(exitCode);
            return;
          }
          child.once('exit', (code) => {
            resolvePromise(code);
          });
        });
      }
      return exitPromise;
    }
  };
}

export async function killWorkerProcess(worker: WorkerProcess): Promise<void> {
  if (!worker.exited()) {
    worker.child.kill('SIGKILL');
  }
  await worker.waitForExit();
}

export async function stopWorkerProcess(worker: WorkerProcess): Promise<void> {
  if (!worker.exited()) {
    worker.child.kill('SIGTERM');
  }
  await worker.waitForExit();
}

export function createSilentLogger(
  bindings: Record<string, string> = {}
): Logger {
  return createLogger(bindings, { level: 'silent' });
}

export { sleep, migrationsDir, workerMain };

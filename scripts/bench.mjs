import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { GenericContainer, Wait } from 'testcontainers';

import { createDatabasePool, createRun, runMigrations } from '@durably/core';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const workerEntry = join(repoRoot, 'packages', 'worker', 'dist', 'main.js');
const migrationsDir = join(repoRoot, 'migrations');

const runs = Number(process.env.BENCH_RUNS ?? 500);
const workerCount = Number(process.env.BENCH_WORKERS ?? 2);
const concurrency = Number(process.env.BENCH_CONCURRENCY ?? 8);
const metricsPortBase = Number(process.env.BENCH_METRICS_PORT ?? 9401);
const timeoutMs = Number(process.env.BENCH_TIMEOUT_MS ?? 600000);

function percentile(sorted, fraction) {
  if (sorted.length === 0) {
    return 0;
  }
  const index = Math.min(
    sorted.length - 1,
    Math.ceil(fraction * sorted.length) - 1
  );
  return sorted[Math.max(0, index)];
}

function summarize(name, values) {
  const sorted = [...values].sort((left, right) => left - right);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    name,
    count: sorted.length,
    meanMs:
      sorted.length === 0 ? 0 : Number((total / sorted.length).toFixed(2)),
    p50Ms: Number(percentile(sorted, 0.5).toFixed(2)),
    p95Ms: Number(percentile(sorted, 0.95).toFixed(2)),
    p99Ms: Number(percentile(sorted, 0.99).toFixed(2)),
    maxMs: Number((sorted[sorted.length - 1] ?? 0).toFixed(2))
  };
}

async function migrate(pool, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  for (;;) {
    try {
      await runMigrations(pool, migrationsDir);
      return;
    } catch (error) {
      if (Date.now() > deadline) {
        throw error;
      }
      await sleep(200);
    }
  }
}

async function waitForRuns(pool, expected, deadlineMs) {
  const deadline = Date.now() + deadlineMs;
  for (;;) {
    const result = await pool.query(
      `SELECT count(*)::int AS finished
       FROM runs
       WHERE status IN ('completed', 'failed', 'cancelled')`
    );
    const finished = result.rows[0].finished;
    if (finished >= expected) {
      return finished;
    }
    if (Date.now() > deadline) {
      throw new Error(`only ${finished} of ${expected} runs finished in time`);
    }
    await sleep(200);
  }
}

async function fetchMetrics(port) {
  try {
    const response = await fetch(`http://127.0.0.1:${port}/metrics`);
    if (!response.ok) {
      return '';
    }
    return await response.text();
  } catch {
    return '';
  }
}

function parseHistogram(text, name) {
  const pattern = new RegExp(
    `^${name}_bucket\\{[^}]*\\}\\s+([0-9.eE+-]+)$`,
    'gm'
  );
  const labels = new RegExp(`^${name}_bucket\\{([^}]*)\\}`, 'gm');
  const values = [...text.matchAll(pattern)].map((match) => Number(match[1]));
  if (values.length === 0) {
    return [];
  }
  const bucketBounds = [...text.matchAll(labels)]
    .map((match) => {
      const le = /le="([^"]+)"/.exec(match[1]);
      return le === null ? null : Number(le[1]);
    })
    .filter((value) => value !== null);
  return bucketBounds.map((le, index) => ({ le, count: values[index] }));
}

function histogramQuantile(buckets, fraction) {
  const total = buckets.at(-1)?.count ?? 0;
  if (total === 0) {
    return 0;
  }
  const target = total * fraction;
  for (const bucket of buckets) {
    if (bucket.count >= target) {
      return Number(bucket.le);
    }
  }
  return Number(buckets.at(-1)?.le ?? 0);
}

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

const databaseUrl = `postgres://durably:durably@${container.getHost()}:${container.getMappedPort(5432)}/durably`;
const pool = await createDatabasePool(databaseUrl);
await migrate(pool, 30000);

const children = [];
for (let index = 0; index < workerCount; index += 1) {
  const child = spawn(process.execPath, [workerEntry], {
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      DURABLY_WORKER_ID: `bench-${index}`,
      DURABLY_WORKER_CONCURRENCY: String(concurrency),
      DURABLY_WORKER_METRICS_PORT: String(metricsPortBase + index),
      DURABLY_WORKER_LEASE_MS: '15000',
      LOG_LEVEL: 'warn'
    },
    stdio: ['ignore', 'inherit', 'inherit']
  });
  children.push(child);
}

await sleep(3000);

const insertStartedAt = process.hrtime.bigint();
const created = [];
for (let index = 0; index < runs; index += 1) {
  created.push(await createRun(pool, { workflow: 'bench', input: {} }));
}
const insertMs = Number(process.hrtime.bigint() - insertStartedAt) / 1e6;

const drainStartedAt = process.hrtime.bigint();
const finished = await waitForRuns(pool, runs, timeoutMs);
const drainMs = Number(process.hrtime.bigint() - drainStartedAt) / 1e6;

const metricsText = (
  await Promise.all(
    children.map((_, index) => fetchMetrics(metricsPortBase + index))
  )
).join('\n');

const runLatencies = await pool.query(
  `SELECT (extract(epoch FROM (completed_at - created_at)) * 1000)::float8 AS latency_ms
   FROM runs
   WHERE status = 'completed'`
);
const stepLatencies = await pool.query(
  `SELECT (extract(epoch FROM (finished_at - started_at)) * 1000)::float8 AS latency_ms
   FROM steps
   WHERE status = 'completed'`
);
const stepCount = stepLatencies.rows.length;
const statuses = await pool.query(
  'SELECT status, count(*)::int AS count FROM runs GROUP BY status'
);

const claimBuckets = parseHistogram(
  metricsText,
  'durably_claim_latency_seconds'
);
const report = {
  measuredAt: new Date().toISOString(),
  configuration: {
    workers: workerCount,
    concurrencyPerWorker: concurrency,
    totalConcurrency: workerCount * concurrency,
    runs,
    stepsPerRun: 5,
    stepWorkMs: 20
  },
  throughput: {
    insertMs: Number(insertMs.toFixed(2)),
    drainMs: Number(drainMs.toFixed(2)),
    runsPerSecond: Number((finished / (drainMs / 1000)).toFixed(1)),
    stepsPerSecond: Number((stepCount / (drainMs / 1000)).toFixed(1)),
    stepExecutionsPerSecond: Number(
      (stepCount / (drainMs / 1000) / 5).toFixed(1)
    )
  },
  runLatency: summarize(
    'run created to completed',
    runLatencies.rows.map((row) => row.latency_ms)
  ),
  stepLatency: summarize(
    'step started to finished',
    stepLatencies.rows.map((row) => row.latency_ms)
  ),
  claimLatencySeconds: {
    p50: histogramQuantile(claimBuckets, 0.5),
    p95: histogramQuantile(claimBuckets, 0.95),
    p99: histogramQuantile(claimBuckets, 0.99)
  },
  statuses: Object.fromEntries(
    statuses.rows.map((row) => [row.status, row.count])
  )
};

console.log(JSON.stringify(report, null, 2));

for (const child of children) {
  child.kill('SIGKILL');
}
await pool.end();
await container.stop();

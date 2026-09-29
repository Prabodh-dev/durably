import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { GenericContainer, Wait } from 'testcontainers';

import {
  claimTasks,
  createDatabasePool,
  createRun,
  createTenant,
  runMigrations
} from '@durably/core';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const migrationsDir = join(repoRoot, 'migrations');

const tenants = Number(process.env.QUEUE_TENANTS ?? 4);
const tasksPerTenant = Number(process.env.QUEUE_TASKS_PER_TENANT ?? 250);
const claimers = Number(process.env.QUEUE_CLAIMERS ?? 8);
const batchSize = Number(process.env.QUEUE_BATCH ?? 10);
const leaseMs = Number(process.env.QUEUE_LEASE_MS ?? 15000);
const runTimeoutMs = Number(process.env.QUEUE_TIMEOUT_MS ?? 300000);

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

const tenantIds = [];
for (let index = 0; index < tenants; index += 1) {
  const id = `queue-tenant-${index}`;
  await createTenant(pool, { id, name: id });
  tenantIds.push(id);
}

const runIds = [];
for (const tenantId of tenantIds) {
  const ids = [];
  for (let index = 0; index < tasksPerTenant; index += 1) {
    const run = await createRun(pool, {
      tenantId,
      workflow: 'bench',
      input: {}
    });
    ids.push(run.id);
  }
  runIds.push(ids);
}

const claimLatencies = [];
const claimsByTenant = Object.fromEntries(tenantIds.map((id) => [id, 0]));
const claimsByWorker = {};
let duplicates = 0;
const seenTaskIds = new Set();
let stopped = false;

async function claimer(workerId) {
  const claimerPool = await createDatabasePool(databaseUrl);
  let workerClaims = 0;
  while (!stopped) {
    const startedAt = process.hrtime.bigint();
    let claimed;
    try {
      claimed = await claimTasks(claimerPool, {
        workerId,
        limit: batchSize,
        leaseMs
      });
    } catch (error) {
      if (Date.now() - startedAt > 0) {
        process.stderr.write(`${workerId} claim failed: ${String(error)}\n`);
      }
      continue;
    }
    claimLatencies.push(Number(process.hrtime.bigint() - startedAt) / 1e6);
    if (claimed.tasks.length === 0) {
      await sleep(5);
      continue;
    }
    for (const task of claimed.tasks) {
      if (seenTaskIds.has(task.id)) {
        duplicates += 1;
        continue;
      }
      seenTaskIds.add(task.id);
      workerClaims += 1;
      claimsByTenant[task.tenant_id] =
        (claimsByTenant[task.tenant_id] ?? 0) + 1;
    }
  }
  claimsByWorker[workerId] = workerClaims;
  await claimerPool.end();
}

const startedAt = process.hrtime.bigint();
const workers = [];
for (let index = 0; index < claimers; index += 1) {
  workers.push(claimer(`queue-claimer-${index}`));
}

const expected = tenants * tasksPerTenant;
const deadline = Date.now() + runTimeoutMs;
let drained = 0;
while (drained < expected && Date.now() < deadline) {
  const result = await pool.query(
    "SELECT count(*)::int AS count FROM tasks WHERE status = 'ready'"
  );
  drained = expected - result.rows[0].count;
  if (drained < expected) {
    await sleep(50);
  }
}
const drainMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
stopped = true;
await Promise.all(workers);

const sortedLatencies = [...claimLatencies].sort((left, right) => left - right);
const shares = Object.values(claimsByTenant).map((count) => count / expected);
const report = {
  measuredAt: new Date().toISOString(),
  configuration: {
    tenants,
    tasksPerTenant,
    tasks: expected,
    claimers,
    batchSize,
    leaseMs
  },
  results: {
    drainMs: Number(drainMs.toFixed(2)),
    claimsPerSecond: Number((expected / (drainMs / 1000)).toFixed(1)),
    uniqueTasksClaimed: seenTaskIds.size,
    duplicateClaims: duplicates,
    drainCompleted: drained >= expected
  },
  claimLatencyMs: {
    samples: sortedLatencies.length,
    p50: Number(percentile(sortedLatencies, 0.5).toFixed(2)),
    p95: Number(percentile(sortedLatencies, 0.95).toFixed(2)),
    p99: Number(percentile(sortedLatencies, 0.99).toFixed(2)),
    max: Number((sortedLatencies.at(-1) ?? 0).toFixed(2))
  },
  fairness: {
    sharePerTenant: Object.fromEntries(
      Object.entries(claimsByTenant).map(([tenant, count]) => [
        tenant,
        Number((count / expected).toFixed(4))
      ])
    ),
    minShare: Number(Math.min(...shares).toFixed(4)),
    maxShare: Number(Math.max(...shares).toFixed(4)),
    spread: Number((Math.max(...shares) - Math.min(...shares)).toFixed(4))
  },
  loadPerClaimer: claimsByWorker
};

console.log(JSON.stringify(report, null, 2));

await pool.end();
await container.stop();

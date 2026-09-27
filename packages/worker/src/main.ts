import { createWorker, onboardUserWorkflow } from './index.js';
import type { WorkflowDefinition } from '@durably/core';

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

const workerId = process.env.DURABLY_WORKER_ID;

const worker = createWorker({
  databaseUrl,
  workflows: [
    onboardUserWorkflow as unknown as WorkflowDefinition<unknown, unknown>
  ],
  concurrency: Number(process.env.DURABLY_WORKER_CONCURRENCY ?? '4'),
  leaseMs: Number(process.env.DURABLY_WORKER_LEASE_MS ?? '15000'),
  ...(process.env.DURABLY_WORKER_REAPER_INTERVAL_MS
    ? {
        reaperIntervalMs: Number(process.env.DURABLY_WORKER_REAPER_INTERVAL_MS)
      }
    : {}),
  ...(process.env.DURABLY_WORKER_POLL_INTERVAL_MS
    ? { pollIntervalMs: Number(process.env.DURABLY_WORKER_POLL_INTERVAL_MS) }
    : {}),
  ...(workerId ? { workerId } : {})
});

const shutdown = async () => {
  await worker.stop();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

await worker.start();

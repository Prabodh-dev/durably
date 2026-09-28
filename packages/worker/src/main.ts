import {
  benchWorkflow,
  chaosWorkflow,
  createWorker,
  delayedApprovalWorkflow,
  onboardUserWorkflow
} from './index.js';
import {
  createDatabasePool,
  createLogger,
  createMetrics,
  startMetricsServer,
  startTelemetry
} from '@durably/core';
import type { MetricsServer, WorkflowDefinition } from '@durably/core';

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

const workerId = process.env.DURABLY_WORKER_ID;
const logger = createLogger({ component: 'worker' }, {});
if (workerId) {
  logger.info({ worker_id: workerId }, 'worker starting');
}

const metrics = createMetrics({ pool: await createDatabasePool(databaseUrl) });
const telemetry = startTelemetry({ serviceName: 'durably-worker' });
if (telemetry.enabled) {
  logger.info({ event: 'telemetry_enabled' }, 'otel exporter configured');
}
const configuredMetricsPort = Number(
  process.env.DURABLY_WORKER_METRICS_PORT ?? '0'
);
let metricsServer: MetricsServer | null = null;
if (configuredMetricsPort > 0) {
  metricsServer = await startMetricsServer({
    registry: metrics.registry,
    port: configuredMetricsPort
  });
}

const worker = createWorker({
  databaseUrl,
  workflows: [
    onboardUserWorkflow as unknown as WorkflowDefinition<unknown, unknown>,
    delayedApprovalWorkflow as unknown as WorkflowDefinition<unknown, unknown>,
    chaosWorkflow as unknown as WorkflowDefinition<unknown, unknown>,
    benchWorkflow as unknown as WorkflowDefinition<unknown, unknown>
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
  ...(process.env.DURABLY_WORKER_LEADER_INTERVAL_MS
    ? {
        leaderIntervalMs: Number(process.env.DURABLY_WORKER_LEADER_INTERVAL_MS)
      }
    : {}),
  ...(process.env.DURABLY_WORKER_STUCK_RUN_INTERVAL_MS
    ? {
        stuckRunIntervalMs: Number(
          process.env.DURABLY_WORKER_STUCK_RUN_INTERVAL_MS
        )
      }
    : {}),
  ...(process.env.DURABLY_WORKER_CRON_INTERVAL_MS
    ? { cronIntervalMs: Number(process.env.DURABLY_WORKER_CRON_INTERVAL_MS) }
    : {}),
  ...(process.env.DURABLY_WORKER_LEADER === 'false' ? { leader: false } : {}),
  metrics,
  telemetry,
  ...(workerId ? { workerId } : {})
});

const shutdown = async () => {
  await worker.stop();
  await telemetry.shutdown();
  if (metricsServer) {
    await metricsServer.close();
  }
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

await worker.start();

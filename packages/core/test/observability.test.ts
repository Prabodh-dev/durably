import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test
} from 'vitest';

import {
  createMetrics,
  createRunTraceparent,
  createRun,
  createTenant,
  extractTraceContext,
  getRunWithSteps,
  isTelemetryEnabled,
  startTelemetry
} from '@durably/core';
import type { WorkflowDefinition } from '@durably/core';
import { createServer } from '@durably/server';
import { createWorker, defineWorkflow } from '@durably/sdk';

import {
  createSilentLogger,
  startPostgres,
  stopPostgres,
  truncateAll,
  waitForCondition
} from './harness.js';
import type { PostgresFixture } from './harness.js';

let fixture: PostgresFixture;

beforeAll(async () => {
  fixture = await startPostgres();
});

beforeEach(async () => {
  await truncateAll(fixture.pool);
});

afterAll(async () => {
  await stopPostgres(fixture);
});

async function readMetrics(): Promise<string> {
  const app = await createServer({
    databaseUrl: fixture.databaseUrl,
    metrics: createMetrics({ pool: fixture.pool })
  });
  await app.ready();
  try {
    const response = await app.inject({ method: 'GET', url: '/metrics' });
    expect(response.statusCode).toBe(200);
    return response.body;
  } finally {
    await app.close();
  }
}

function value(body: string, name: string, labels: string): number | null {
  const pattern = new RegExp(`^${name}\\{${labels}\\} ([0-9.e+-]+)$`, 'm');
  const match = pattern.exec(body);
  return match ? Number(match[1]) : null;
}

describe.sequential('prometheus metrics', () => {
  test('queue depth and running tasks per tenant are scraped from the database', async () => {
    await createTenant(fixture.pool, { id: 'metrics-tenant', name: 'Metrics' });
    await createRun(fixture.pool, {
      tenantId: 'metrics-tenant',
      workflow: 'onboard-user',
      input: { email: 'metrics@example.com' }
    });

    const body = await readMetrics();
    expect(value(body, 'durably_queue_depth', 'status="ready"')).toBe(1);
    expect(value(body, 'durably_queue_depth', 'status="leased"')).toBe(0);
    expect(value(body, 'durably_queue_depth', 'status="done"')).toBe(0);
  });

  test('the worker metric set exposes every documented series', async () => {
    const metrics = createMetrics({ pool: fixture.pool });
    metrics.leader.set({ worker_id: 'w1' }, 1);
    metrics.claimLatency.observe({ worker_id: 'w1' }, 0.004);
    metrics.taskDuration.observe(
      { workflow: 'onboard-user', outcome: 'completed' },
      0.4
    );
    metrics.stepAttempts.inc({ workflow: 'onboard-user' });
    metrics.stepRetries.inc({ workflow: 'onboard-user' });
    metrics.deadLetters.inc({ reason: 'step_exhausted' });
    metrics.leaseExpirations.inc();
    metrics.cronFires.inc({ outcome: 'fired' });
    metrics.reaperSweep.set({ worker_id: 'w1' }, Date.now() / 1000);

    const body = await metrics.registry.metrics();
    for (const series of [
      'durably_leader',
      'durably_leader_sweep_timestamp_seconds',
      'durably_claim_latency_seconds_count',
      'durably_task_duration_seconds_count',
      'durably_step_attempts_total',
      'durably_step_retries_total',
      'durably_dead_letters_total',
      'durably_lease_expirations_total',
      'durably_queue_depth',
      'durably_running_tasks',
      'durably_cron_ticks_total',
      'durably_process_process_cpu_user_seconds_total'
    ]) {
      expect(body).toContain(series);
    }
  });

  test('metrics are served without an api key', async () => {
    const app = await createServer({
      databaseUrl: fixture.databaseUrl,
      requireApiKey: true,
      metrics: createMetrics({ pool: fixture.pool })
    });
    await app.ready();
    try {
      expect(
        (await app.inject({ method: 'GET', url: '/metrics' })).statusCode
      ).toBe(200);
      expect(
        (await app.inject({ method: 'GET', url: '/healthz' })).statusCode
      ).toBe(200);
      expect(
        (await app.inject({ method: 'GET', url: '/v1/runs' })).statusCode
      ).toBe(401);
    } finally {
      await app.close();
    }
  });
});

describe.sequential('run tracing', () => {
  test('telemetry is disabled when no otlp endpoint is configured', () => {
    const previous = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    try {
      expect(isTelemetryEnabled({ serviceName: 'test' })).toBe(false);
      const handle = startTelemetry({ serviceName: 'test' });
      expect(handle.enabled).toBe(false);
      expect(handle.tracer).toBeNull();
    } finally {
      if (previous !== undefined) {
        process.env.OTEL_EXPORTER_OTLP_ENDPOINT = previous;
      }
    }
  });

  test('telemetry is enabled when an otlp endpoint is set', () => {
    const previous = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4318/v1/traces';
    try {
      expect(isTelemetryEnabled({ serviceName: 'test' })).toBe(true);
    } finally {
      if (previous === undefined) {
        delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
      } else {
        process.env.OTEL_EXPORTER_OTLP_ENDPOINT = previous;
      }
    }
  });

  test('a generated traceparent is a valid w3c header with a distinct trace id', () => {
    const first = createRunTraceparent();
    const second = createRunTraceparent();
    expect(first).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    expect(first).not.toBe(second);
  });

  test('a traceparent stored on a run keeps one trace id across attempts and workers', async () => {
    const { ensureRunTraceparent } = await import('@durably/core');
    const { trace } = await import('@opentelemetry/api');

    const run = await createRun(fixture.pool, {
      workflow: 'onboard-user',
      input: { email: 'trace@example.com' }
    });

    const traceparent = await ensureRunTraceparent(
      fixture.pool,
      run.id,
      createRunTraceparent
    );
    expect(traceparent).not.toBeNull();

    const secondAttempt = await ensureRunTraceparent(
      fixture.pool,
      run.id,
      createRunTraceparent
    );
    expect(secondAttempt).toBe(traceparent);

    const stored = await getRunWithSteps(fixture.pool, run.id);
    expect(stored.run?.traceparent).toBe(traceparent);

    const spanContext = trace.getSpanContext(
      extractTraceContext(traceparent as string)
    );
    expect(spanContext).toBeDefined();
    expect(spanContext?.traceId).toBe((traceparent as string).split('-')[1]);
    expect(spanContext?.isRemote).toBe(true);
  });

  test('a real run emits one run span and one span per step under one trace id', async () => {
    const { InMemorySpanExporter, SimpleSpanProcessor } =
      await import('@opentelemetry/sdk-trace-base');
    const { NodeTracerProvider } =
      await import('@opentelemetry/sdk-trace-node');
    const { createWorker } = await import('@durably/sdk');

    const exporter = new InMemorySpanExporter();
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)]
    });

    const traced = defineWorkflow<{ value: number }, number>(
      { id: 'traced' },
      async ({ input, step }) => {
        const first = await step.run('double', async () => input.value * 2);
        return step.run('increment', async () => first + 1);
      }
    );

    const worker = createWorker({
      databaseUrl: fixture.databaseUrl,
      workflows: [traced as unknown as WorkflowDefinition<unknown, unknown>],
      concurrency: 2,
      leaseMs: 10_000,
      pollIntervalMs: 25,
      metrics: createMetrics(),
      logger: createSilentLogger({ worker_id: 'trace-worker' }),
      telemetry: {
        enabled: true,
        tracer: provider.getTracer('test'),
        shutdown: async () => {
          await provider.shutdown();
        }
      }
    });

    void worker.start().catch(() => undefined);
    try {
      const run = await createRun(fixture.pool, {
        workflow: 'traced',
        input: { value: 21 }
      });

      await waitForCondition(
        async () => {
          const current = await getRunWithSteps(fixture.pool, run.id);
          return current.run?.status === 'completed' ? 'completed' : undefined;
        },
        (value) => value === 'completed',
        30000,
        50
      );

      const stored = await getRunWithSteps(fixture.pool, run.id);
      expect(stored.run?.output).toBe(43);
      const traceId = (stored.run?.traceparent as string).split('-')[1];

      const spans = exporter.getFinishedSpans();
      const runSpans = spans.filter((span) => span.name === 'durably.run');
      const stepSpans = spans.filter((span) =>
        span.name.startsWith('durably.step ')
      );

      expect(runSpans).toHaveLength(1);
      expect(stepSpans.map((span) => span.name).sort()).toEqual([
        'durably.step double',
        'durably.step increment'
      ]);
      for (const span of spans) {
        expect(span.spanContext().traceId).toBe(traceId);
      }
      expect(
        stepSpans.every(
          (span) =>
            span.parentSpanContext?.spanId === runSpans[0]?.spanContext().spanId
        )
      ).toBe(true);
    } finally {
      await worker.stop();
    }
  });
});

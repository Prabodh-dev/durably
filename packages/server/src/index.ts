import Fastify, { type FastifyInstance } from 'fastify';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';

import {
  cancelRun,
  createDatabasePool,
  createRun,
  getRunWithSteps,
  listDeadLetters,
  listRuns,
  replayDeadLetter,
  runMigrations
} from '@durably/core';

export type ServerOptions = {
  databaseUrl: string;
  migrationsDir?: string;
  logger?: boolean;
};

function errorShape(
  code: string,
  message: string,
  details?: unknown
): { error: { code: string; message: string; details?: unknown } } {
  return {
    error: {
      code,
      message,
      details
    }
  };
}

function parseJsonNumber(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export async function createServer(
  options: ServerOptions
): Promise<FastifyInstance> {
  const app = Fastify({ logger: options.logger ?? false });
  const pool = await createDatabasePool(options.databaseUrl);
  await runMigrations(
    pool,
    options.migrationsDir ??
      fileURLToPath(new URL('../../../migrations', import.meta.url))
  );

  app.setErrorHandler((error, _request, reply) => {
    const statusCode = (error as { statusCode?: number }).statusCode ?? 500;
    const message = error instanceof Error ? error.message : 'unexpected error';
    reply.status(statusCode).send(errorShape('request_failed', message));
  });

  app.get('/healthz', async () => ({ ok: true }));

  app.post('/v1/runs', async (request, reply) => {
    const schema = z.object({
      workflow: z.string().min(1),
      input: z.unknown(),
      tenantId: z.string().min(1).optional(),
      idempotencyKey: z.string().min(1).optional(),
      priority: z.number().int().optional(),
      taskMaxAttempts: z.number().int().positive().optional()
    });
    const parsed = schema.safeParse(request.body);
    if (!parsed.success) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            'invalid run payload',
            parsed.error.flatten()
          )
        );
    }

    const run = await createRun(pool, {
      workflow: parsed.data.workflow,
      input: parsed.data.input,
      ...(parsed.data.tenantId ? { tenantId: parsed.data.tenantId } : {}),
      ...(parsed.data.idempotencyKey
        ? { idempotencyKey: parsed.data.idempotencyKey }
        : {}),
      ...(parsed.data.priority !== undefined
        ? { priority: parsed.data.priority }
        : {}),
      ...(parsed.data.taskMaxAttempts !== undefined
        ? { taskMaxAttempts: parsed.data.taskMaxAttempts }
        : {})
    });
    return reply.status(201).send(run);
  });

  app.get('/v1/runs/:id', async (request, reply) => {
    const schema = z.object({ id: z.string().uuid() });
    const querySchema = z.object({ tenantId: z.string().min(1).optional() });
    const paramsResult = schema.safeParse(request.params);
    const queryResult = querySchema.safeParse(request.query);
    if (!paramsResult.success || !queryResult.success) {
      return reply.status(400).send(
        errorShape('validation_failed', 'invalid run lookup', {
          params: paramsResult.success
            ? undefined
            : paramsResult.error.flatten(),
          query: queryResult.success ? undefined : queryResult.error.flatten()
        })
      );
    }

    const runWithSteps = await getRunWithSteps(
      pool,
      paramsResult.data.id,
      queryResult.data.tenantId
    );
    if (!runWithSteps.run) {
      return reply.status(404).send(errorShape('not_found', 'run not found'));
    }

    return reply.send(runWithSteps);
  });

  app.get('/v1/runs', async (request, reply) => {
    const querySchema = z.object({
      tenantId: z.string().min(1).optional(),
      status: z
        .enum([
          'pending',
          'running',
          'sleeping',
          'completed',
          'failed',
          'cancelled'
        ])
        .optional(),
      workflow: z.string().min(1).optional(),
      limit: z.string().optional(),
      offset: z.string().optional()
    });
    const parsed = querySchema.safeParse(request.query);
    if (!parsed.success) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            'invalid run list query',
            parsed.error.flatten()
          )
        );
    }

    const runs = await listRuns(pool, {
      ...(parsed.data.tenantId ? { tenantId: parsed.data.tenantId } : {}),
      ...(parsed.data.status ? { status: parsed.data.status } : {}),
      ...(parsed.data.workflow ? { workflow: parsed.data.workflow } : {}),
      limit: parseJsonNumber(parsed.data.limit, 50),
      offset: parseJsonNumber(parsed.data.offset, 0)
    });
    return reply.send(runs);
  });

  app.post('/v1/runs/:id/cancel', async (request, reply) => {
    const schema = z.object({ id: z.string().uuid() });
    const querySchema = z.object({ tenantId: z.string().min(1).optional() });
    const paramsResult = schema.safeParse(request.params);
    const queryResult = querySchema.safeParse(request.query);
    if (!paramsResult.success || !queryResult.success) {
      return reply.status(400).send(
        errorShape('validation_failed', 'invalid cancel request', {
          params: paramsResult.success
            ? undefined
            : paramsResult.error.flatten(),
          query: queryResult.success ? undefined : queryResult.error.flatten()
        })
      );
    }

    const run = await cancelRun(
      pool,
      paramsResult.data.id,
      queryResult.data.tenantId
    );
    if (!run) {
      return reply.status(404).send(errorShape('not_found', 'run not found'));
    }

    return reply.send(run);
  });

  app.get('/v1/dead-letters', async (request, reply) => {
    const querySchema = z.object({
      tenantId: z.string().min(1).optional(),
      limit: z.string().optional(),
      offset: z.string().optional()
    });
    const parsed = querySchema.safeParse(request.query);
    if (!parsed.success) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            'invalid dead letter query',
            parsed.error.flatten()
          )
        );
    }

    const entries = await listDeadLetters(pool, {
      ...(parsed.data.tenantId ? { tenantId: parsed.data.tenantId } : {}),
      limit: parseJsonNumber(parsed.data.limit, 50),
      offset: parseJsonNumber(parsed.data.offset, 0)
    });
    return reply.send(entries);
  });

  app.post('/v1/dead-letters/:id/replay', async (request, reply) => {
    const schema = z.object({ id: z.string().uuid() });
    const querySchema = z.object({ tenantId: z.string().min(1).optional() });
    const paramsResult = schema.safeParse(request.params);
    const queryResult = querySchema.safeParse(request.query);
    if (!paramsResult.success || !queryResult.success) {
      return reply.status(400).send(
        errorShape('validation_failed', 'invalid replay request', {
          params: paramsResult.success
            ? undefined
            : paramsResult.error.flatten(),
          query: queryResult.success ? undefined : queryResult.error.flatten()
        })
      );
    }

    const replayed = await replayDeadLetter(
      pool,
      paramsResult.data.id,
      queryResult.data.tenantId
    );
    if (!replayed) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'dead letter not found'));
    }

    return reply.send({ replayed: true });
  });

  return app;
}

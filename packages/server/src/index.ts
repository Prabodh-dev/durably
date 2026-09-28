import Fastify, { type FastifyInstance, type FastifyRequest } from 'fastify';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';

import type { DurablyMetrics } from '@durably/core';
import {
  DEFAULT_TENANT_ID,
  InvalidApiKeyError,
  UnknownTenantError,
  cancelRun,
  createDatabasePool,
  createMetrics,
  createRun,
  createSchedule,
  createTenant,
  deleteSchedule,
  getRunWithSteps,
  getSchedule,
  getTenant,
  issueApiKey,
  listApiKeys,
  listDeadLetters,
  listRuns,
  listSchedules,
  listTenants,
  replayDeadLetter,
  resolveApiKeyTenant,
  revokeApiKey,
  runMigrations,
  updateSchedule,
  updateTenant
} from '@durably/core';

export type ServerOptions = {
  databaseUrl: string;
  migrationsDir?: string;
  logger?: boolean;
  adminKey?: string;
  requireApiKey?: boolean;
  metrics?: DurablyMetrics;
};

export type RequestAuth = {
  tenantId: string;
  apiKeyAuthenticated: boolean;
};

declare module 'fastify' {
  interface FastifyRequest {
    auth: RequestAuth;
  }
}

const AUTH_SLOT = Symbol('durably.auth');

const PUBLIC_PATHS = new Set(['/healthz', '/metrics']);

type RequestWithAuthSlot = FastifyRequest & { [AUTH_SLOT]?: RequestAuth };

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
  const metrics = options.metrics ?? createMetrics({ pool });
  app.addHook('onClose', async () => {
    await pool.end();
  });
  await runMigrations(
    pool,
    options.migrationsDir ??
      fileURLToPath(new URL('../../../migrations', import.meta.url))
  );

  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof UnknownTenantError) {
      return reply.status(404).send(errorShape('not_found', error.message));
    }
    const statusCode = (error as { statusCode?: number }).statusCode ?? 500;
    const message = error instanceof Error ? error.message : 'unexpected error';
    reply.status(statusCode).send(errorShape('request_failed', message));
  });

  const adminKey = options.adminKey ?? process.env.DURABLY_ADMIN_KEY;
  const requireApiKey =
    options.requireApiKey ?? process.env.DURABLY_REQUIRE_API_KEY === 'true';

  app.decorateRequest('auth', {
    getter(this: FastifyRequest): RequestAuth {
      const slot = this as RequestWithAuthSlot;
      return (
        slot[AUTH_SLOT] ?? {
          tenantId: DEFAULT_TENANT_ID,
          apiKeyAuthenticated: false
        }
      );
    },
    setter(this: FastifyRequest, value: RequestAuth): void {
      (this as RequestWithAuthSlot)[AUTH_SLOT] = value;
    }
  });

  app.addHook('onRequest', async (request, reply) => {
    if (PUBLIC_PATHS.has(request.url.split('?')[0] ?? request.url)) {
      return;
    }
    if (request.url.startsWith('/v1/admin')) {
      return;
    }

    const header = request.headers.authorization;
    if (!header?.startsWith('Bearer ')) {
      if (requireApiKey) {
        return reply
          .status(401)
          .send(errorShape('unauthorized', 'a bearer api key is required'));
      }
      request.auth = {
        tenantId: DEFAULT_TENANT_ID,
        apiKeyAuthenticated: false
      };
      return;
    }

    const secret = header.slice('Bearer '.length).trim();
    if (secret.length === 0) {
      return reply
        .status(401)
        .send(errorShape('unauthorized', 'a bearer api key is required'));
    }

    try {
      request.auth = {
        tenantId: await resolveApiKeyTenant(pool, secret),
        apiKeyAuthenticated: true
      };
      request.log.info(
        { event: 'api_key_resolved', tenant_id: request.auth.tenantId },
        'api key resolved'
      );
    } catch (error) {
      if (error instanceof InvalidApiKeyError) {
        return reply
          .status(401)
          .send(errorShape('unauthorized', error.message));
      }
      throw error;
    }
  });

  app.get('/healthz', async () => ({ ok: true }));

  app.get('/metrics', async (_request, reply) => {
    reply.header('content-type', metrics.registry.contentType);
    return reply.send(await metrics.registry.metrics());
  });

  app.post('/v1/runs', async (request, reply) => {
    const schema = z.object({
      workflow: z.string().min(1),
      input: z.unknown(),
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
      tenantId: request.auth.tenantId,
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
    const paramsResult = schema.safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid run lookup'));
    }

    const runWithSteps = await getRunWithSteps(
      pool,
      paramsResult.data.id,
      request.auth.tenantId
    );
    if (!runWithSteps.run) {
      return reply.status(404).send(errorShape('not_found', 'run not found'));
    }

    return reply.send(runWithSteps);
  });

  app.get('/v1/runs', async (request, reply) => {
    const querySchema = z.object({
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
      tenantId: request.auth.tenantId,
      ...(parsed.data.status ? { status: parsed.data.status } : {}),
      ...(parsed.data.workflow ? { workflow: parsed.data.workflow } : {}),
      limit: parseJsonNumber(parsed.data.limit, 50),
      offset: parseJsonNumber(parsed.data.offset, 0)
    });
    return reply.send(runs);
  });

  app.post('/v1/runs/:id/cancel', async (request, reply) => {
    const schema = z.object({ id: z.string().uuid() });
    const paramsResult = schema.safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid cancel request'));
    }

    const run = await cancelRun(
      pool,
      paramsResult.data.id,
      request.auth.tenantId
    );
    if (!run) {
      return reply.status(404).send(errorShape('not_found', 'run not found'));
    }

    return reply.send(run);
  });

  app.get('/v1/dead-letters', async (request, reply) => {
    const querySchema = z.object({
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
      tenantId: request.auth.tenantId,
      limit: parseJsonNumber(parsed.data.limit, 50),
      offset: parseJsonNumber(parsed.data.offset, 0)
    });
    return reply.send(entries);
  });

  app.post('/v1/dead-letters/:id/replay', async (request, reply) => {
    const schema = z.object({ id: z.string().uuid() });
    const paramsResult = schema.safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid replay request'));
    }

    const replayed = await replayDeadLetter(
      pool,
      paramsResult.data.id,
      request.auth.tenantId
    );
    if (!replayed) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'dead letter not found'));
    }

    return reply.send({ replayed: true });
  });

  const scheduleCreateSchema = z.object({
    workflow: z.string().min(1),
    cron: z.string().min(1),
    timezone: z.string().min(1).optional(),
    input: z.unknown(),
    enabled: z.boolean().optional(),
    catchup: z.enum(['none', 'latest']).optional()
  });

  const scheduleUpdateSchema = z
    .object({
      workflow: z.string().min(1).optional(),
      cron: z.string().min(1).optional(),
      timezone: z.string().min(1).optional(),
      input: z.unknown().optional(),
      enabled: z.boolean().optional(),
      catchup: z.enum(['none', 'latest']).optional()
    })
    .refine((value) => Object.keys(value).length > 0, {
      message: 'at least one field is required'
    });

  const scheduleParamsSchema = z.object({ id: z.string().uuid() });
  const scheduleListQuerySchema = z.object({
    limit: z.string().optional(),
    offset: z.string().optional()
  });

  app.post('/v1/schedules', async (request, reply) => {
    const parsed = scheduleCreateSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            'invalid schedule payload',
            parsed.error.flatten()
          )
        );
    }

    try {
      const schedule = await createSchedule(pool, {
        workflow: parsed.data.workflow,
        cron: parsed.data.cron,
        input: parsed.data.input,
        tenantId: request.auth.tenantId,
        ...(parsed.data.timezone ? { timezone: parsed.data.timezone } : {}),
        ...(parsed.data.enabled !== undefined
          ? { enabled: parsed.data.enabled }
          : {}),
        ...(parsed.data.catchup ? { catchup: parsed.data.catchup } : {})
      });
      return reply.status(201).send(schedule);
    } catch (error) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            error instanceof Error ? error.message : 'invalid schedule payload'
          )
        );
    }
  });

  app.get('/v1/schedules', async (request, reply) => {
    const parsed = scheduleListQuerySchema.safeParse(request.query);
    if (!parsed.success) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            'invalid schedule query',
            parsed.error.flatten()
          )
        );
    }

    const schedules = await listSchedules(pool, {
      tenantId: request.auth.tenantId,
      limit: parseJsonNumber(parsed.data.limit, 50),
      offset: parseJsonNumber(parsed.data.offset, 0)
    });
    return reply.send(schedules);
  });

  app.get('/v1/schedules/:id', async (request, reply) => {
    const paramsResult = scheduleParamsSchema.safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid schedule lookup'));
    }

    const schedule = await getSchedule(
      pool,
      paramsResult.data.id,
      request.auth.tenantId
    );
    if (!schedule) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'schedule not found'));
    }
    return reply.send(schedule);
  });

  app.patch('/v1/schedules/:id', async (request, reply) => {
    const paramsResult = scheduleParamsSchema.safeParse(request.params);
    const bodyResult = scheduleUpdateSchema.safeParse(request.body);
    if (!paramsResult.success || !bodyResult.success) {
      return reply.status(400).send(
        errorShape('validation_failed', 'invalid schedule update', {
          params: paramsResult.success
            ? undefined
            : paramsResult.error.flatten(),
          body: bodyResult.success ? undefined : bodyResult.error.flatten()
        })
      );
    }

    try {
      const schedule = await updateSchedule(
        pool,
        paramsResult.data.id,
        request.auth.tenantId,
        {
          ...(bodyResult.data.workflow !== undefined
            ? { workflow: bodyResult.data.workflow }
            : {}),
          ...(bodyResult.data.cron !== undefined
            ? { cron: bodyResult.data.cron }
            : {}),
          ...(bodyResult.data.timezone !== undefined
            ? { timezone: bodyResult.data.timezone }
            : {}),
          ...(bodyResult.data.input !== undefined
            ? { input: bodyResult.data.input }
            : {}),
          ...(bodyResult.data.enabled !== undefined
            ? { enabled: bodyResult.data.enabled }
            : {}),
          ...(bodyResult.data.catchup !== undefined
            ? { catchup: bodyResult.data.catchup }
            : {})
        }
      );
      if (!schedule) {
        return reply
          .status(404)
          .send(errorShape('not_found', 'schedule not found'));
      }
      return reply.send(schedule);
    } catch (error) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            error instanceof Error ? error.message : 'invalid schedule update'
          )
        );
    }
  });

  app.delete('/v1/schedules/:id', async (request, reply) => {
    const paramsResult = scheduleParamsSchema.safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid schedule delete'));
    }

    const deleted = await deleteSchedule(
      pool,
      paramsResult.data.id,
      request.auth.tenantId
    );
    if (!deleted) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'schedule not found'));
    }
    return reply.status(204).send();
  });

  app.addHook('preHandler', async (request, reply) => {
    if (!request.url.startsWith('/v1/admin')) {
      return;
    }
    const header = request.headers.authorization;
    const presented = header?.startsWith('Bearer ')
      ? header.slice('Bearer '.length).trim()
      : '';
    if (!adminKey || presented !== adminKey) {
      return reply.status(404).send(errorShape('not_found', 'route not found'));
    }
  });

  const tenantIdParams = z.object({ id: z.string().min(1) });

  app.post('/v1/admin/tenants', async (request, reply) => {
    const schema = z.object({
      id: z.string().min(1).max(64),
      name: z.string().min(1),
      maxConcurrentTasks: z.number().int().min(0).optional()
    });
    const parsed = schema.safeParse(request.body);
    if (!parsed.success) {
      return reply
        .status(400)
        .send(
          errorShape(
            'validation_failed',
            'invalid tenant payload',
            parsed.error.flatten()
          )
        );
    }
    const tenant = await createTenant(pool, {
      id: parsed.data.id,
      name: parsed.data.name,
      ...(parsed.data.maxConcurrentTasks !== undefined
        ? { maxConcurrentTasks: parsed.data.maxConcurrentTasks }
        : {})
    });
    return reply.status(201).send(tenant);
  });

  app.get('/v1/admin/tenants', async () => listTenants(pool));

  app.patch('/v1/admin/tenants/:id', async (request, reply) => {
    const paramsResult = tenantIdParams.safeParse(request.params);
    const bodyResult = z
      .object({
        name: z.string().min(1).optional(),
        maxConcurrentTasks: z.number().int().min(0).optional()
      })
      .refine((value) => Object.keys(value).length > 0, {
        message: 'at least one field is required'
      })
      .safeParse(request.body);
    if (!paramsResult.success || !bodyResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid tenant update'));
    }
    const tenant = await updateTenant(pool, paramsResult.data.id, {
      ...(bodyResult.data.name !== undefined
        ? { name: bodyResult.data.name }
        : {}),
      ...(bodyResult.data.maxConcurrentTasks !== undefined
        ? { maxConcurrentTasks: bodyResult.data.maxConcurrentTasks }
        : {})
    });
    if (!tenant) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'tenant not found'));
    }
    return reply.send(tenant);
  });

  app.get('/v1/admin/tenants/:id/keys', async (request, reply) => {
    const paramsResult = tenantIdParams.safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid tenant lookup'));
    }
    const tenant = await getTenant(pool, paramsResult.data.id);
    if (!tenant) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'tenant not found'));
    }
    return reply.send(await listApiKeys(pool, paramsResult.data.id));
  });

  app.post('/v1/admin/tenants/:id/keys', async (request, reply) => {
    const paramsResult = tenantIdParams.safeParse(request.params);
    const bodyResult = z
      .object({ name: z.string().min(1) })
      .safeParse(request.body);
    if (!paramsResult.success || !bodyResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid api key payload'));
    }
    const tenant = await getTenant(pool, paramsResult.data.id);
    if (!tenant) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'tenant not found'));
    }
    const issued = await issueApiKey(pool, {
      tenantId: paramsResult.data.id,
      name: bodyResult.data.name
    });
    return reply.status(201).send({ ...issued.key, secret: issued.secret });
  });

  app.delete('/v1/admin/tenants/:id/keys/:keyId', async (request, reply) => {
    const paramsResult = z
      .object({ id: z.string().min(1), keyId: z.string().uuid() })
      .safeParse(request.params);
    if (!paramsResult.success) {
      return reply
        .status(400)
        .send(errorShape('validation_failed', 'invalid api key delete'));
    }
    const revoked = await revokeApiKey(
      pool,
      paramsResult.data.keyId,
      paramsResult.data.id
    );
    if (!revoked) {
      return reply
        .status(404)
        .send(errorShape('not_found', 'api key not found'));
    }
    return reply.status(204).send();
  });

  return app;
}

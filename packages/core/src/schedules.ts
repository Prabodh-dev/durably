import { randomUUID } from 'node:crypto';
import { CronExpressionParser } from 'cron-parser';
import type { Pool } from 'pg';

import { createRunInTransaction } from './database.js';
import { DEFAULT_TENANT_ID, UnknownTenantError } from './tenancy.js';
import { systemClock } from './utils.js';
import type { CatchupPolicy, ScheduleRecord } from './types.js';

const MAX_OCCURRENCES_SCAN = 20_000;

export type CreateScheduleInput = {
  tenantId?: string;
  workflow: string;
  cron: string;
  timezone?: string;
  input: unknown;
  enabled?: boolean;
  catchup?: CatchupPolicy;
};

export type UpdateScheduleInput = {
  workflow?: string;
  cron?: string;
  timezone?: string;
  input?: unknown;
  enabled?: boolean;
  catchup?: CatchupPolicy;
};

export type ScheduleFilter = {
  tenantId?: string;
  limit?: number;
  offset?: number;
};

export type ScheduleTickResult = {
  scheduleId: string;
  tenantId: string;
  firedAt: Date | null;
  missed: number | null;
};

export class InvalidCronExpressionError extends Error {
  public readonly expression: string;

  public constructor(expression: string, cause: unknown) {
    super(`invalid cron expression "${expression}": ${String(cause)}`);
    this.name = 'InvalidCronExpressionError';
    this.expression = expression;
  }
}

export function scheduleFireKey(scheduleId: string, fireTime: Date): string {
  return `${scheduleId}:${fireTime.toISOString()}`;
}

function assertValidCron(expression: string, timezone: string): void {
  try {
    CronExpressionParser.parse(expression, { tz: timezone });
  } catch (error) {
    throw new InvalidCronExpressionError(expression, error);
  }
}

function assertValidTimezone(timezone: string): void {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: timezone });
  } catch (error) {
    throw new Error(`invalid timezone "${timezone}": ${String(error)}`);
  }
}

export function latestOccurrenceAtOrBefore(
  expression: string,
  timezone: string,
  at: Date
): Date {
  try {
    return CronExpressionParser.parse(expression, {
      currentDate: new Date(at.getTime() + 1),
      tz: timezone
    })
      .prev()
      .toDate();
  } catch (error) {
    throw new InvalidCronExpressionError(expression, error);
  }
}

export type OccurrenceScan = {
  newest: Date | null;
  count: number | null;
  truncated: boolean;
};

function scanOccurrences(
  expression: string,
  timezone: string,
  from: Date,
  to: Date,
  limit: number = MAX_OCCURRENCES_SCAN
): OccurrenceScan {
  if (to.getTime() <= from.getTime()) {
    return { newest: null, count: 0, truncated: false };
  }

  let next: () => { toDate: () => Date };
  try {
    const parsed = CronExpressionParser.parse(expression, {
      currentDate: from,
      tz: timezone
    });
    next = () => parsed.next();
  } catch (error) {
    throw new InvalidCronExpressionError(expression, error);
  }

  let newest: Date | null = null;
  let count = 0;
  for (let step = 0; step < limit; step += 1) {
    const date = next().toDate();
    if (date.getTime() > to.getTime()) {
      return { newest, count, truncated: false };
    }
    if (date.getTime() > from.getTime()) {
      newest = date;
      count += 1;
    }
  }
  return { newest, count: null, truncated: true };
}

export type FireSelection = {
  fireTime: Date | null;
  missed: number | null;
  newest: Date | null;
};

export function selectFireTime(
  expression: string,
  timezone: string,
  from: Date,
  to: Date,
  catchup: CatchupPolicy
): FireSelection {
  const scan = scanOccurrences(expression, timezone, from, to);
  if (scan.newest === null) {
    return { fireTime: null, missed: 0, newest: null };
  }

  if (scan.truncated) {
    return {
      fireTime: catchup === 'latest' ? scan.newest : null,
      missed: null,
      newest: scan.newest
    };
  }

  if (catchup === 'none' && scan.count !== 1) {
    return { fireTime: null, missed: scan.count, newest: scan.newest };
  }
  return { fireTime: scan.newest, missed: scan.count, newest: scan.newest };
}

export async function createSchedule(
  pool: Pool,
  input: CreateScheduleInput
): Promise<ScheduleRecord> {
  const timezone = input.timezone ?? 'UTC';
  assertValidTimezone(timezone);
  assertValidCron(input.cron, timezone);

  const tenantId = input.tenantId ?? DEFAULT_TENANT_ID;
  const tenant = await pool.query<{ id: string }>(
    'SELECT id FROM tenants WHERE id = $1 LIMIT 1',
    [tenantId]
  );
  if ((tenant.rowCount ?? 0) === 0) {
    throw new UnknownTenantError(tenantId);
  }

  const result = await pool.query<ScheduleRecord>(
    `INSERT INTO schedules (
       id, tenant_id, workflow, cron, timezone, input, enabled, catchup,
       last_fire_time, created_at, updated_at
     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, now(), now())
     RETURNING *`,
    [
      randomUUID(),
      tenantId,
      input.workflow,
      input.cron,
      timezone,
      input.input,
      input.enabled ?? true,
      input.catchup ?? 'latest'
    ]
  );
  return result.rows[0] as ScheduleRecord;
}

export async function getSchedule(
  pool: Pool,
  scheduleId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<ScheduleRecord | null> {
  const result = await pool.query<ScheduleRecord>(
    'SELECT * FROM schedules WHERE id = $1 AND tenant_id = $2 LIMIT 1',
    [scheduleId, tenantId]
  );
  return result.rows[0] ?? null;
}

export async function listSchedules(
  pool: Pool,
  filter: ScheduleFilter = {}
): Promise<ScheduleRecord[]> {
  const limit = filter.limit ?? 50;
  const offset = filter.offset ?? 0;
  const result = filter.tenantId
    ? await pool.query<ScheduleRecord>(
        `SELECT * FROM schedules WHERE tenant_id = $1
         ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
        [filter.tenantId, limit, offset]
      )
    : await pool.query<ScheduleRecord>(
        `SELECT * FROM schedules
         ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
        [limit, offset]
      );
  return result.rows;
}

export async function updateSchedule(
  pool: Pool,
  scheduleId: string,
  tenantId: string,
  patch: UpdateScheduleInput
): Promise<ScheduleRecord | null> {
  const existing = await getSchedule(pool, scheduleId, tenantId);
  if (!existing) {
    return null;
  }

  const merged = {
    workflow: patch.workflow ?? existing.workflow,
    cron: patch.cron ?? existing.cron,
    timezone: patch.timezone ?? existing.timezone,
    input: patch.input ?? existing.input,
    enabled: patch.enabled ?? existing.enabled,
    catchup: patch.catchup ?? existing.catchup
  };
  assertValidTimezone(merged.timezone);
  assertValidCron(merged.cron, merged.timezone);

  const result = await pool.query<ScheduleRecord>(
    `UPDATE schedules
     SET workflow = $3,
         cron = $4,
         timezone = $5,
         input = $6,
         enabled = $7,
         catchup = $8,
         updated_at = now()
     WHERE id = $1 AND tenant_id = $2
     RETURNING *`,
    [
      scheduleId,
      tenantId,
      merged.workflow,
      merged.cron,
      merged.timezone,
      merged.input,
      merged.enabled,
      merged.catchup
    ]
  );
  return result.rows[0] ?? null;
}

export async function deleteSchedule(
  pool: Pool,
  scheduleId: string,
  tenantId = DEFAULT_TENANT_ID
): Promise<boolean> {
  const result = await pool.query(
    'DELETE FROM schedules WHERE id = $1 AND tenant_id = $2',
    [scheduleId, tenantId]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function tickSchedules(
  pool: Pool,
  now: Date = systemClock.now()
): Promise<ScheduleTickResult[]> {
  const client = await pool.connect();
  const swallowError = (): void => undefined;
  client.on('error', swallowError);
  const release = async (): Promise<void> => {
    client.off('error', swallowError);
    await client.release();
  };
  const outcomes: ScheduleTickResult[] = [];

  try {
    await client.query('BEGIN');
    const due = await client.query<ScheduleRecord>(
      `SELECT * FROM schedules
       WHERE enabled = true
       ORDER BY id
       FOR UPDATE SKIP LOCKED`
    );

    for (const schedule of due.rows) {
      const from = schedule.last_fire_time ?? schedule.created_at;
      const selection = selectFireTime(
        schedule.cron,
        schedule.timezone,
        from,
        now,
        schedule.catchup
      );

      if (selection.missed === 0 || selection.newest === null) {
        continue;
      }

      await client.query(
        `UPDATE schedules
         SET last_fire_time = $2, updated_at = now()
         WHERE id = $1`,
        [schedule.id, selection.newest]
      );

      if (selection.fireTime === null) {
        outcomes.push({
          scheduleId: schedule.id,
          tenantId: schedule.tenant_id,
          firedAt: null,
          missed: selection.missed
        });
        continue;
      }

      await createRunInTransaction(client, {
        tenantId: schedule.tenant_id,
        workflow: schedule.workflow,
        input: schedule.input,
        idempotencyKey: scheduleFireKey(schedule.id, selection.fireTime)
      });

      outcomes.push({
        scheduleId: schedule.id,
        tenantId: schedule.tenant_id,
        firedAt: selection.fireTime,
        missed: selection.missed
      });
    }

    if (outcomes.length > 0) {
      await client.query("NOTIFY task_ready, 'cron'");
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    await release();
    throw error;
  }

  await release();
  return outcomes;
}

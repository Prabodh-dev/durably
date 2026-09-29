import { createHash, randomBytes, randomUUID } from 'node:crypto';

import type { Pool } from 'pg';

export const DEFAULT_TENANT_ID = 'default';
export const API_KEY_PREFIX_LENGTH = 12;

export type TenantRecord = {
  id: string;
  name: string;
  max_concurrent_tasks: number;
  last_claim_at: Date | null;
  created_at: Date;
};

export type ApiKeyRecord = {
  id: string;
  tenant_id: string;
  name: string;
  key_prefix: string;
  created_at: Date;
  revoked_at: Date | null;
};

export type IssuedApiKey = {
  key: ApiKeyRecord;
  secret: string;
};

export type TenantFilter = {
  limit?: number;
  offset?: number;
};

export class UnknownTenantError extends Error {
  public readonly tenantId: string;

  public constructor(tenantId: string) {
    super(`unknown tenant ${tenantId}`);
    this.name = 'UnknownTenantError';
    this.tenantId = tenantId;
  }
}

export class InvalidApiKeyError extends Error {
  public constructor() {
    super('invalid or revoked api key');
    this.name = 'InvalidApiKeyError';
  }
}

export function hashApiKey(secret: string): string {
  return createHash('sha256').update(secret, 'utf8').digest('hex');
}

function generateApiKeySecret(): string {
  return `dk_${randomBytes(32).toString('base64url')}`;
}

function apiKeyLookupPrefix(secret: string): string {
  return secret.slice(0, API_KEY_PREFIX_LENGTH);
}

export async function createTenant(
  pool: Pool,
  input: { id: string; name: string; maxConcurrentTasks?: number }
): Promise<TenantRecord> {
  const result = await pool.query<TenantRecord>(
    `INSERT INTO tenants (id, name, max_concurrent_tasks)
     VALUES ($1, $2, $3)
     ON CONFLICT (id) DO UPDATE
       SET name = EXCLUDED.name,
           max_concurrent_tasks = EXCLUDED.max_concurrent_tasks
     RETURNING *`,
    [input.id, input.name, input.maxConcurrentTasks ?? 0]
  );
  return result.rows[0] as TenantRecord;
}

export async function getTenant(
  pool: Pool,
  tenantId: string
): Promise<TenantRecord | null> {
  const result = await pool.query<TenantRecord>(
    'SELECT * FROM tenants WHERE id = $1 LIMIT 1',
    [tenantId]
  );
  return result.rows[0] ?? null;
}

export async function listTenants(
  pool: Pool,
  filter: TenantFilter = {}
): Promise<TenantRecord[]> {
  const limit = filter.limit ?? 50;
  const offset = filter.offset ?? 0;
  const result = await pool.query<TenantRecord>(
    'SELECT * FROM tenants ORDER BY id LIMIT $1 OFFSET $2',
    [limit, offset]
  );
  return result.rows;
}

export async function updateTenant(
  pool: Pool,
  tenantId: string,
  patch: { name?: string; maxConcurrentTasks?: number }
): Promise<TenantRecord | null> {
  const existing = await getTenant(pool, tenantId);
  if (!existing) {
    return null;
  }
  const result = await pool.query<TenantRecord>(
    `UPDATE tenants
     SET name = $2, max_concurrent_tasks = $3
     WHERE id = $1
     RETURNING *`,
    [
      tenantId,
      patch.name ?? existing.name,
      patch.maxConcurrentTasks ?? existing.max_concurrent_tasks
    ]
  );
  return result.rows[0] ?? null;
}

export async function issueApiKey(
  pool: Pool,
  input: { tenantId: string; name: string }
): Promise<IssuedApiKey> {
  const secret = generateApiKeySecret();
  const result = await pool.query<ApiKeyRecord>(
    `INSERT INTO api_keys (id, tenant_id, name, key_prefix, key_hash, created_at, revoked_at)
     VALUES ($1, $2, $3, $4, $5, now(), NULL)
     RETURNING id, tenant_id, name, key_prefix, created_at, revoked_at`,
    [
      randomUUID(),
      input.tenantId,
      input.name,
      apiKeyLookupPrefix(secret),
      hashApiKey(secret)
    ]
  );
  return { key: result.rows[0] as ApiKeyRecord, secret };
}

export async function listApiKeys(
  pool: Pool,
  tenantId: string
): Promise<ApiKeyRecord[]> {
  const result = await pool.query<ApiKeyRecord>(
    `SELECT id, tenant_id, name, key_prefix, created_at, revoked_at
     FROM api_keys
     WHERE tenant_id = $1
     ORDER BY created_at DESC`,
    [tenantId]
  );
  return result.rows;
}

export async function revokeApiKey(
  pool: Pool,
  keyId: string,
  tenantId: string
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE api_keys
     SET revoked_at = now()
     WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL`,
    [keyId, tenantId]
  );
  return (result.rowCount ?? 0) > 0;
}

export async function resolveApiKeyTenant(
  pool: Pool,
  secret: string
): Promise<string> {
  const result = await pool.query<{ tenant_id: string }>(
    `SELECT tenant_id
     FROM api_keys
     WHERE key_hash = $1 AND revoked_at IS NULL
     LIMIT 1`,
    [hashApiKey(secret)]
  );
  const tenantId = result.rows[0]?.tenant_id;
  if (!tenantId) {
    throw new InvalidApiKeyError();
  }
  return tenantId;
}

export async function tenantRunningTasks(
  pool: Pool,
  tenantId: string
): Promise<number> {
  const result = await pool.query<{ count: number }>(
    "SELECT count(*)::int AS count FROM tasks WHERE tenant_id = $1 AND status = 'leased'",
    [tenantId]
  );
  return result.rows[0]?.count ?? 0;
}

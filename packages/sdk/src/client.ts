import type {
  ApiKeyRecord,
  CatchupPolicy,
  DeadLetterRecord,
  RunFilter,
  ScheduleRecord,
  TenantRecord,
  WorkflowRunRecord,
  WorkflowStepRecord
} from '@durably/core';

export type CreateClientOptions = {
  baseUrl: string;
  apiKey?: string;
  adminKey?: string;
};

export type ClientRunInput = {
  workflow: string;
  input: unknown;
  idempotencyKey?: string;
  priority?: number;
  taskMaxAttempts?: number;
};

export type ListRunsInput = Omit<RunFilter, 'tenantId'>;

export type CreateScheduleInput = {
  workflow: string;
  cron: string;
  input: unknown;
  timezone?: string;
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

export type PageInput = {
  limit?: number;
  offset?: number;
};

export type IssuedApiKey = ApiKeyRecord & { secret: string };

export class DurablyApiError extends Error {
  public readonly statusCode: number;
  public readonly code: string;

  public constructor(statusCode: number, code: string, message: string) {
    super(message);
    this.name = 'DurablyApiError';
    this.statusCode = statusCode;
    this.code = code;
  }
}

export type Client = {
  run(..._args: [ClientRunInput]): Promise<WorkflowRunRecord>;
  get(
    ..._args: [string]
  ): Promise<{ run: WorkflowRunRecord | null; steps: WorkflowStepRecord[] }>;
  list(..._args: [ListRunsInput?]): Promise<WorkflowRunRecord[]>;
  cancel(..._args: [string]): Promise<WorkflowRunRecord | null>;
  deadLetters(..._args: [PageInput?]): Promise<DeadLetterRecord[]>;
  replayDeadLetter(..._args: [string]): Promise<boolean>;
  createSchedule(..._args: [CreateScheduleInput]): Promise<ScheduleRecord>;
  getSchedule(..._args: [string]): Promise<ScheduleRecord | null>;
  listSchedules(..._args: [PageInput?]): Promise<ScheduleRecord[]>;
  updateSchedule(
    ..._args: [string, UpdateScheduleInput]
  ): Promise<ScheduleRecord | null>;
  deleteSchedule(..._args: [string]): Promise<boolean>;
  listTenants(): Promise<TenantRecord[]>;
  createTenant(..._args: [TenantInput]): Promise<TenantRecord>;
  updateTenant(..._args: [string, TenantInput]): Promise<TenantRecord | null>;
  listApiKeys(..._args: [string]): Promise<ApiKeyRecord[]>;
  issueApiKey(..._args: [string, string]): Promise<IssuedApiKey>;
  revokeApiKey(..._args: [string, string]): Promise<boolean>;
};

export type TenantInput = {
  id?: string;
  name?: string;
  maxConcurrentTasks?: number;
};

function pageQuery(filter: PageInput = {}): string {
  const params = new URLSearchParams();
  if (typeof filter.limit === 'number') {
    params.set('limit', String(filter.limit));
  }
  if (typeof filter.offset === 'number') {
    params.set('offset', String(filter.offset));
  }
  const query = params.toString();
  return query.length > 0 ? `?${query}` : '';
}

export function createClient(options: CreateClientOptions): Client {
  async function request<T>(
    path: string,
    init: RequestInit,
    apiKey?: string
  ): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set('content-type', 'application/json');
    if (apiKey) {
      headers.set('authorization', `Bearer ${apiKey}`);
    }

    const response = await fetch(new URL(path, options.baseUrl), {
      ...init,
      headers
    });

    if (response.status === 204) {
      return undefined as T;
    }

    const payload = (await response.json().catch(() => null)) as
      { error?: { code?: string; message?: string } } | T | null;
    if (!response.ok) {
      const detail =
        payload && typeof payload === 'object' && 'error' in payload
          ? payload.error
          : undefined;
      throw new DurablyApiError(
        response.status,
        detail?.code ?? 'request_failed',
        detail?.message ?? response.statusText
      );
    }

    return payload as T;
  }

  return {
    async run(input: ClientRunInput): Promise<WorkflowRunRecord> {
      return request<WorkflowRunRecord>(
        '/v1/runs',
        { method: 'POST', body: JSON.stringify(input) },
        options.apiKey
      );
    },
    async get(
      id: string
    ): Promise<{ run: WorkflowRunRecord | null; steps: WorkflowStepRecord[] }> {
      return request(
        `/v1/runs/${encodeURIComponent(id)}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async list(filter: ListRunsInput = {}): Promise<WorkflowRunRecord[]> {
      const params = new URLSearchParams();
      if (filter.status) {
        params.set('status', filter.status);
      }
      if (filter.workflow) {
        params.set('workflow', filter.workflow);
      }
      if (typeof filter.limit === 'number') {
        params.set('limit', String(filter.limit));
      }
      if (typeof filter.offset === 'number') {
        params.set('offset', String(filter.offset));
      }
      const query = params.toString();
      return request<WorkflowRunRecord[]>(
        `/v1/runs${query.length > 0 ? `?${query}` : ''}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async cancel(id: string): Promise<WorkflowRunRecord | null> {
      return request<WorkflowRunRecord | null>(
        `/v1/runs/${encodeURIComponent(id)}/cancel`,
        { method: 'POST' },
        options.apiKey
      );
    },
    async deadLetters(filter: PageInput = {}): Promise<DeadLetterRecord[]> {
      return request<DeadLetterRecord[]>(
        `/v1/dead-letters${pageQuery(filter)}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async replayDeadLetter(id: string): Promise<boolean> {
      const result = await request<{ replayed: boolean }>(
        `/v1/dead-letters/${encodeURIComponent(id)}/replay`,
        { method: 'POST' },
        options.apiKey
      );
      return result.replayed;
    },
    async createSchedule(input: CreateScheduleInput): Promise<ScheduleRecord> {
      return request<ScheduleRecord>(
        '/v1/schedules',
        { method: 'POST', body: JSON.stringify(input) },
        options.apiKey
      );
    },
    async getSchedule(id: string): Promise<ScheduleRecord | null> {
      return request<ScheduleRecord | null>(
        `/v1/schedules/${encodeURIComponent(id)}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async listSchedules(filter: PageInput = {}): Promise<ScheduleRecord[]> {
      return request<ScheduleRecord[]>(
        `/v1/schedules${pageQuery(filter)}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async updateSchedule(
      id: string,
      patch: UpdateScheduleInput
    ): Promise<ScheduleRecord | null> {
      return request<ScheduleRecord | null>(
        `/v1/schedules/${encodeURIComponent(id)}`,
        { method: 'PATCH', body: JSON.stringify(patch) },
        options.apiKey
      );
    },
    async deleteSchedule(id: string): Promise<boolean> {
      try {
        await request<void>(
          `/v1/schedules/${encodeURIComponent(id)}`,
          { method: 'DELETE' },
          options.apiKey
        );
        return true;
      } catch (error) {
        if (error instanceof DurablyApiError && error.statusCode === 404) {
          return false;
        }
        throw error;
      }
    },
    async listTenants(): Promise<TenantRecord[]> {
      return request<TenantRecord[]>(
        '/v1/admin/tenants',
        { method: 'GET' },
        options.adminKey
      );
    },
    async createTenant(input: TenantInput): Promise<TenantRecord> {
      return request<TenantRecord>(
        '/v1/admin/tenants',
        { method: 'POST', body: JSON.stringify(input) },
        options.adminKey
      );
    },
    async updateTenant(
      id: string,
      input: TenantInput
    ): Promise<TenantRecord | null> {
      return request<TenantRecord | null>(
        `/v1/admin/tenants/${encodeURIComponent(id)}`,
        { method: 'PATCH', body: JSON.stringify(input) },
        options.adminKey
      );
    },
    async listApiKeys(tenantId: string): Promise<ApiKeyRecord[]> {
      return request<ApiKeyRecord[]>(
        `/v1/admin/tenants/${encodeURIComponent(tenantId)}/keys`,
        { method: 'GET' },
        options.adminKey
      );
    },
    async issueApiKey(tenantId: string, name: string): Promise<IssuedApiKey> {
      return request<IssuedApiKey>(
        `/v1/admin/tenants/${encodeURIComponent(tenantId)}/keys`,
        { method: 'POST', body: JSON.stringify({ name }) },
        options.adminKey
      );
    },
    async revokeApiKey(tenantId: string, keyId: string): Promise<boolean> {
      try {
        await request<void>(
          `/v1/admin/tenants/${encodeURIComponent(tenantId)}/keys/${encodeURIComponent(keyId)}`,
          { method: 'DELETE' },
          options.adminKey
        );
        return true;
      } catch (error) {
        if (error instanceof DurablyApiError && error.statusCode === 404) {
          return false;
        }
        throw error;
      }
    }
  };
}

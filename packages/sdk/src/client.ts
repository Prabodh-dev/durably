import type {
  DeadLetterRecord,
  RunFilter,
  WorkflowRunRecord,
  WorkflowStepRecord
} from '@durably/core';

export type CreateClientOptions = {
  baseUrl: string;
  apiKey?: string;
};

export type ClientRunInput = {
  workflow: string;
  input: unknown;
  tenantId?: string;
  idempotencyKey?: string;
};

export type ListRunsInput = RunFilter;

export type Client = {
  run(..._args: [ClientRunInput]): Promise<WorkflowRunRecord>;
  get(
    ..._args: [string, string?]
  ): Promise<{ run: WorkflowRunRecord | null; steps: WorkflowStepRecord[] }>;
  list(..._args: [ListRunsInput?]): Promise<WorkflowRunRecord[]>;
  cancel(..._args: [string, string?]): Promise<WorkflowRunRecord | null>;
  deadLetters(
    ..._args: [{ tenantId?: string; limit?: number; offset?: number }?]
  ): Promise<DeadLetterRecord[]>;
  replayDeadLetter(..._args: [string, string?]): Promise<boolean>;
};

async function requestJson<T>(
  baseUrl: string,
  path: string,
  init: RequestInit,
  apiKey?: string
): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('content-type', 'application/json');
  if (apiKey) {
    headers.set('authorization', `Bearer ${apiKey}`);
  }

  const response = await fetch(new URL(path, baseUrl), {
    ...init,
    headers
  });

  const payload = (await response.json()) as T | { error?: unknown };
  if (!response.ok) {
    throw new Error(JSON.stringify(payload));
  }

  return payload as T;
}

export function createClient(options: CreateClientOptions): Client {
  return {
    async run(input: ClientRunInput): Promise<WorkflowRunRecord> {
      return requestJson<WorkflowRunRecord>(
        options.baseUrl,
        '/v1/runs',
        {
          method: 'POST',
          body: JSON.stringify(input)
        },
        options.apiKey
      );
    },
    async get(
      id: string,
      tenantId?: string
    ): Promise<{ run: WorkflowRunRecord | null; steps: WorkflowStepRecord[] }> {
      const suffix = tenantId
        ? `?tenantId=${encodeURIComponent(tenantId)}`
        : '';
      return requestJson<{
        run: WorkflowRunRecord | null;
        steps: WorkflowStepRecord[];
      }>(
        options.baseUrl,
        `/v1/runs/${encodeURIComponent(id)}${suffix}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async list(filter: ListRunsInput = {}): Promise<WorkflowRunRecord[]> {
      const params = new URLSearchParams();
      if (filter.tenantId) {
        params.set('tenantId', filter.tenantId);
      }
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
      return requestJson<WorkflowRunRecord[]>(
        options.baseUrl,
        `/v1/runs?${params.toString()}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async cancel(
      id: string,
      tenantId?: string
    ): Promise<WorkflowRunRecord | null> {
      const suffix = tenantId
        ? `?tenantId=${encodeURIComponent(tenantId)}`
        : '';
      return requestJson<WorkflowRunRecord | null>(
        options.baseUrl,
        `/v1/runs/${encodeURIComponent(id)}/cancel${suffix}`,
        { method: 'POST' },
        options.apiKey
      );
    },
    async deadLetters(
      filter: { tenantId?: string; limit?: number; offset?: number } = {}
    ): Promise<DeadLetterRecord[]> {
      const params = new URLSearchParams();
      if (filter.tenantId) {
        params.set('tenantId', filter.tenantId);
      }
      if (typeof filter.limit === 'number') {
        params.set('limit', String(filter.limit));
      }
      if (typeof filter.offset === 'number') {
        params.set('offset', String(filter.offset));
      }
      return requestJson<DeadLetterRecord[]>(
        options.baseUrl,
        `/v1/dead-letters?${params.toString()}`,
        { method: 'GET' },
        options.apiKey
      );
    },
    async replayDeadLetter(id: string, tenantId?: string): Promise<boolean> {
      const suffix = tenantId
        ? `?tenantId=${encodeURIComponent(tenantId)}`
        : '';
      return requestJson<boolean>(
        options.baseUrl,
        `/v1/dead-letters/${encodeURIComponent(id)}/replay${suffix}`,
        { method: 'POST' },
        options.apiKey
      );
    }
  };
}

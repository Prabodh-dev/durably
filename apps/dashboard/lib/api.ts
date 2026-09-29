export type RunStatus =
  'pending' | 'running' | 'sleeping' | 'completed' | 'failed' | 'cancelled';

export type RunRecord = {
  id: string;
  tenant_id: string;
  workflow: string;
  input: unknown;
  output: unknown;
  status: RunStatus;
  idempotency_key: string | null;
  error: unknown;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
};

export type StepRecord = {
  run_id: string;
  step_key: string;
  status: 'completed' | 'failed';
  output: unknown;
  attempts: number;
  last_error: unknown;
  started_at: string;
  finished_at: string;
};

export type RunWithSteps = {
  run: RunRecord | null;
  steps: StepRecord[];
};

export type DeadLetterRecord = {
  id: string;
  run_id: string;
  task_id: string | null;
  tenant_id: string;
  step_key: string;
  reason: string;
  error: unknown;
  attempts: number;
  created_at: string;
};

export type ScheduleRecord = {
  id: string;
  tenant_id: string;
  workflow: string;
  cron: string;
  input: unknown;
  enabled: boolean;
  last_fire_time: string | null;
  next_fire_time: string | null;
  created_at: string;
  updated_at: string;
};

export type ApiFailure = {
  error: {
    code: string;
    message: string;
  };
};

const baseUrl = process.env.DURABLY_API_URL ?? 'http://localhost:3000';
const apiKey = process.env.DURABLY_API_KEY ?? '';

export class ApiRequestError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ApiRequestError';
    this.status = status;
    this.code = code;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {
    accept: 'application/json',
    ...((init?.headers as Record<string, string> | undefined) ?? {})
  };
  if (apiKey) {
    headers.authorization = `Bearer ${apiKey}`;
  }

  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers,
    cache: 'no-store'
  });
  const text = await response.text();
  const body: unknown = text.length > 0 ? JSON.parse(text) : null;

  if (!response.ok) {
    const failure = body as ApiFailure | null;
    throw new ApiRequestError(
      response.status,
      failure?.error?.code ?? 'unknown',
      failure?.error?.message ?? `request failed with ${response.status}`
    );
  }
  return body as T;
}

export function getRuns(params: {
  status?: RunStatus;
  workflow?: string;
  limit?: number;
  offset?: number;
}): Promise<RunRecord[]> {
  const query = new URLSearchParams();
  if (params.status) {
    query.set('status', params.status);
  }
  if (params.workflow) {
    query.set('workflow', params.workflow);
  }
  query.set('limit', String(params.limit ?? 50));
  query.set('offset', String(params.offset ?? 0));
  return request<RunRecord[]>(`/v1/runs?${query.toString()}`);
}

export function getRun(id: string): Promise<RunWithSteps> {
  return request<RunWithSteps>(`/v1/runs/${id}`);
}

export function getDeadLetters(
  limit = 50,
  offset = 0
): Promise<DeadLetterRecord[]> {
  return request<DeadLetterRecord[]>(
    `/v1/dead-letters?limit=${limit}&offset=${offset}`
  );
}

export function getSchedules(
  limit = 50,
  offset = 0
): Promise<ScheduleRecord[]> {
  return request<ScheduleRecord[]>(
    `/v1/schedules?limit=${limit}&offset=${offset}`
  );
}

export async function getMetrics(): Promise<string> {
  const response = await fetch(`${baseUrl}/metrics`, { cache: 'no-store' });
  if (!response.ok) {
    throw new ApiRequestError(
      response.status,
      'metrics_unavailable',
      `metrics endpoint returned ${response.status}`
    );
  }
  return response.text();
}

export type QueueSnapshot = {
  queueDepth: Record<string, number>;
  runningTasks: number;
  leaders: Array<{ workerId: string; value: number }>;
  totalDeadLetters: number;
  leaseExpirations: number;
};

function sumMetric(text: string, name: string): number {
  const pattern = new RegExp(
    `^${name}(?:\\{[^}]*\\})?\\s+([0-9.eE+-]+)$`,
    'gm'
  );
  let total = 0;
  for (const match of text.matchAll(pattern)) {
    total += Number(match[1]);
  }
  return total;
}

export function parseQueueSnapshot(metrics: string): QueueSnapshot {
  const queueDepth: Record<string, number> = {};
  for (const match of metrics.matchAll(
    /^durably_queue_depth\{status="([^"]+)"\}\s+([0-9.eE+-]+)$/gm
  )) {
    queueDepth[match[1]] = Number(match[2]);
  }

  const leaders = [
    ...metrics.matchAll(
      /^durably_leader\{worker_id="([^"]+)"\}\s+([0-9.eE+-]+)$/gm
    )
  ].map((match) => ({ workerId: match[1], value: Number(match[2]) }));

  return {
    queueDepth,
    runningTasks: sumMetric(metrics, 'durably_running_tasks'),
    leaders,
    totalDeadLetters: sumMetric(metrics, 'durably_dead_letters_total'),
    leaseExpirations: sumMetric(metrics, 'durably_lease_expirations_total')
  };
}

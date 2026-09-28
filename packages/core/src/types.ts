export type RetryPolicy = {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
};

export type StepOptions = Partial<RetryPolicy>;

export type StepFn<T> = (..._args: [string]) => Promise<T> | T;

export type SleepDuration = string | number;

export type StepRunner = {
  run<T>(..._args: [string, StepFn<T>, StepOptions?]): Promise<T>;
  sleep(..._args: [string, SleepDuration]): Promise<void>;
  sleepUntil(..._args: [string, Date | string | number]): Promise<void>;
};

export type WorkflowContext<TInput> = {
  input: TInput;
  runId: string;
  step: StepRunner;
  signal: AbortSignal;
};

export type WorkflowHandler<TInput, TOutput> = (
  ..._args: [WorkflowContext<TInput>]
) => Promise<TOutput> | TOutput;

export type WorkflowDefinition<TInput = unknown, TOutput = unknown> = {
  id: string;
  retry?: StepOptions;
  handler: WorkflowHandler<TInput, TOutput>;
};

export type WorkflowRegistry = ReadonlyMap<
  string,
  WorkflowDefinition<unknown, unknown>
>;

export type RunStatus =
  'pending' | 'running' | 'sleeping' | 'completed' | 'failed' | 'cancelled';

export type TaskStatus = 'ready' | 'leased' | 'done';

export type StepStatus = 'completed' | 'failed';

export type TaskOutcome =
  'completed' | 'retried' | 'dead_lettered' | 'slept' | 'abandoned' | 'error';

export type DeadLetterReason =
  'task_exhausted' | 'lease_lost' | 'workflow_failed' | 'cancelled';

export type WorkflowRunRecord = {
  id: string;
  tenant_id: string;
  workflow: string;
  input: unknown;
  output: unknown | null;
  status: RunStatus;
  idempotency_key: string | null;
  error: unknown | null;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
  traceparent: string | null;
};

export type WorkflowStepRecord = {
  run_id: string;
  step_key: string;
  status: StepStatus;
  output: unknown | null;
  attempts: number;
  last_error: unknown | null;
  started_at: Date;
  finished_at: Date;
  wake_at: Date | null;
};

export type TaskRecord = {
  id: string;
  run_id: string;
  tenant_id: string;
  run_at: Date;
  status: TaskStatus;
  priority: number;
  attempts: number;
  max_attempts: number;
  locked_by: string | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
  claimed_at: Date | null;
  last_error: unknown | null;
};

export type DeadLetterRecord = {
  id: string;
  run_id: string;
  task_id: string;
  tenant_id: string;
  reason: DeadLetterReason;
  error: unknown | null;
  payload: unknown | null;
  created_at: Date;
  replayed_at: Date | null;
};

export type Clock = {
  now(): Date;
};

export type CatchupPolicy = 'none' | 'latest';

export type ScheduleRecord = {
  id: string;
  tenant_id: string;
  workflow: string;
  cron: string;
  timezone: string;
  input: unknown;
  enabled: boolean;
  catchup: CatchupPolicy;
  last_fire_time: Date | null;
  created_at: Date;
  updated_at: Date;
};

export type DurationUnit = 'ms' | 's' | 'm' | 'h' | 'd' | 'w';

export type RetryState = {
  attempts: number;
  policy: RetryPolicy;
};

export type WorkflowExecutionError = Error & {
  stepKey?: string;
  attempt?: number;
  policy?: RetryPolicy;
};

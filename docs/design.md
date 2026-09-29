# Design

## Model

A workflow is a function in a worker process. The engine never runs it start to finish. Each call
enqueues a task, a worker claims the task, the worker replays the function from the beginning, and
every completed step is short-circuited by its stored result. Progress lives in PostgreSQL, so a
worker that dies loses nothing that was recorded.

Three tables carry the state:

- `runs` holds one row per workflow invocation, its status, its tenant, and its trace context.
- `steps` holds one row per step key per run, the step's output, its attempt count, and its last
  error.
- `tasks` holds the queue: one ready, leased, or done row per step that still has to execute.

A step is the unit of work and a task is the unit of scheduling. A task points at a run and a step
key. When a step completes, the transaction that records it also schedules the next task, so a
completed step always has its successor already enqueued.

## Claiming

`claimTasks` is a single statement. It counts each tenant's running tasks, refuses tenants at their
concurrency limit, rotates a starting tenant so no tenant can monopolise the claim, and selects
ready tasks `FOR UPDATE SKIP LOCKED` ordered by priority and due time. A task that is selected
becomes leased with a `lease_token` and a `lease_expires_at`.

`SKIP LOCKED` is what makes the invariant hold: a task already locked by another claimer is skipped
rather than waited on, so two workers can never hold the same task and no worker blocks behind a
slow one. The chaos and queue-model runs both record every task id they claim, and no id was ever
claimed twice.

Every later state change is a compare-and-set:

```sql
UPDATE tasks SET ... WHERE id = $1 AND lease_token = $2
```

If the row count is zero the worker lost its lease. A worker that lost its lease cannot write a step
result, cannot mark a run failed, and cannot enqueue anything. This is the only mechanism that
prevents a resurrected worker from corrupting state.

## Leases

A worker extends its lease on a timer while it holds a task. If the extension fails, or returns
false because another worker already reaped the task, the worker aborts the step through an
`AbortController` and abandons the work. The reaper, a leader duty, returns tasks whose lease
expired to `ready`.

Losing a lease is not an error the step function needs to understand. The step is simply run again
by whoever picks the task up, and the idempotency key makes the visible effect safe to repeat.

## Idempotency and at-least-once

Delivery is at-least-once and this is not negotiable: a worker can execute a step and die before
recording the attempt, so the effect of that step can happen twice. What the engine guarantees is
that a step whose result was recorded is never executed again, because the result row exists and
replay returns it without calling the function.

Step functions receive an idempotency key derived from the run id and the step key. Persisting that
key with a unique constraint, as the chaos workflow does, makes the effect itself idempotent. The
chaos tests assert this directly: a step that completed before a fault never executes again, and a
step that was interrupted may execute once more, and never more.

## Retries

A failing step is retried with exponential backoff and full jitter, up to the step's configured
attempt limit, then the run is failed and a dead letter is written. Retries are per step, and the
attempt count lives on the task so a crash does not reset it.

## Sleeps and timers

`step.sleep` and `step.sleepUntil` do not hold a worker. The step row records the wake time and the
run moves to `sleeping`. The task is scheduled for the wake instant and the worker slot is released
immediately. The wake time is decided once: a retry replays the same recorded wake time rather than
extending it, so a flaky step cannot sleep forever.

A run whose wake time has already passed returns immediately on replay, which is what makes a crash
during a sleep harmless.

## Leader election

One worker at a time holds a PostgreSQL advisory lock on a fixed key. The holder runs the duties
that must not run everywhere: reaping expired leases, ticking cron schedules, and sweeping runs that
are stuck. The lock is session scoped, so a killed leader releases it when its connection drops and a
survivor takes over on its next tick. A worker that cannot reach the database retries on an interval
rather than in a loop.

The LISTEN connection is not load bearing. Losing it costs latency, not correctness, because claims
also happen on a poll timer, and the worker reopens the connection in the background.

## Cron

Schedules are rows with a five field cron expression, a tenant, and the last fire time. The leader
ticks them. Each tick selects due schedules `FOR UPDATE SKIP LOCKED`, computes the next occurrence
after the last fire time, and starts a run with an idempotency key of `schedule id:occurrence`. The
unique index on `(tenant_id, idempotency_key)` is what makes a duplicate fire impossible, including
the case where the leader is killed between starting the run and recording the fire time.

Catching up after downtime is bounded: the tick starts at most one run per schedule per tick, so a
long outage produces a catch-up run rather than a burst of one run per missed minute.

## Multi-tenancy

Every row that a request or a worker can reach carries a `tenant_id`, and every query is scoped by
it. An API key is a random secret stored only as a hash, mapped to a tenant, and checked before the
handler runs. A tenant may cap its own concurrency; the claim query enforces the cap at claim time
rather than trusting the worker.

## Observability

- Structured logs carry `run_id`, `step_key`, `tenant_id`, `task_id`, and `worker_id` where they are
  known.
- Prometheus metrics cover queue depth, running tasks, leadership, claim latency, task duration,
  step attempts and retries, dead letters, lease expirations, and cron ticks. The API and each
  worker expose `/metrics`.
- OpenTelemetry traces are started per task. The trace context is persisted on the run, so a
  resumed or retried task stays in the same trace as its first attempt.

## Database resilience

A worker must survive a database that goes away. The rules that follow from that are enforced in
code:

- Every checked-out `pg` client gets an `error` listener. Without one, a severed connection is an
  unhandled error event and the process dies. The pending query already rejects with the same
  error, so nothing is swallowed.
- Pools get an `error` listener for the same reason: an idle connection dropping emits on the pool.
- The lease heartbeat and the claim loop catch their failures. A claim that cannot reach the
  database is a warning and the next poll retries.
- Connections are created with a `statement_timeout`, because a database that accepts a query and
  never answers is the one failure the reaper cannot rescue. The default is 30 s and
  `DURABLY_STATEMENT_TIMEOUT_MS` overrides it.
- The leader's client has a connection timeout, so a socket that is accepted and never answered
  cannot park election, and therefore the reaper, indefinitely.

## Control surface

`packages/server` exposes runs, dead letters, and schedules over HTTP, scoped to the tenant of the
bearer API key presented on the request. Tenant administration is a separate route group guarded by
the admin key.

`apps/dashboard` is a Next.js App Router application that renders that API: an overview with queue
depth and run counts, a run list with status filters, a run detail page with its steps, and the
dead letter and schedule tables. It reads `DURABLY_API_URL` and, when the API requires a key,
`DURABLY_API_KEY`. Every page is server rendered and fetches on each request, so the numbers are the
current state of the database rather than a cached snapshot.

## Known limitations

### Execution

- A run is executed by replaying its function. A function with side effects outside a step runs
  those side effects on every replay; they must live inside steps and use the idempotency key.
- Exactly-once execution is not claimed anywhere. The guarantee is at-least-once delivery with
  exactly-once effect for steps that honour their idempotency key.
- The claim query holds a row lock for the duration of the claim only. A very large batch makes the
  statement long, and batch size is a tuning parameter.
- Cron catch-up starts one run per tick, so a schedule that missed many occurrences catches up at
  the tick rate rather than instantly.
- Traces are persisted per run, not per step. Step level spans are emitted but only the run level
  context survives a crash.
- Workflows are plain functions with no versioning story. Changing a workflow that has runs in
  flight can change what those runs do when they are replayed, because replay is a fresh execution of
  the current code.

### Operations

- The API server drains in-flight HTTP requests on SIGTERM and closes its pool, but it has no
  request queue, so a deployment behind a load balancer can still drop connections that arrive
  during the grace period.
- The worker exits once its leases are released and its duty timers drain. A step in flight is
  abandoned and reaped, which is correct but means shutdown latency depends on how fast
  `worker.stop()` returns.
- Leadership is held for as long as the process lives and the connection stays up. A paused
  container holding the advisory lock stalls the reaper and the cron ticker for the whole cluster
  until the connection drops.
- There is no structured audit log of administrative actions. Tenant creation and API key issuance
  are not recorded anywhere except the database row they create.
- The dashboard is read only. Runs cannot be cancelled or replayed from it, and dead letter replay
  is an API call with no UI.
- Metrics for leadership, lease expirations, and dead letters are process local. A server that
  never claimed a task reports zero lease expirations regardless of what the workers did; only the
  worker that performed the work counts it.
- `statement_timeout` applies to every pooled connection, so a legitimately long running query on a
  connection created by `createDatabasePool` is killed at 30 seconds by default. There is no separate
  long-running query path.

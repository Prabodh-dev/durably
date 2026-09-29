# durably

A durable workflow and job engine. Developers define multi-step workflows as code, and the engine
guarantees completion across worker crashes, network failures, and downstream errors. PostgreSQL is
the only datastore.

## Hard rules

- No emojis anywhere: code, logs, docs, commit messages, test names.
- No comments in code unless they explain a non-obvious why. No banner comments, no commented-out
  code, no TODOs.
- No placeholder, stubbed, or mocked implementations. If something is not built, do not leave a shell
  of it.
- Do not create files that are not needed. Do not add features that were not asked for.
- Generated output is never committed: no `dist/`, `.next/`, `*.tsbuildinfo`, `next-env.d.ts`, or
  source maps under `src/`. The lockfile is always committed.
- TypeScript strict mode, ESM, Node 20+, pnpm workspaces.
- Tooling must work on Windows and PowerShell. Write scripts as Node scripts, never bash-only.
- Raw SQL through the pg package. No ORM, no Redis, no BullMQ, no external broker.
- Keep dependencies minimal. Before adding a package, check whether the standard library or an
  existing dependency already covers it.

## Stack

- API: Fastify with Zod validation
- Database: PostgreSQL, accessed with pg
- Logging: pino
- Metrics: prom-client
- Tracing: OpenTelemetry
- Tests: Vitest with Testcontainers (real Postgres)
- Dashboard: Next.js 14 App Router, Tailwind CSS v3 (never v4)
- Lint and format: ESLint and Prettier

## Layout

- packages/core: db pool, migration runner, queue, engine, retry logic, schedules, leader duties
- packages/sdk: defineWorkflow, step API, client, worker runtime
- packages/server: Fastify HTTP API
- packages/worker: worker process and the sample workflows it registers
- apps/dashboard: Next.js operator dashboard
- migrations: numbered .sql files applied in order
- ops: Prometheus scrape config and Grafana provisioning
- scripts: crash demo, benchmark, queue model
- docs: design.md, benchmarks.md

## Commands

- pnpm install
- pnpm build
- pnpm lint
- pnpm typecheck
- pnpm format
- pnpm test
- docker compose up

`pnpm test` starts real containers, so it needs Docker and takes a few minutes. Run lint, typecheck,
and the relevant tests after every meaningful change, and fix failures before moving on.

Test concurrency is capped at two workers in `vitest.config.ts`. Every test file starts its own
Postgres container, and running them all at once starves Docker and turns timing-sensitive tests
into flakes. Raise the cap only with measurements showing the suite still fits its timeouts.

## Correctness invariants

These must hold at all times. Any change that could break one needs a test proving it does not.

- A task is claimed by exactly one worker at a time, using FOR UPDATE SKIP LOCKED.
- Every task state transition is a compare-and-set on task id and lease_token. A worker that lost
  its lease can never modify task or run state.
- A completed step is never executed again. Step results are persisted with INSERT ... ON CONFLICT
  DO NOTHING, and the stored result always wins.
- Marking a step done and enqueueing or rescheduling the next task happen in one transaction.
- Delivery is at-least-once. Effects are made exactly-once through idempotency keys passed to step
  functions. Never claim exactly-once execution.
- Retry delay is exponential backoff with full jitter.
- Every query is scoped by tenant_id.

## Database resilience

A worker must survive a database that goes away, and a severed connection must never take a process
down. These rules exist because their absence caused real outages, so do not remove them:

- Every checked-out pg client gets an `error` listener. Without one, a dropped connection is an
  unhandled error event and kills the process. The pending query already rejects with the same
  error.
- Pools get an `error` listener for the same reason: an idle connection dropping emits on the pool.
- The lease heartbeat and the claim loop catch their own failures. A claim that cannot reach the
  database is a warning, and the next poll retries.
- Connections are created with a `statement_timeout`, because a database that accepts a query and
  never answers is the one failure the reaper cannot rescue.
- The leader's client has a connect timeout, and a failed session backs off instead of retrying in a
  loop.
- Migrations are serialised with a bounded advisory lock, so a server and a worker starting together
  cannot execute the same DDL concurrently.

## Code conventions

- Small, single-purpose functions. Explicit types on exported functions. No any.
- Errors are typed and carry enough context to debug: run id, step key, task id.
- Migrations are append-only. Never edit an applied migration; add a new one.
- All external input is validated with Zod at the boundary.
- Time-dependent logic takes an injectable clock so it can be tested without sleeping.
- Logs are structured and carry run_id, step_key, tenant_id, and worker_id where available.
- Export a function only if something outside its own file uses it.

## Testing rules

- Use a real Postgres through Testcontainers. Never mock the database.
- Concurrency and crash behavior must be tested with real separate processes and real kills, not
  simulated.
- Tests must be deterministic. No arbitrary sleeps; wait on conditions with a timeout.
- Every bug fix gets a regression test that fails without the fix.

## Docs

- Plain technical language. No marketing tone, no filler.
- Numbers in docs come from actual measured runs.
- State limitations honestly.

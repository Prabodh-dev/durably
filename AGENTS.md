# durably

A durable workflow and job engine in the spirit of Inngest and Temporal. Developers define multi-step workflows as code, and the engine guarantees completion across worker crashes, network failures, and downstream errors. PostgreSQL is the only datastore.

## Hard rules

- No emojis anywhere: code, logs, docs, commit messages, test names.
- No comments in code unless they explain a non-obvious why. No banner comments, no commented-out code, no TODOs.
- No placeholder, stubbed, or mocked implementations. If something is not built, do not leave a shell of it.
- Do not create files that are not needed. Do not add features that were not asked for.
- TypeScript strict mode, ESM, Node 20+, pnpm workspaces.
- Tooling must work on Windows and PowerShell. Write scripts as Node scripts, never bash-only.
- Raw SQL through the pg package. No ORM, no Redis, no BullMQ, no external broker.
- Keep dependencies minimal. Before adding a package, check whether the standard library or an existing dependency already covers it.

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

- packages/core: db pool, migration runner, queue, engine, retry logic
- packages/sdk: defineWorkflow, step API, client
- packages/server: Fastify HTTP API
- packages/worker: worker process
- apps/dashboard: Next.js dashboard
- migrations: numbered .sql files applied in order
- examples: sample workflows
- scripts: demo, bench, and model scripts
- docs: design.md, benchmarks.md

## Commands

- pnpm install
- pnpm lint
- pnpm typecheck
- pnpm test
- docker compose up

Run lint, typecheck, and the relevant tests after every meaningful change. Fix failures before moving on. Never report work as done without running it.

## Correctness invariants

These must hold at all times. Any change that could break one needs a test proving it does not.

- A task is claimed by exactly one worker at a time, using FOR UPDATE SKIP LOCKED.
- Every task state transition is a compare-and-set on task id and lease_token. A worker that lost its lease can never modify task or run state.
- A completed step is never executed again. Step results are persisted with INSERT ... ON CONFLICT DO NOTHING, and the stored result always wins.
- Marking a step done and enqueueing or rescheduling the next task happen in one transaction.
- Delivery is at-least-once. Effects are made exactly-once through idempotency keys passed to step functions. Never claim exactly-once execution.
- Retry delay is exponential backoff with full jitter.
- Every query is scoped by tenant_id once multi-tenancy exists.

## Code conventions

- Small, single-purpose functions. Explicit types on exported functions. No any.
- Errors are typed and carry enough context to debug: run id, step key, task id.
- Migrations are append-only. Never edit an applied migration; add a new one.
- All external input is validated with Zod at the boundary.
- Time-dependent logic takes an injectable clock so it can be tested without sleeping.
- Logs are structured and carry run_id, step_key, tenant_id, and worker_id where available.

## Testing rules

- Use a real Postgres through Testcontainers. Never mock the database.
- Concurrency and crash behavior must be tested with real separate processes and real kills, not simulated.
- Tests must be deterministic. No arbitrary sleeps; wait on conditions with a timeout.
- Every bug fix gets a regression test.

## Docs

- Plain technical language. No marketing tone, no filler.
- Numbers in docs come from actual measured runs.
- State limitations honestly.

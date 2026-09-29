# Benchmarks

All numbers below come from scripts in this repository, run on the machine that produced them. Each
script prints JSON; nothing here is estimated.

Reproduce with:

```
node scripts/bench.mjs
node scripts/queue-model.mjs
```

Both scripts start a real `postgres:16-alpine` container through Testcontainers, apply the
migrations, and run against it. There is no mock anywhere in the path.

## Environment

| Item               | Value                                                  |
| ------------------ | ------------------------------------------------------ |
| Postgres           | `postgres:16-alpine` container, default settings       |
| Node               | v22.16.0                                               |
| Worker entry point | `packages/worker/dist/main.js`, one process per worker |
| Host               | Windows, Docker Desktop 4.41.2                         |

Containerised Postgres shares the host CPU, so absolute throughput is a property of this
configuration, not a claim about production hardware. The relative comparisons and the invariant
checks are the useful part.

## End-to-end throughput

`scripts/bench.mjs` submits 500 runs of the `bench` workflow. Each run is five steps of 20 ms of
configured work, so a run cannot finish in less than 100 ms of pure step time regardless of engine
overhead. Run latency is measured from `runs.created_at` to `runs.completed_at` and therefore
includes queue wait. Step latency is `steps.finished_at - steps.started_at`.

Configuration: 2 workers, concurrency 8 each, 16 task slots in total.

| Metric                     | Value                   |
| -------------------------- | ----------------------- |
| Runs                       | 500 completed, 0 failed |
| Drain time                 | 3128 ms                 |
| Runs per second            | 159.8                   |
| Step executions per second | 799.2                   |
| Run latency p50            | 1253 ms                 |
| Run latency p95            | 2777 ms                 |
| Run latency p99            | 2900 ms                 |
| Step latency p50           | 23 ms                   |
| Step latency p95           | 30 ms                   |
| Step latency p99           | 30 ms                   |
| Claim query latency p50    | 2.5 ms                  |
| Claim query latency p95    | 5 ms                    |
| Claim query latency p99    | 10 ms                   |

Readings:

- Step latency p50 of 23 ms against 20 ms of configured work means the per-step engine overhead is
  roughly 3 ms: one claim, the step function, and the transaction that records the result and
  schedules the next task.
- The run latency is dominated by queue wait, not by step execution. 500 runs at 16 slots with five
  sequential steps each need at least 500 * 5 / 16 * 100 ms = 1562 ms of wall time, and the measured
  3128 ms includes the time to insert the runs while the workers are already claiming.
- Throughput scales with concurrency slots because steps are independent across runs. The benchmark
  is step-bound, not database-bound, at this size: the claim query stays at single-digit
  milliseconds.

## Queue model under contention

`scripts/queue-model.mjs` measures the claim path directly. It creates tenants, inserts one task per
run, then runs independent claimers, each with its own connection pool, calling `claimTasks` in a
loop. Every claimed task id is recorded, so a task claimed by two workers is detected rather than
assumed away.

Configuration: 8 tenants, 2000 runs per tenant, 16000 tasks, 16 concurrent claimers, batch size 10,
15 s lease.

| Metric               | Value                |
| -------------------- | -------------------- |
| Drain time           | 2818 ms              |
| Claims per second    | 5677.4               |
| Unique tasks claimed | 16000                |
| Duplicate claims     | 0                    |
| Claim latency p50    | 5.16 ms              |
| Claim latency p95    | 9.63 ms              |
| Claim latency p99    | 11.60 ms             |
| Share per tenant     | 0.125 each, spread 0 |

The smaller configuration of 4 tenants, 1000 tasks, 8 claimers drained 1000 tasks in 216 ms at
4620 claims per second with a claim latency p50 of 1.85 ms, and on a later run of the same
configuration at 3710 claims per second. Duplicate claims were zero in every run.

Readings:

- `FOR UPDATE SKIP LOCKED` with a per-tenant fairness ordering gave exactly even tenant shares at
  both sizes. The ordering rotates the tenant cursor per claim, so a tenant with a large backlog
  cannot starve the others.
- Duplicate claims were zero in the 17000 claims of the large configuration alone. The invariant is
  checked by observation, not by reading the query.
- Claim latency grows from 1.85 ms to about 5 ms at the p50 as claimers rise from 8 to 16, which is
  the expected cost of `SKIP LOCKED` contention on the ready index.
- Repeated runs of the same configuration on this host differ by roughly 20 percent, so treat the
  throughput figures as a range for this configuration rather than a fixed number.

## What is not measured

- No comparison against other engines was run, so no comparison is claimed.
- No sustained soak test exists. The longest continuous measurement here is a 3 s drain.
- The benchmark inserts runs sequentially from a single connection, so `insertMs` reflects insert
  throughput, not engine throughput.
- Dead letter behaviour, cron catch-up, and lease expiry recovery are covered by tests, not by
  numbers.

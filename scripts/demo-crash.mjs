import { execFileSync } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { Pool } from 'pg';

const composeArgs = [
  'compose',
  'up',
  '-d',
  '--build',
  '--scale',
  'worker=3',
  'postgres',
  'server',
  'worker'
];
execFileSync('docker', composeArgs, { stdio: 'inherit' });

const databaseUrl =
  process.env.DATABASE_URL ??
  'postgres://durably:durably@localhost:15432/durably';
const apiUrl = process.env.DURABLY_API_URL ?? 'http://localhost:3000';
const pool = new Pool({ connectionString: databaseUrl });

async function waitForCondition(
  callback,
  predicate,
  timeoutMs = 30000,
  intervalMs = 100
) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await callback();
    if (value !== undefined && value !== null && predicate(value)) {
      return value;
    }
    await sleep(intervalMs);
  }
  throw new Error('condition timed out');
}

async function waitForHealth() {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    try {
      const response = await fetch(`${apiUrl}/healthz`);
      if (response.ok) {
        return;
      }
    } catch {
      void 0;
    }
    await sleep(1000);
  }
  throw new Error('server did not become healthy');
}

async function killLeasedWorker() {
  const leased = await pool.query(
    `SELECT locked_by FROM tasks WHERE status = 'leased' ORDER BY lease_expires_at DESC LIMIT 1`
  );
  const workerId = leased.rows[0]?.locked_by;
  if (!workerId) {
    throw new Error('no leased worker found');
  }

  const workerIds = execFileSync('docker', ['compose', 'ps', '-q', 'worker'], {
    encoding: 'utf8'
  })
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (const containerId of workerIds) {
    const inspect = execFileSync(
      'docker',
      ['inspect', '--format', '{{.Config.Hostname}}', containerId],
      {
        encoding: 'utf8'
      }
    ).trim();
    if (inspect === workerId) {
      execFileSync('docker', ['kill', containerId], { stdio: 'inherit' });
      return;
    }
  }

  throw new Error(`unable to match worker ${workerId} to a container`);
}

async function main() {
  await waitForHealth();

  const idempotencyKey =
    process.env.DEMO_IDEMPOTENCY_KEY ?? `demo-crash-${Date.now()}`;

  const runResponse = await fetch(`${apiUrl}/v1/runs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      workflow: 'onboard-user',
      input: {
        email: 'demo@example.com',
        name: 'Demo User',
        plan: 'pro'
      },
      idempotencyKey
    })
  });
  if (!runResponse.ok) {
    throw new Error(await runResponse.text());
  }
  const run = await runResponse.json();

  console.log(`run ${run.id} created`);
  const timeline = [`created:${run.id}`];

  await waitForCondition(
    async () => {
      const steps = await pool.query(
        `SELECT step_key FROM steps WHERE run_id = $1 AND status = 'completed'`,
        [run.id]
      );
      return steps.rows.length >= 1 ? steps.rows.length : undefined;
    },
    (value) => value >= 1
  );
  timeline.push('step-completed:create-profile');

  await killLeasedWorker();
  timeline.push('killed-worker-holding-lease');

  let lastStatus = '';
  for (;;) {
    const current = await fetch(`${apiUrl}/v1/runs/${run.id}`);
    const body = await current.json();
    if (body.run && body.run.status !== lastStatus) {
      lastStatus = body.run.status;
      timeline.push(`status:${body.run.status}`);
    }

    if (body.run?.status === 'completed') {
      const sideEffects = await pool.query(
        'SELECT count(*)::int AS count FROM example_side_effects WHERE idempotency_key = $1',
        [`${run.id}:create-profile`]
      );
      timeline.push(`sideEffects:${sideEffects.rows[0].count}`);
      const stepRows = await pool.query(
        'SELECT step_key, attempts, status FROM steps WHERE run_id = $1 ORDER BY started_at ASC',
        [run.id]
      );
      for (const step of stepRows.rows) {
        timeline.push(
          `step:${step.step_key}:attempts:${step.attempts}:status:${step.status}`
        );
      }
      console.log(timeline.join('\n'));
      break;
    }

    await sleep(200);
  }

  await pool.end();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

import { setTimeout as delay } from 'node:timers/promises';

import { defineWorkflow } from '@durably/sdk';

const BENCH_STEP_COUNT = 5;
const BENCH_STEP_MS = 20;

type BenchOutput = {
  runId: string;
  value: number;
};

export const benchWorkflow = defineWorkflow<Record<string, never>, BenchOutput>(
  {
    id: 'bench',
    retry: { maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 1000 }
  },
  async ({ runId, step }) => {
    let value = 0;
    for (let index = 0; index < BENCH_STEP_COUNT; index += 1) {
      const current = index;
      value = await step.run(`step-${current}`, async () => {
        await delay(BENCH_STEP_MS);
        return value + 1;
      });
    }
    return { runId, value };
  }
);

export const BENCH_WORKFLOW_ID = 'bench';
export const BENCH_TOTAL_STEP_MS = BENCH_STEP_COUNT * BENCH_STEP_MS;

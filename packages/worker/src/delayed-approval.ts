import { defineWorkflow } from '@durably/sdk';

export type DelayedApprovalInput = {
  email: string;
  delay: string;
  wakeAt?: string;
};

export type DelayedApprovalOutput = {
  approved: boolean;
  email: string;
};

export const delayedApprovalWorkflow = defineWorkflow<
  DelayedApprovalInput,
  DelayedApprovalOutput
>(
  {
    id: 'delayed-approval',
    retry: { maxAttempts: 5, baseDelayMs: 100, maxDelayMs: 1000 }
  },
  async ({ input, step }) => {
    const queued = await step.run('queue-request', async () => {
      return { email: input.email, queued: true };
    });

    if (input.wakeAt) {
      await step.sleepUntil('wait-for-approval', input.wakeAt);
    } else {
      await step.sleep('wait-for-approval', input.delay);
    }

    const decision = await step.run('record-decision', async () => {
      return { approved: true, email: queued.email };
    });

    return decision;
  }
);

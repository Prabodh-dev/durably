import { randomUUID } from 'node:crypto';

import type { Clock, RetryPolicy } from './types.js';

export const systemClock: Clock = {
  now: () => new Date()
};

export function createRetryPolicy(policy?: Partial<RetryPolicy>): RetryPolicy {
  return {
    maxAttempts: policy?.maxAttempts ?? 5,
    baseDelayMs: policy?.baseDelayMs ?? 1000,
    maxDelayMs: policy?.maxDelayMs ?? 30000
  };
}

export function computeBackoffDelayMs(
  attempt: number,
  policy: RetryPolicy,
  random: () => number = Math.random
): number {
  const capped = Math.min(policy.maxDelayMs, policy.baseDelayMs * 2 ** attempt);
  return Math.floor(random() * capped);
}

export function createLeaseToken(): string {
  return randomUUID();
}

export function serializeError(error: unknown): unknown {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
      stepKey: (error as { stepKey?: string }).stepKey,
      attempt: (error as { attempt?: number }).attempt,
      policy: (error as { policy?: RetryPolicy }).policy
    };
  }

  return error;
}

export function toDate(value: Date | string | number | null | undefined): Date {
  if (value instanceof Date) {
    return value;
  }

  if (typeof value === 'string' || typeof value === 'number') {
    return new Date(value);
  }

  return new Date();
}

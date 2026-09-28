import { randomUUID } from 'node:crypto';

import type { Clock, DurationUnit, RetryPolicy } from './types.js';

const DURATION_UNITS: Record<DurationUnit, number> = {
  ms: 1,
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
  w: 604_800_000
};

const DURATION_PATTERN = /^(-?\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)$/i;

export class InvalidDurationError extends Error {
  public readonly input: string;

  public constructor(input: string) {
    super(
      `invalid duration "${input}", expected a number followed by ms, s, m, h, d or w`
    );
    this.name = 'InvalidDurationError';
    this.input = input;
  }
}

export function parseDurationMs(value: string | number): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new InvalidDurationError(String(value));
    }
    return value;
  }

  const match = DURATION_PATTERN.exec(value.trim());
  const amount = match?.[1];
  const unit = match?.[2]?.toLowerCase() as DurationUnit | undefined;
  if (amount === undefined || unit === undefined) {
    throw new InvalidDurationError(value);
  }

  const scale = DURATION_UNITS[unit];
  return Number(amount) * scale;
}

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

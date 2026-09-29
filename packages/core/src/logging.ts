import { pino } from 'pino';
import type { Logger } from 'pino';

export type { Logger };

export type LoggerOptions = {
  level?: string;
  bindings?: Record<string, string | number | boolean | null>;
};

export type LogFields = Record<
  string,
  string | number | boolean | null | undefined
>;

export function createLogger(
  bindings: LoggerOptions['bindings'] = {},
  options: LoggerOptions = {}
): Logger {
  return pino({
    level: options.level ?? process.env.LOG_LEVEL ?? 'info',
    base: {
      pid: process.pid,
      ...bindings
    },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label: string) => ({ level: label })
    }
  });
}

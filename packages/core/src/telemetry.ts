import { randomBytes } from 'node:crypto';

import {
  SpanKind,
  SpanStatusCode,
  context as otelContext,
  trace
} from '@opentelemetry/api';
import type {
  Context,
  Span,
  TextMapGetter,
  TextMapSetter,
  Tracer
} from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import { resourceFromAttributes } from '@opentelemetry/resources';
import {
  BatchSpanProcessor,
  NodeTracerProvider
} from '@opentelemetry/sdk-trace-node';
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION
} from '@opentelemetry/semantic-conventions';

export type TelemetryHandle = {
  readonly enabled: boolean;
  tracer: Tracer | null;
  shutdown(): Promise<void>;
};

export type TelemetryOptions = {
  serviceName: string;
  serviceVersion?: string;
  endpoint?: string;
};

export const NOOP_TELEMETRY: TelemetryHandle = {
  enabled: false,
  tracer: null,
  shutdown: async () => undefined
};

function randomHex(bytes: number): string {
  return randomBytes(bytes).toString('hex');
}

export function isTelemetryEnabled(options: TelemetryOptions): boolean {
  const endpoint =
    options.endpoint ??
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ??
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;
  return typeof endpoint === 'string' && endpoint.trim().length > 0;
}

export function startTelemetry(options: TelemetryOptions): TelemetryHandle {
  const endpoint =
    options.endpoint ??
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ??
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;

  if (!isTelemetryEnabled(options)) {
    return NOOP_TELEMETRY;
  }

  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: options.serviceName,
      [ATTR_SERVICE_VERSION]: options.serviceVersion ?? '0.0.0'
    }),
    spanProcessors: [
      new BatchSpanProcessor(
        new OTLPTraceExporter({
          ...(endpoint !== undefined ? { url: endpoint } : {})
        })
      )
    ]
  });

  provider.register();

  return {
    enabled: true,
    tracer: provider.getTracer(options.serviceName),
    shutdown: async () => {
      await provider.shutdown();
    }
  };
}

const TRACE_PROPAGATOR = new W3CTraceContextPropagator();

const CARRIER_GETTER: TextMapGetter<Record<string, string>> = {
  keys: (carrier) => Object.keys(carrier),
  get: (carrier, key) => carrier[key]
};

const CARRIER_SETTER: TextMapSetter<Record<string, string>> = {
  set: (carrier, key, value) => {
    carrier[key] = value;
  }
};

export function createRunTraceparent(): string {
  const traceId = randomHex(16);
  const spanId = randomHex(8);
  return `00-${traceId}-${spanId}-01`;
}

export function extractTraceContext(traceparent: string): Context {
  return TRACE_PROPAGATOR.extract(
    otelContext.active(),
    { traceparent },
    CARRIER_GETTER
  );
}

export function activeTraceparent(): string | undefined {
  const carrier: Record<string, string> = {};
  TRACE_PROPAGATOR.inject(otelContext.active(), carrier, CARRIER_SETTER);
  return carrier.traceparent;
}

export function startRunSpan(
  handle: TelemetryHandle,
  traceparent: string,
  attributes: Record<string, string | number>
): Span {
  if (!handle.tracer) {
    return trace.getTracer('durably').startSpan('disabled');
  }

  const parent = extractTraceContext(traceparent);
  const span = handle.tracer.startSpan(
    'durably.run',
    {
      kind: SpanKind.INTERNAL,
      attributes
    },
    parent
  );
  return span;
}

export function startStepSpan(
  tracer: Tracer | null,
  parentContext: Context,
  stepKey: string,
  attributes: Record<string, string | number>
): Span {
  if (!tracer) {
    return trace.getTracer('durably').startSpan('disabled');
  }

  return tracer.startSpan(
    `durably.step ${stepKey}`,
    { kind: SpanKind.INTERNAL, attributes },
    parentContext
  );
}

export function endSpan(span: Span, error?: unknown): void {
  if (error === undefined) {
    span.setStatus({ code: SpanStatusCode.OK });
  } else {
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: error instanceof Error ? error.message : String(error)
    });
    span.recordException(
      error instanceof Error ? error : new Error(String(error))
    );
  }
  span.end();
}

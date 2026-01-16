import api, { Span } from '@opentelemetry/api';

export const contexBindingHelper = <T>(parentSpan: Span, func: T): T => {
  const ctx = api.trace.setSpan(api.context.active(), parentSpan);
  return api.context.bind(ctx, func);
};
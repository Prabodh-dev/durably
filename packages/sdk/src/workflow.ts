import type {
  StepOptions,
  WorkflowDefinition,
  WorkflowHandler
} from '@durably/core';

export function defineWorkflow<TInput, TOutput>(
  definition: { id: string; retry?: StepOptions },
  handler: WorkflowHandler<TInput, TOutput>
): WorkflowDefinition<TInput, TOutput> {
  return definition.retry === undefined
    ? {
        id: definition.id,
        handler
      }
    : {
        id: definition.id,
        retry: definition.retry,
        handler
      };
}

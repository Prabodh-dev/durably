import {
  DataTable,
  ErrorNote,
  Panel,
  StatusBadge,
  formatTime
} from '@/components/ui';
import { getRun } from '@/lib/api';

export const dynamic = 'force-dynamic';

export default async function RunDetailPage({
  params
}: {
  params: { id: string };
}): Promise<JSX.Element> {
  let detail = null;
  let failure: string | null = null;
  try {
    detail = await getRun(params.id);
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }

  if (failure !== null) {
    return (
      <div className="space-y-6">
        <h1 className="text-xl font-semibold text-slate-100">Run</h1>
        <ErrorNote message={failure} />
      </div>
    );
  }

  if (detail === null || detail.run === null) {
    return (
      <div className="space-y-6">
        <h1 className="text-xl font-semibold text-slate-100">Run</h1>
        <ErrorNote message="run not found" />
      </div>
    );
  }

  const run = detail.run;

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-100">
        Run <span className="font-mono text-sm text-slate-400">{run.id}</span>
      </h1>

      <Panel title="Summary">
        <dl className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
          <div>
            <dt className="text-slate-400">Workflow</dt>
            <dd>{run.workflow}</dd>
          </div>
          <div>
            <dt className="text-slate-400">Tenant</dt>
            <dd>{run.tenant_id}</dd>
          </div>
          <div>
            <dt className="text-slate-400">Status</dt>
            <dd>
              <StatusBadge status={run.status} />
            </dd>
          </div>
          <div>
            <dt className="text-slate-400">Idempotency key</dt>
            <dd className="font-mono text-xs">{run.idempotency_key ?? '-'}</dd>
          </div>
          <div>
            <dt className="text-slate-400">Created</dt>
            <dd>{formatTime(run.created_at)}</dd>
          </div>
          <div>
            <dt className="text-slate-400">Completed</dt>
            <dd>{formatTime(run.completed_at)}</dd>
          </div>
        </dl>
      </Panel>

      <Panel title="Steps">
        <DataTable
          headers={[
            'Step',
            'Status',
            'Attempts',
            'Started',
            'Finished',
            'Output'
          ]}
        >
          {detail.steps.map((step) => (
            <tr key={step.step_key}>
              <td className="py-2 pr-4 font-mono text-xs">{step.step_key}</td>
              <td className="py-2 pr-4">
                <StatusBadge status={step.status} />
              </td>
              <td className="py-2 pr-4">{step.attempts}</td>
              <td className="py-2 pr-4 text-slate-400">
                {formatTime(step.started_at)}
              </td>
              <td className="py-2 pr-4 text-slate-400">
                {formatTime(step.finished_at)}
              </td>
              <td className="py-2 pr-4 font-mono text-xs text-slate-400">
                {step.output === null ? '-' : JSON.stringify(step.output)}
              </td>
            </tr>
          ))}
        </DataTable>
      </Panel>

      {run.error === null ? null : (
        <Panel title="Error">
          <pre className="overflow-x-auto text-xs text-rose-300">
            {JSON.stringify(run.error, null, 2)}
          </pre>
        </Panel>
      )}
    </div>
  );
}

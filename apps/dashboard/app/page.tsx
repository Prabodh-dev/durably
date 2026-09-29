import {
  ErrorNote,
  Panel,
  StatusBadge,
  DataTable,
  formatTime
} from '@/components/ui';
import {
  ApiRequestError,
  getDeadLetters,
  getMetrics,
  getRuns,
  parseQueueSnapshot
} from '@/lib/api';
import type { DeadLetterRecord, RunRecord } from '@/lib/api';
import Link from 'next/link';

export const dynamic = 'force-dynamic';

const SAMPLE_SIZE = 200;

export default async function OverviewPage(): Promise<JSX.Element> {
  let snapshot = null;
  let metricsError: string | null = null;
  try {
    snapshot = parseQueueSnapshot(await getMetrics());
  } catch (error) {
    metricsError =
      error instanceof ApiRequestError
        ? `${error.code}: ${error.message}`
        : String(error);
  }

  let runs: RunRecord[] = [];
  let deadLetters: DeadLetterRecord[] = [];
  let runsError: string | null = null;
  try {
    [runs, deadLetters] = await Promise.all([
      getRuns({ limit: SAMPLE_SIZE }),
      getDeadLetters(SAMPLE_SIZE)
    ]);
  } catch (error) {
    runsError = error instanceof Error ? error.message : String(error);
  }

  const byStatus = (status: string): number =>
    runs.filter((run) => run.status === status).length;
  const recent = runs.slice(0, 10);

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-100">Overview</h1>

      {metricsError !== null ? <ErrorNote message={metricsError} /> : null}

      {snapshot !== null ? (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <Panel title="Ready tasks">
            <p className="text-2xl font-semibold text-slate-100">
              {snapshot.queueDepth.ready ?? 0}
            </p>
          </Panel>
          <Panel title="Running tasks">
            <p className="text-2xl font-semibold text-slate-100">
              {snapshot.runningTasks}
            </p>
          </Panel>
          <Panel title={`Dead letters, latest ${SAMPLE_SIZE}`}>
            <p className="text-2xl font-semibold text-slate-100">
              {deadLetters.length}
            </p>
          </Panel>
          <Panel title={`Failed runs, latest ${SAMPLE_SIZE}`}>
            <p className="text-2xl font-semibold text-slate-100">
              {byStatus('failed')}
            </p>
          </Panel>
        </div>
      ) : null}

      {snapshot !== null && snapshot.leaders.length > 0 ? (
        <Panel title="Leadership">
          <ul className="space-y-1 text-sm">
            {snapshot.leaders.map((leader) => (
              <li key={leader.workerId} className="flex justify-between">
                <span className="text-slate-300">{leader.workerId}</span>
                <span
                  className={
                    leader.value === 1 ? 'text-emerald-400' : 'text-slate-500'
                  }
                >
                  {leader.value === 1 ? 'leader' : 'follower'}
                </span>
              </li>
            ))}
          </ul>
        </Panel>
      ) : null}

      <Panel title="Recent runs">
        {runsError !== null ? (
          <ErrorNote message={runsError} />
        ) : (
          <DataTable
            headers={['Run', 'Workflow', 'Tenant', 'Status', 'Created']}
          >
            {recent.map((run) => (
              <tr key={run.id}>
                <td className="py-2 pr-4">
                  <Link
                    href={`/runs/${run.id}`}
                    className="font-mono text-xs text-sky-400 hover:underline"
                  >
                    {run.id.slice(0, 8)}
                  </Link>
                </td>
                <td className="py-2 pr-4">{run.workflow}</td>
                <td className="py-2 pr-4">{run.tenant_id}</td>
                <td className="py-2 pr-4">
                  <StatusBadge status={run.status} />
                </td>
                <td className="py-2 pr-4 text-slate-400">
                  {formatTime(run.created_at)}
                </td>
              </tr>
            ))}
          </DataTable>
        )}
      </Panel>
    </div>
  );
}

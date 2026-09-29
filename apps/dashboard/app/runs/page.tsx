import Link from 'next/link';

import { DataTable, ErrorNote, StatusBadge, formatTime } from '@/components/ui';
import { getRuns } from '@/lib/api';
import type { RunRecord, RunStatus } from '@/lib/api';

export const dynamic = 'force-dynamic';

const statuses: Array<RunStatus | undefined> = [
  undefined,
  'pending',
  'running',
  'sleeping',
  'completed',
  'failed',
  'cancelled'
];

export default async function RunsPage({
  searchParams
}: {
  searchParams: { status?: string; workflow?: string };
}): Promise<JSX.Element> {
  const selected = statuses.includes(searchParams.status as RunStatus)
    ? (searchParams.status as RunStatus | undefined)
    : undefined;

  let runs: RunRecord[] = [];
  let failure: string | null = null;
  try {
    runs = await getRuns({
      ...(selected ? { status: selected } : {}),
      ...(searchParams.workflow ? { workflow: searchParams.workflow } : {}),
      limit: 100
    });
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-100">Runs</h1>

      <nav className="flex flex-wrap gap-2 text-sm">
        {statuses.map((status) => (
          <Link
            key={status ?? 'all'}
            href={status ? `/runs?status=${status}` : '/runs'}
            className={`rounded border px-3 py-1 ${
              status === selected
                ? 'border-sky-600 bg-sky-950 text-sky-200'
                : 'border-slate-700 text-slate-300 hover:border-slate-500'
            }`}
          >
            {status ?? 'all'}
          </Link>
        ))}
      </nav>

      <div className="rounded-lg border border-slate-800 bg-slate-900 px-4 py-3">
        {failure !== null ? (
          <ErrorNote message={failure} />
        ) : (
          <DataTable
            headers={[
              'Run',
              'Workflow',
              'Tenant',
              'Status',
              'Created',
              'Completed'
            ]}
          >
            {runs.map((run) => (
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
                <td className="py-2 pr-4 text-slate-400">
                  {formatTime(run.completed_at)}
                </td>
              </tr>
            ))}
          </DataTable>
        )}
      </div>
    </div>
  );
}

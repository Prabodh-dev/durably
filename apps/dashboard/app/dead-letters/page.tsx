import { DataTable, ErrorNote, formatTime } from '@/components/ui';
import { getDeadLetters } from '@/lib/api';
import type { DeadLetterRecord } from '@/lib/api';

export const dynamic = 'force-dynamic';

export default async function DeadLettersPage(): Promise<JSX.Element> {
  let entries: DeadLetterRecord[] = [];
  let failure: string | null = null;
  try {
    entries = await getDeadLetters();
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-100">Dead letters</h1>
      <div className="rounded-lg border border-slate-800 bg-slate-900 px-4 py-3">
        {failure !== null ? (
          <ErrorNote message={failure} />
        ) : (
          <DataTable
            headers={['Recorded', 'Run', 'Step', 'Reason', 'Attempts', 'Error']}
          >
            {entries.map((entry) => (
              <tr key={entry.id}>
                <td className="py-2 pr-4 text-slate-400">
                  {formatTime(entry.created_at)}
                </td>
                <td className="py-2 pr-4 font-mono text-xs">
                  {entry.run_id.slice(0, 8)}
                </td>
                <td className="py-2 pr-4 font-mono text-xs">
                  {entry.step_key}
                </td>
                <td className="py-2 pr-4">{entry.reason}</td>
                <td className="py-2 pr-4">{entry.attempts}</td>
                <td className="py-2 pr-4 font-mono text-xs text-rose-300">
                  {entry.error === null
                    ? '-'
                    : JSON.stringify(entry.error).slice(0, 120)}
                </td>
              </tr>
            ))}
          </DataTable>
        )}
      </div>
    </div>
  );
}

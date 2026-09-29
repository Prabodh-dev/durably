import { DataTable, ErrorNote, formatTime } from '@/components/ui';
import { getSchedules } from '@/lib/api';
import type { ScheduleRecord } from '@/lib/api';

export const dynamic = 'force-dynamic';

export default async function SchedulesPage(): Promise<JSX.Element> {
  let schedules: ScheduleRecord[] = [];
  let failure: string | null = null;
  try {
    schedules = await getSchedules();
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-semibold text-slate-100">Schedules</h1>
      <div className="rounded-lg border border-slate-800 bg-slate-900 px-4 py-3">
        {failure !== null ? (
          <ErrorNote message={failure} />
        ) : (
          <DataTable
            headers={[
              'Schedule',
              'Workflow',
              'Cron',
              'Enabled',
              'Last fire',
              'Next fire'
            ]}
          >
            {schedules.map((schedule) => (
              <tr key={schedule.id}>
                <td className="py-2 pr-4 font-mono text-xs">
                  {schedule.id.slice(0, 8)}
                </td>
                <td className="py-2 pr-4">{schedule.workflow}</td>
                <td className="py-2 pr-4 font-mono text-xs">{schedule.cron}</td>
                <td className="py-2 pr-4">
                  {schedule.enabled ? 'enabled' : 'disabled'}
                </td>
                <td className="py-2 pr-4 text-slate-400">
                  {formatTime(schedule.last_fire_time)}
                </td>
                <td className="py-2 pr-4 text-slate-400">
                  {formatTime(schedule.next_fire_time)}
                </td>
              </tr>
            ))}
          </DataTable>
        )}
      </div>
    </div>
  );
}

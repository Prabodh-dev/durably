import type { ReactNode } from 'react';

export function Panel({
  title,
  children
}: {
  title: string;
  children: ReactNode;
}): JSX.Element {
  return (
    <section className="rounded-lg border border-slate-800 bg-slate-900">
      <h2 className="border-b border-slate-800 px-4 py-3 text-sm font-semibold text-slate-200">
        {title}
      </h2>
      <div className="px-4 py-3">{children}</div>
    </section>
  );
}

const statusStyles: Record<string, string> = {
  completed: 'bg-emerald-900/40 text-emerald-300',
  running: 'bg-sky-900/40 text-sky-300',
  sleeping: 'bg-indigo-900/40 text-indigo-300',
  pending: 'bg-slate-800 text-slate-300',
  failed: 'bg-rose-900/40 text-rose-300',
  cancelled: 'bg-slate-800 text-slate-400'
};

export function StatusBadge({ status }: { status: string }): JSX.Element {
  return (
    <span
      className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${
        statusStyles[status] ?? 'bg-slate-800 text-slate-300'
      }`}
    >
      {status}
    </span>
  );
}

export function DataTable({
  headers,
  children
}: {
  headers: string[];
  children: ReactNode;
}): JSX.Element {
  return (
    <table className="w-full text-left text-sm">
      <thead>
        <tr className="text-slate-400">
          {headers.map((header) => (
            <th key={header} className="py-2 pr-4 font-medium">
              {header}
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-800">{children}</tbody>
    </table>
  );
}

export function ErrorNote({ message }: { message: string }): JSX.Element {
  return (
    <p className="rounded border border-rose-800 bg-rose-950/40 px-4 py-3 text-sm text-rose-300">
      {message}
    </p>
  );
}

export function formatTime(value: string | null): string {
  if (value === null) {
    return '-';
  }
  return new Date(value).toISOString().replace('T', ' ').slice(0, 23);
}

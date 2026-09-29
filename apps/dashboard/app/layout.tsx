import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import Link from 'next/link';

import './globals.css';

export const metadata: Metadata = {
  title: 'durably',
  description: 'Durable workflow engine control surface'
};

const navigation = [
  { href: '/', label: 'Overview' },
  { href: '/schedules', label: 'Schedules' },
  { href: '/dead-letters', label: 'Dead letters' }
];

export default function RootLayout({
  children
}: {
  children: ReactNode;
}): JSX.Element {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen">
          <header className="border-b border-slate-800 bg-slate-900">
            <div className="mx-auto flex max-w-6xl items-center gap-6 px-6 py-4">
              <span className="text-lg font-semibold text-slate-100">
                durably
              </span>
              <nav className="flex gap-4 text-sm">
                {navigation.map((item) => (
                  <Link
                    key={item.href}
                    href={item.href}
                    className="text-slate-300 hover:text-white"
                  >
                    {item.label}
                  </Link>
                ))}
                <Link href="/runs" className="text-slate-300 hover:text-white">
                  Runs
                </Link>
              </nav>
            </div>
          </header>
          <main className="mx-auto max-w-6xl px-6 py-8">{children}</main>
        </div>
      </body>
    </html>
  );
}

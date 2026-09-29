import { createServer } from './index.js';

const port = Number(process.env.PORT ?? '3000');
const databaseUrl = process.env.DATABASE_URL;

if (!Number.isInteger(port) || port <= 0) {
  throw new Error('PORT must be a positive integer');
}

if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

const server = await createServer({ databaseUrl, logger: true });
await server.listen({ port, host: '0.0.0.0' });

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  server.log.info({ signal }, 'shutting down');
  try {
    await server.close();
    process.exit(0);
  } catch (error) {
    server.log.error({ err: error }, 'shutdown failed');
    process.exit(1);
  }
}

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});
process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

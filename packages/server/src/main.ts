import { createServer } from './index.js';

const port = Number(process.env.PORT ?? '3000');
const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error('DATABASE_URL is required');
}

const server = await createServer({ databaseUrl, logger: true });
await server.listen({ port, host: '0.0.0.0' });

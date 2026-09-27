import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const rootDir = fileURLToPath(new URL('.', import.meta.url));

export default defineConfig({
  test: {
    environment: 'node',
    globals: true,
    include: ['packages/**/test/**/*.test.ts'],
    testTimeout: 120000,
    hookTimeout: 120000
  },
  resolve: {
    alias: {
      '@durably/core': resolve(rootDir, 'packages/core/src/index.ts'),
      '@durably/sdk': resolve(rootDir, 'packages/sdk/src/index.ts'),
      '@durably/server': resolve(rootDir, 'packages/server/src/index.ts'),
      '@durably/worker': resolve(rootDir, 'packages/worker/src/index.ts')
    }
  }
});

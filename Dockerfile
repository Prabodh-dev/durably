FROM node:20-bookworm-slim AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-workspace.yaml tsconfig.json tsconfig.base.json vitest.config.ts eslint.config.js prettier.config.cjs ./
COPY packages ./packages
COPY migrations ./migrations
COPY examples ./examples
COPY scripts ./scripts
RUN pnpm install
RUN pnpm build

FROM node:20-bookworm-slim AS runtime
WORKDIR /app
RUN corepack enable
COPY --from=build /app/package.json /app/pnpm-workspace.yaml /app/tsconfig.json /app/tsconfig.base.json /app/vitest.config.ts /app/eslint.config.js /app/prettier.config.cjs ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/migrations ./migrations
COPY --from=build /app/examples ./examples
COPY --from=build /app/scripts ./scripts
CMD ["node", "packages/server/dist/main.js"]

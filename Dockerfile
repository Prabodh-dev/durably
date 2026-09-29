FROM node:20.20.2-bookworm-slim AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json tsconfig.base.json ./
COPY packages ./packages
COPY migrations ./migrations
RUN pnpm install --frozen-lockfile
RUN pnpm build

FROM node:20.20.2-bookworm-slim AS runtime
WORKDIR /app
RUN corepack enable
COPY --from=build /app/package.json /app/pnpm-lock.yaml /app/pnpm-workspace.yaml ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages ./packages
COPY --from=build /app/migrations ./migrations
CMD ["node", "packages/server/dist/main.js"]

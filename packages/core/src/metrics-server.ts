import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Registry } from 'prom-client';

export type MetricsServer = {
  readonly port: number;
  close(): Promise<void>;
};

export async function startMetricsServer(options: {
  registry: Registry;
  port: number;
  host?: string;
  path?: string;
}): Promise<MetricsServer> {
  const path = options.path ?? '/metrics';
  const server: Server = createServer((request, response) => {
    if (request.url !== path) {
      response.writeHead(404, { 'content-type': 'text/plain' });
      response.end('not found');
      return;
    }

    void options.registry
      .metrics()
      .then((body) => {
        response.writeHead(200, {
          'content-type': options.registry.contentType
        });
        response.end(body);
      })
      .catch((error: unknown) => {
        response.writeHead(500, { 'content-type': 'text/plain' });
        response.end(error instanceof Error ? error.message : 'metrics error');
      });
  });

  await new Promise<void>((resolveListen, rejectListen) => {
    server.once('error', rejectListen);
    server.listen(options.port, options.host ?? '0.0.0.0', () => {
      server.removeListener('error', rejectListen);
      resolveListen();
    });
  });

  const address = server.address() as AddressInfo;

  return {
    port: address.port,
    close(): Promise<void> {
      return new Promise<void>((resolveClose, rejectClose) => {
        server.close((error) => {
          if (error) {
            rejectClose(error);
            return;
          }
          resolveClose();
        });
      });
    }
  };
}

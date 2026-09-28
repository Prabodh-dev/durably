import net from 'node:net';
import { GenericContainer, Wait } from 'testcontainers';

const server = net.createServer((socket) => {
  socket.on('data', (data) => socket.write(data));
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const upstreamPort = server.address().port;
console.log('upstream port', upstreamPort);

const container = await new GenericContainer('ghcr.io/shopify/toxiproxy:2.9.0')
  .withExposedPorts(8474, 8666)
  .withWaitStrategy(Wait.forHttp('/version', 8474).forStatusCode(200))
  .start();

const control = `http://${container.getHost()}:${container.getMappedPort(8474)}`;
const listenPort = container.getMappedPort(8666);

const created = await fetch(`${control}/proxies`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({
    name: 'p1',
    listen: `0.0.0.0:8666`,
    upstream: `host.docker.internal:${upstreamPort}`,
    enabled: true
  })
});
console.log('create', created.status, await created.text());

async function connect() {
  return new Promise((resolve) => {
    const socket = net.connect(listenPort, '127.0.0.1');
    const timer = setTimeout(() => {
      socket.destroy();
      resolve('timeout');
    }, 3000);
    socket.on('connect', () => {
      clearTimeout(timer);
      socket.destroy();
      resolve('connected');
    });
    socket.on('error', (error) => {
      clearTimeout(timer);
      resolve(`error:${error.code ?? error.message}`);
    });
  });
}

console.log('before cut', await connect());
const cut = await fetch(`${control}/proxies/p1`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ enabled: false })
});
console.log('cut', cut.status, await cut.text());
for (let i=0;i<300;i++) { await connect(); }
console.log('hammered during cut');
const restore = await fetch(`${control}/proxies/p1`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ enabled: true })
});
console.log('restore', restore.status, await restore.text());
const state = await (await fetch(`${control}/proxies/p1`)).text();
console.log('state', state);
for (let attempt = 0; attempt < 5; attempt++) {
  console.log(`after restore attempt ${attempt}`, await connect());
}
await container.stop();
server.close();

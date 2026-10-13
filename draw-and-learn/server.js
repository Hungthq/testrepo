// Draw & Learn local server: serves the app and asks Claude what a child drew.
// (On Vercel, api/look.js does the same job as a Vercel Function.)
// The API key stays here on the server and is never sent to the browser.
//
//   ANTHROPIC_API_KEY=sk-ant-... npm start
//   then open http://localhost:3000 (or http://<this-computer's-IP>:3000 on a tablet)

import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { look } from './lib/look.js';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const MAX_BODY = 6 * 1024 * 1024;

const STATIC = {
  '/': 'index.html',
  '/index.html': 'index.html',
  '/style.css': 'style.css',
  '/app.js': 'app.js',
};
const TYPES = { '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'text/javascript' };

function send(res, status, body, type = 'application/json') {
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store' });
  res.end(typeof body === 'string' || Buffer.isBuffer(body) ? body : JSON.stringify(body));
}

async function readJson(req) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY) throw Object.assign(new Error('Picture is too big'), { status: 413 });
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function handleLook(req, res) {
  let body;
  try {
    body = await readJson(req);
  } catch (err) {
    return send(res, err.status || 400, { error: err.message });
  }
  const result = await look(body, req.headers['x-family-code']);
  send(res, result.status, result.body);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (req.method === 'POST' && url.pathname === '/api/look') return handleLook(req, res);
  if (req.method === 'GET' && url.pathname === '/api/health') return send(res, 200, { ok: true });

  const file = STATIC[url.pathname];
  if (req.method !== 'GET' || !file) return send(res, 404, 'Not found', 'text/plain');
  try {
    const data = await fs.readFile(path.join(ROOT, file));
    send(res, 200, data, TYPES[path.extname(file)]);
  } catch {
    send(res, 404, 'Not found', 'text/plain');
  }
});

server.listen(PORT, () => {
  console.log(`🖍️  Draw & Learn is running at http://localhost:${PORT}`);
  if (!process.env.ANTHROPIC_API_KEY && !process.env.ANTHROPIC_AUTH_TOKEN) {
    console.log('⚠️  ANTHROPIC_API_KEY is not set, so "What did I draw?" will not work yet.');
  }
});

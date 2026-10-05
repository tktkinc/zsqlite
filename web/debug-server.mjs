import { randomBytes, randomUUID } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';

const limit = 64 * 1024;
const actions = new Set(['snapshot', 'probe', 'stats', 'query', 'reload']);
export function attachDebug(server) {
  const token = randomBytes(32).toString('hex');
  const sessions = new Map();
  const pending = new Map();
  const sockets = new WebSocketServer({ noServer: true, maxPayload: limit, perMessageDeflate: false });
  function record(session, event) {
    session.events.push({ time: new Date().toISOString(), ...event });
    if (session.events.length > 300) session.events.shift();
    session.lastSeen = new Date().toISOString();
  }
  function sendJson(res, status, data) {
    res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(data));
  }
  server.on('upgrade', (req, socket, head) => {
    const path = new URL(req.url, 'http://localhost').pathname;
    const host = req.headers['x-forwarded-host'] ?? req.headers.host;
    let sameOrigin = false;
    try { sameOrigin = new URL(req.headers.origin).host === host; } catch { /* Browser connections require their page origin. */ }
    if (path !== '/debug/ws' || !sameOrigin) { socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return; }
    sockets.handleUpgrade(req, socket, head, ws => sockets.emit('connection', ws, req));
  });
  sockets.on('connection', (ws, req) => {
    const id = randomUUID();
    const session = { id, ws, connected: true, address: req.headers['x-forwarded-for'] ?? req.socket.remoteAddress,
      started: new Date().toISOString(), lastSeen: new Date().toISOString(), info: {}, events: [] };
    sessions.set(id, session);
    while (sessions.size > 24) {
      const old = [...sessions.values()].find(item => !item.connected);
      if (!old) break;
      sessions.delete(old.id);
    }
    ws.send(JSON.stringify({ kind: 'connected', id }));
    ws.on('message', raw => {
      try {
        const message = JSON.parse(raw.toString());
        if (!message || typeof message !== 'object') return;
        if (message.kind === 'hello') { session.info = message.info ?? {}; record(session, { kind: 'hello', data: session.info }); }
        else if (message.kind === 'event') record(session, { kind: String(message.name).slice(0, 80), data: message.data });
        else if (message.kind === 'result') {
          const request = pending.get(message.id);
          if (request?.session === session.id) {
            clearTimeout(request.timer); pending.delete(message.id);
            record(session, { kind: 'command-result', action: request.action, data: message.result, error: message.error });
            request.resolve({ result: message.result, error: message.error });
          }
        }
      } catch { record(session, { kind: 'invalid-message' }); }
    });
    ws.on('error', error => record(session, { kind: 'socket-error', data: error.message }));
    ws.on('close', () => {
      session.connected = false;
      record(session, { kind: 'disconnected' });
      for (const [key, request] of pending) if (request.session === id) {
        clearTimeout(request.timer); pending.delete(key); request.resolve({ error: 'Browser disconnected' });
      }
    });
  });
  async function command(session, payload) {
    if (!session?.connected || session.ws.readyState !== WebSocket.OPEN) return { error: 'Browser is not connected' };
    const id = randomUUID();
    return new Promise(resolve => {
      const timer = setTimeout(() => { pending.delete(id); resolve({ error: 'Browser command timed out' }); }, 15000);
      pending.set(id, { session: session.id, resolve, timer, action: payload.action });
      record(session, { kind: 'command', action: payload.action });
      session.ws.send(JSON.stringify({ kind: 'command', id, ...payload }));
    });
  }
  const hub = {
    token,
    async handle(req, res, path) {
      if (path === '/debug/config') { sendJson(res, 200, { enabled: true, websocket: '/debug/ws' }); return true; }
      if (!path.startsWith('/api/debug/')) return false;
      // The controller token never goes to the browser. Only the local operator gets it.
      res.removeHeader('Access-Control-Allow-Origin');
      if (req.headers.authorization !== `Bearer ${token}`) { sendJson(res, 401, { error: 'Debug controller token required' }); return true; }
      if (path === '/api/debug/sessions' && req.method === 'GET') {
        sendJson(res, 200, [...sessions.values()].map(({ ws, events, ...session }) => ({ ...session, events: events.length })));
        return true;
      }
      const match = /^\/api\/debug\/sessions\/([a-f0-9-]+)\/(events|command)$/.exec(path);
      const session = match && sessions.get(match[1]);
      if (!session) { sendJson(res, 404, { error: 'No such browser session' }); return true; }
      if (match[2] === 'events' && req.method === 'GET') { sendJson(res, 200, session.events); return true; }
      if (match[2] !== 'command' || req.method !== 'POST') { sendJson(res, 405, { error: 'Unsupported debug operation' }); return true; }
      let size = 0, body = '';
      for await (const chunk of req) {
        size += chunk.length;
        if (size > limit) { sendJson(res, 413, { error: 'Debug command is too large' }); return true; }
        body += chunk;
      }
      let payload;
      try { payload = JSON.parse(body); } catch { sendJson(res, 400, { error: 'Invalid debug command' }); return true; }
      if (!actions.has(payload?.action)) { sendJson(res, 400, { error: 'Unsupported debug command' }); return true; }
      sendJson(res, 200, await command(session, payload));
      return true;
    },
    close() { for (const session of sessions.values()) session.ws.terminate(); sockets.close(); },
  };
  server.once('close', () => hub.close());
  return hub;
}

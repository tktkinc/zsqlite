#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
const [file, action = 'sessions', session, ...args] = process.argv.slice(2);
if (!file || (action !== 'sessions' && !session)) throw new Error('Usage: node web/debug-cli.mjs DEBUG_FILE [sessions | events ID | snapshot ID | probe ID | stats ID | query ID SQL | reload ID]');
const { url, token } = JSON.parse(await readFile(file, 'utf8'));
const path = action === 'sessions' ? '/api/debug/sessions' : `/api/debug/sessions/${encodeURIComponent(session)}/${action === 'events' ? 'events' : 'command'}`;
const response = await fetch(new URL(path, url), { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  ...(action !== 'sessions' && action !== 'events' ? { method: 'POST', body: JSON.stringify({ action, ...(action === 'query' ? { sql: args.join(' ') } : {}) }) } : {}) });
if (!response.ok) throw new Error(`Debug endpoint HTTP ${response.status}: ${await response.text()}`);
console.log(JSON.stringify(await response.json(), null, 2));

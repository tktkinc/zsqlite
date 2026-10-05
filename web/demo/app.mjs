import { openDatabase, httpStorage } from '/wasm/index.mjs';
import { parseSearch, searchArticles, makeSnippet, plainText, pageSize } from '/search.mjs';

const $ = id => document.getElementById(id);
let stores = [];
let db;
let tables = [];
let busy = false;
let hasSearch = false;
let results;
let activeQuery;
let memoryChosen = false;
if (window.zsqliteDebug) {
  window.zsqliteDebug.handlers.stats = () => { if (!db) throw new Error('Database is not open'); return db.stats(); };
  window.zsqliteDebug.handlers.query = command => { if (!db) throw new Error('Database is not open'); return db.query(command.sql, command.parameters ?? [], { maxRows: 20 }); };
  window.zsqliteDebug.log('app-module-loaded', {});
}

function bytes(value) {
  if (value >= 1024 ** 3) return `${(value / 1024 ** 3).toFixed(2)} GiB`;
  if (value >= 1024 ** 2) return `${(value / 1024 ** 2).toFixed(1)} MiB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${value} B`;
}
function state(label, kind = '') {
  window.zsqliteDebug?.log('state', { label, kind });
  $('connection-state').textContent = label;
  $('state-dot').className = `dot ${kind}`;
}
function controls() {
  for (const id of ['connect', 'store', 'transport', 'memory-budget', 'disk-budget']) $(id).disabled = busy;
  for (const id of ['search', 'search-button']) $(id).disabled = busy || !db || !hasSearch;
  for (const id of ['run', 'schema', 'clear-cache']) $(id).disabled = busy || !db;
  $('clear-search').disabled = busy;
  $('previous-page').disabled = busy || !results || results.page === 0;
  $('next-page').disabled = busy || !results || (results.page + 1) * pageSize >= results.count;
  for (const button of document.querySelectorAll('.table-button')) button.disabled = busy || !db;
}
async function updateStats() {
  const stats = await db.stats();
  $('requests').textContent = `${stats.requests.toLocaleString()} requests`;
  $('received').textContent = `${bytes(stats.bytes)} received`;
  $('cache').textContent = `Cache: ${bytes(stats.memoryCacheBytes)} / ${bytes(stats.memoryCacheBudget)} RAM`;
  $('disk-cache').textContent = stats.diskCacheAvailable
    ? `${bytes(stats.diskCacheBytes)} / ${bytes(stats.diskCacheBudget)} disk`
    : 'Disk cache unavailable';
  $('disk-cache').title = stats.diskCacheError || 'Cached data and decoded pages stay on disk between visits.';
  $('cache-hits').textContent = `${stats.cacheHits.toLocaleString()} cache hits`;
  $('db-compressed').textContent = bytes(stats.sealedBytes);
  $('db-logical').textContent = bytes(stats.logicalBytes);
  $('db-ratio').textContent = stats.sealedBytes ? `${(stats.logicalBytes / stats.sealedBytes).toFixed(2)}×` : '—';
  $('db-pages').textContent = `${stats.pages.toLocaleString()} × ${bytes(stats.pageSize)}`;
}
async function work(label, action) {
  if (busy) return;
  busy = true;
  controls();
  $('error').hidden = true;
  $('status').textContent = label;
  const started = performance.now();
  window.zsqliteDebug?.log('work-start', { label });
  try {
    await action();
    window.zsqliteDebug?.log('work-done', { label, elapsedMs: performance.now() - started });
    $('status').textContent = `Ready · ${(performance.now() - started).toFixed(0)} ms`;
  } catch (error) {
    window.zsqliteDebug?.log('work-error', { label, message: error.message, stack: error.stack });
    $('error').textContent = error.message;
    $('error').hidden = false;
    $('status').textContent = 'The request failed. You can try another search or query.';
    if (!db) state('Could not open store', 'failed');
  } finally {
    if (db) { try { await updateStats(); } catch { /* A worker may have already closed. */ } }
    busy = false;
    controls();
  }
}
function appendSnippet(parent, snippet) {
  if (snippet.before) parent.append('…');
  let position = 0;
  for (const match of snippet.matches) {
    parent.append(snippet.text.slice(position, match.start));
    const mark = document.createElement('mark');
    mark.textContent = snippet.text.slice(match.start, match.end);
    parent.append(mark);
    position = match.end;
  }
  parent.append(snippet.text.slice(position));
  if (snippet.after) parent.append('…');
}
function renderResults(elapsed) {
  $('search-results').hidden = false;
  $('search-welcome').hidden = true;
  document.body.classList.add('has-results');
  $('results-title').textContent = `Results for “${activeQuery.text}”`;
  const notes = { all: 'Best matches first', prefix: 'Including word-prefix matches', any: 'Related results matching any search term', title: 'Title matches' };
  $('results-summary').textContent = `${results.count.toLocaleString()} results · ${notes[results.mode]}`;
  $('search-time').textContent = `${elapsed.toFixed(0)} ms search time`;
  $('results-list').replaceChildren();
  if (!results.rows.length) {
    const empty = document.createElement('li');
    empty.className = 'no-results';
    empty.textContent = 'No matches. Try a shorter query, a different spelling, or a word followed by *.';
    $('results-list').append(empty);
  }
  for (const row of results.rows) {
    const item = document.createElement('li');
    item.className = 'search-result';
    const heading = document.createElement('h3');
    let url;
    try { const parsed = new URL(row.url); if (['http:', 'https:'].includes(parsed.protocol)) url = parsed; } catch { /* Stored source URL is optional. */ }
    const title = document.createElement(url ? 'a' : 'span');
    title.textContent = row.title;
    if (url) { title.href = url.href; title.target = '_blank'; title.rel = 'noopener noreferrer'; }
    heading.append(title);
    const source = document.createElement('p');
    source.className = 'result-source';
    source.textContent = url ? `${url.hostname} › ${row.path.replaceAll('_', ' ')}` : row.path.replaceAll('_', ' ');
    const excerpt = document.createElement('p');
    excerpt.className = 'result-snippet';
    const text = plainText(row.content);
    if (text) appendSnippet(excerpt, makeSnippet(text, activeQuery.terms));
    else excerpt.textContent = 'No stored preview is available for this result.';
    item.append(heading, source, excerpt);
    $('results-list').append(item);
  }
  $('page-label').textContent = results.count
    ? `${results.page * pageSize + 1}–${Math.min((results.page + 1) * pageSize, results.count)} of ${results.count.toLocaleString()}`
    : '0 results';
  $('pagination').hidden = results.count === 0;
}
async function search(page = 0, route = true) {
  activeQuery = parseSearch($('search').value);
  const before = await db.stats();
  const started = performance.now();
  results = await searchArticles(db, activeQuery, { page, fts: tables.includes('entry_fts'),
    mode: page && results ? results.mode : 'all' });
  renderResults(performance.now() - started);
  $('search-time').textContent = `${(performance.now() - started).toFixed(0)} ms search time`;
  const after = await db.stats();
  $('search-downloaded').textContent = `${bytes(after.bytes - before.bytes)} downloaded`;
  $('search-cached').textContent = `${bytes(after.cacheReadBytes - before.cacheReadBytes)} cached data read`;
  if (route) {
    const hash = new URLSearchParams({ q: activeQuery.text, page: String(results.page + 1) });
    window.history.pushState(null, '', `#${hash}`);
  }
  $('search-form').parentElement.scrollIntoView({ block: 'start' });
}
function resetSearch(route = true) {
  results = undefined;
  activeQuery = undefined;
  $('search').value = '';
  $('search-results').hidden = true;
  $('search-welcome').hidden = false;
  document.body.classList.remove('has-results');
  if (route) window.history.pushState(null, '', location.pathname);
}
async function restoreRoute() {
  const route = new URLSearchParams(location.hash.slice(1));
  if (route.has('q') && hasSearch) {
    $('search').value = route.get('q');
    const page = Number(route.get('page'));
    await search(Number.isSafeInteger(page) && page > 0 ? page - 1 : 0, false);
  } else resetSearch(false);
}
async function runSql() {
  const started = performance.now();
  const result = await db.query($('sql').value, [], { maxRows: 200 });
  $('query-summary').textContent = `${result.rows.length} rows · ${(performance.now() - started).toFixed(0)} ms`;
  const table = document.createElement('table');
  const header = table.createTHead().insertRow();
  for (const column of result.columns) {
    const cell = document.createElement('th'); cell.textContent = column; header.append(cell);
  }
  const body = table.createTBody();
  for (const row of result.rows) {
    const tr = body.insertRow();
    for (const value of row) {
      const cell = tr.insertCell();
      cell.textContent = value === null ? 'NULL' : value instanceof Uint8Array ? `BLOB · ${bytes(value.length)}` : String(value);
      cell.title = cell.textContent;
    }
  }
  $('query-result').replaceChildren(table);
}
async function connect() {
  if (db) { await db.close(); db = undefined; }
  results = undefined;
  hasSearch = false;
  $('query-result').replaceChildren();
  $('query-summary').textContent = '';
  state('Opening sealed store…');
  const store = stores.find(store => store.id === $('store').value);
  const httpMode = $('transport').value;
  const memoryBudget = Number($('memory-budget').value) * 1024 ** 2;
  const objectCacheBytes = Math.floor(memoryBudget / 4);
  let openingBytes = 0;
  db = await openDatabase({ storage: httpStorage({ url: new URL(httpMode === 'get' ? store.getUrl : store.url, location.href).href, httpMode }),
    cacheKey: new URL(store.url, location.href).href, cacheBytes: memoryBudget - objectCacheBytes - 2 * 1024 ** 2,
    objectCacheBytes, onProgress: progress => {
      window.zsqliteDebug?.log('worker-progress', progress);
      if (!db && progress.stage === 'http-response') {
        openingBytes += progress.responseBytes || 0;
        $('status').textContent = `Opening database · ${bytes(openingBytes)} downloaded`;
      } else if (!progress.stage.startsWith('http-')) $('status').textContent = progress.message;
    }, diskCacheBytes: Number($('disk-budget').value) * 1024 ** 3 });
  try { localStorage.setItem('zsqlite-cache-settings', JSON.stringify({ memory: $('memory-budget').value, disk: $('disk-budget').value, memoryChosen })); } catch { /* Preferences are optional. */ }
  tables = (await db.query("SELECT name FROM pragma_table_list WHERE schema='main' AND type IN ('table','virtual') AND name NOT LIKE 'sqlite_%' ORDER BY name")).rows.map(row => row[0]);
  hasSearch = tables.includes('entries') && tables.includes('blobs') && tables.includes('entry_fts');
  $('tables').replaceChildren();
  for (const name of tables) {
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'table-button'; button.textContent = name;
    button.addEventListener('click', () => {
      $('sql').value = `SELECT * FROM "${name.replaceAll('"', '""')}" LIMIT 20;`;
      void work(`Reading ${name}…`, runSql);
    });
    $('tables').append(button);
  }
  $('store-title').textContent = store.name;
  $('store-description').textContent = `${tables.length} tables in this sealed store`;
  $('store-date').textContent = '';
  if (hasSearch && tables.includes('archive')) {
    const metadata = await db.query('SELECT title, description, date, entry_count FROM archive LIMIT 1');
    if (metadata.rows.length) {
      const [title, description, date, count] = metadata.rows[0];
      $('store-title').textContent = title || store.name;
      $('store-description').textContent = `${description || 'Knowledge archive'} · ${Number(count).toLocaleString()} entries`;
      $('store-date').textContent = date ? `Published ${date}` : '';
    }
  }
  if (!hasSearch) {
    $('sql-console').open = true;
    $('sql').value = "SELECT name, type, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 20;";
    await runSql();
  }
  await restoreRoute();
  $('connect').textContent = 'Reconnect';
  state(httpMode === 'get' ? 'Connected · GET only' : httpMode === 'range' ? 'Connected · ranges' : 'Connected · auto', 'ready');
}
$('connection-form').addEventListener('submit', event => { event.preventDefault(); void work('Opening the knowledge store…', connect); });
$('search-form').addEventListener('submit', event => { event.preventDefault(); void work('Finding the best matches…', () => search()); });
$('clear-search').addEventListener('click', () => { resetSearch(); controls(); $('search').focus(); });
$('previous-page').addEventListener('click', () => work('Loading results…', () => search(results.page - 1)));
$('next-page').addEventListener('click', () => work('Loading results…', () => search(results.page + 1)));
window.addEventListener('popstate', () => { if (db) void work('Restoring search…', restoreRoute); });
$('sql-form').addEventListener('submit', event => { event.preventDefault(); void work('Running SQL…', runSql); });
$('schema').addEventListener('click', () => { $('sql').value = "SELECT name, type, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 100;"; void work('Reading schema…', runSql); });
$('clear-cache').addEventListener('click', () => work('Clearing the disk cache…', () => db.clearCache()));
$('keep-cache').addEventListener('click', async () => {
  try { $('cache-note').textContent = await navigator.storage.persist() ? 'Disk storage is protected from automatic eviction.' : 'Cache stays between visits, but the browser may reclaim space when needed.'; }
  catch { $('cache-note').textContent = 'This browser cannot protect cached storage from eviction.'; }
});
$('transport').addEventListener('change', () => {
  $('transport-note').textContent = $('transport').value === 'get'
    ? 'Ordinary GET works with simple static servers. Repeated reads use the cache.'
    : 'Fetch the compressed data a query needs; reuse cached data between visits.';
});
$('memory-budget').addEventListener('change', () => { memoryChosen = true; });
$('sql').addEventListener('keydown', event => {
  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) { event.preventDefault(); void work('Running SQL…', runSql); }
});
const mobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent)
  || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
if (mobile) $('memory-budget').value = '64';
try {
  const preferences = JSON.parse(localStorage.getItem('zsqlite-cache-settings') ?? '{}');
  memoryChosen = preferences.memoryChosen === true;
  for (const [id, value] of [['memory-budget', preferences.memory], ['disk-budget', preferences.disk]]) {
    // Older versions saved the 256 MiB default as a preference on every open.
    // Preserve explicit selections while allowing that mobile default to shrink.
    if (id === 'memory-budget' && mobile && !memoryChosen && value === '256') continue;
    if ([...$(id).options].some(option => option.value === value)) $(id).value = value;
  }
} catch { /* Use defaults when storage is unavailable. */ }
await work('Loading the available knowledge stores…', async () => {
  const response = await fetch('/api/stores');
  if (!response.ok) throw new Error('Could not load the available stores.');
  stores = await response.json();
  for (const store of stores) {
    const option = document.createElement('option'); option.value = store.id; option.textContent = store.name; $('store').append(option);
  }
  if (!stores.length) throw new Error('No knowledge stores are configured.');
  await connect();
});

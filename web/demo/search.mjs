const fold = text => text.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase();
const quote = text => `"${text.replaceAll('"', '""')}"`;
const like = text => text.replace(/[\\%_]/g, '\\$&');
export const pageSize = 20;

export function parseSearch(input) {
  const text = input.trim();
  if (text.length > 300) throw new Error('Keep your search under 300 characters.');
  const units = [...text.matchAll(/"([^"]+)"|(\S+)/gu)].map(match => {
    const value = match[1] ?? match[2];
    return { text: value.replaceAll('"', '').replace(/\*+$/, ''), prefix: !match[1] && value.endsWith('*') };
  }).filter(unit => /[\p{L}\p{N}]/u.test(unit.text));
  if (!units.length) throw new Error('Enter a word or phrase to search.');
  if (units.length > 20) throw new Error('Use at most 20 search terms.');
  return { text, units, title: units.map(unit => unit.text).join(' '),
    terms: units.flatMap(unit => unit.text.match(/[\p{L}\p{N}]+/gu) ?? []),
    expression: units.map(unit => quote(unit.text) + (unit.prefix ? '*' : '')).join(' AND ') };
}

export async function searchArticles(db, parsed, { page = 0, fts = true, mode = 'all' } = {}) {
  let expression = parsed.expression;
  if (mode === 'prefix') expression = parsed.units.map((unit, i) => quote(unit.text)
    + (unit.prefix || i === parsed.units.length - 1 ? '*' : '')).join(' AND ');
  if (mode === 'any') expression = parsed.units.map(unit => quote(unit.text) + (unit.prefix ? '*' : '')).join(' OR ');
  const source = fts ? 'entry_fts JOIN entries e ON e.id=entry_fts.rowid' : 'entries e JOIN blobs b ON b.id=e.id';
  const filter = fts ? 'entry_fts MATCH ? AND e.present=1' : "e.title LIKE ? ESCAPE '\\' AND e.present=1";
  const parameter = fts ? expression : `%${like(parsed.title)}%`;
  const count = (await db.query(`SELECT count(*) FROM ${source} WHERE ${filter}`, [parameter])).rows[0][0];
  if (!count && fts && mode === 'all' && !parsed.units.at(-1).prefix && parsed.units.at(-1).text.length >= 3) {
    return searchArticles(db, parsed, { page, fts, mode: 'prefix' });
  }
  if (!count && fts && mode !== 'any' && parsed.units.length > 1) {
    return searchArticles(db, parsed, { page, fts, mode: 'any' });
  }
  page = Math.max(0, Math.min(page, Math.ceil(Number(count) / pageSize) - 1));
  const ranked = fts ? "AND entry_fts.rank MATCH 'bm25(10.0,1.0)'" : '';
  const score = fts ? 'entry_fts.rank' : '0';
  // Rank the entire matching set before paging: exact title, title phrase at
  // the beginning, title phrase anywhere, then remaining content matches.
  // Within each tier, BM25 weights title hits 10x body hits.
  const result = await db.query(`SELECT e.id, e.title, e.path, e.canonical_url, ${score} AS score
    FROM ${source} WHERE ${filter} ${ranked}
    ORDER BY CASE WHEN e.title=? COLLATE NOCASE THEN 0
      WHEN e.title LIKE ? ESCAPE '\\' THEN 1
      WHEN instr(lower(e.title),lower(?))>0 THEN 2 ELSE 3 END,
      score, e.title COLLATE NOCASE, e.id LIMIT ? OFFSET ?`,
  [parameter, parsed.title, `${like(parsed.title)}%`, parsed.title, pageSize, page * pageSize]);
  const rows = result.rows.map(([id, title, path, url, score]) => ({ id, title, path, url, score }));
  if (rows.length) {
    // The FTS index is contentless. Fetch HTML only for this page's top hits.
    const content = await db.query(`SELECT id, content FROM blobs WHERE id IN (${rows.map(() => '?').join(',')})`, rows.map(row => row.id));
    const payloads = new Map(content.rows);
    for (const row of rows) row.content = payloads.get(row.id);
  }
  return { rows, count: Number(count), page, mode: fts ? mode : 'title' };
}

// Keep excerpts as text + match offsets. The UI creates <mark> nodes, never
// interprets content or query strings as result markup.
export function makeSnippet(text, terms, { length = 300 } = {}) {
  text = text.replace(/\s+/gu, ' ').trim();
  const wanted = [...new Set(terms.map(fold))];
  const matches = [];
  for (const word of text.matchAll(/[\p{L}\p{N}]+/gu)) {
    const normalized = fold(word[0]);
    const term = wanted.findIndex(value => normalized === value || (value.length >= 3 && normalized.startsWith(value)));
    if (term >= 0) matches.push({ start: word.index, end: word.index + word[0].length, term });
  }
  let start = 0;
  let best = -1;
  let left = 0;
  let right = 0;
  const frequencies = new Array(wanted.length).fill(0);
  // Favor an excerpt covering several different terms, then local density and
  // the article's opening. Inspect windows around hits, rather than just the lead.
  for (const match of matches) {
    const candidate = Math.max(0, match.start - 80);
    while (right < matches.length && matches[right].end <= candidate + length) frequencies[matches[right++].term]++;
    while (left < right && matches[left].start < candidate) frequencies[matches[left++].term]--;
    const distinct = frequencies.filter(frequency => frequency > 0).length;
    const score = distinct * 100 + Math.min(right - left, 20) - candidate / Math.max(text.length, 1);
    if (score > best) { best = score; start = candidate; }
  }
  if (start > 0) {
    const boundary = text.indexOf(' ', start);
    if (boundary >= 0 && boundary - start < 40) start = boundary + 1;
  }
  let end = Math.min(text.length, start + length);
  if (end < text.length) {
    const boundary = text.lastIndexOf(' ', end);
    if (boundary > end - 40) end = boundary;
  }
  const excerpt = text.slice(start, end);
  return { text: excerpt, before: start > 0, after: end < text.length,
    matches: matches.filter(hit => hit.start >= start && hit.end <= end).map(hit => ({ start: hit.start - start, end: hit.end - start })) };
}

export function plainText(content) {
  if (content === undefined || content === null) return '';
  const html = content instanceof Uint8Array ? new TextDecoder().decode(content) : String(content);
  const document = new DOMParser().parseFromString(html, 'text/html');
  for (const node of document.querySelectorAll('script,style,nav,iframe,object,embed,[hidden]')) node.remove();
  for (const node of document.querySelectorAll('p,div,li,h1,h2,h3,h4,h5,h6,td,th,br')) {
    node.append(document.createTextNode(' '));
  }
  return document.body.textContent.replace(/\s+/gu, ' ').trim();
}

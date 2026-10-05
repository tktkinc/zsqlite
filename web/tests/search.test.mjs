import assert from 'node:assert/strict';
import test from 'node:test';
import { parseSearch, makeSnippet } from '../demo/search.mjs';

test('search supports phrases and prefixes and treats unquoted operators as literal words', () => {
  assert.equal(parseSearch('"new york" jazz*').expression, '"new york" AND "jazz"*');
  assert.equal(parseSearch('Chicago OR jazz').expression, '"Chicago" AND "OR" AND "jazz"');
  assert.equal(parseSearch('café').title, 'café');
  assert.throws(() => parseSearch('***'), /word or phrase/);
});

test('snippets find matching text beyond the lead and favor distinct query terms', () => {
  const text = 'Ordinary introduction. '.repeat(30) + 'Chicago Chicago Chicago. '.repeat(3)
    + 'A city with jazz clubs and Chicago blues on every corner. ' + 'Further information. '.repeat(30);
  const snippet = makeSnippet(text, ['Chicago', 'jazz']);
  assert.ok(snippet.before && snippet.after);
  assert.match(snippet.text, /jazz clubs/);
  const marked = snippet.matches.map(({ start, end }) => snippet.text.slice(start, end));
  assert.ok(marked.includes('Chicago') && marked.includes('jazz'));
  assert.ok(snippet.text.length <= 300);
  const unicode = makeSnippet('Travel to CAFÉ and enjoy <script>plain text</script>.', ['cafe']);
  assert.equal(unicode.text.slice(unicode.matches[0].start, unicode.matches[0].end), 'CAFÉ');
});

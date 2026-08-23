'use strict';
/**
 * Local smoke test for llytpr-wl.v01nh — no external network required.
 * Boots the express app on an ephemeral port and verifies the HTTP surface,
 * plus a focused unit test of the HotChunks full-file RAM serve path.
 *
 * Run: npm run test:api
 * Made by Kakinie with llytpr-wl.v01nh TEAM. V1
 */
process.env.LLYTPR_NO_PROXY = '1'; // smoke は外部プロキシを触らない

const http = require('node:http');
const assert = require('node:assert');
const app = require('..');

async function req(port, path, headers = {}) {
  return new Promise((resolve, reject) => {
    const r = http.request({ host: '127.0.0.1', port, path, headers }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    r.on('error', reject);
    r.end();
  });
}

async function main() {
  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  const port = server.address().port;
  let failures = 0;
  const check = (name, cond, extra = '') => {
    if (cond) console.log('  ok  ' + name);
    else { failures++; console.error('  FAIL ' + name + ' ' + extra); }
  };

  // --- API surface
  const health = await req(port, '/api/health');
  check('GET /api/health -> 200 ok', health.status === 200 && JSON.parse(health.body).ok === true);

  const home = await req(port, '/');
  check('GET / -> 200 Vandal SPA', home.status === 200 && home.body.toString().includes('Vandal'));

  const fallback = await req(port, '/watch?v=abcdefghijk');
  check('SPA fallback for /watch', fallback.status === 200 && fallback.body.toString().includes('Vandal'));

  const badId = await req(port, '/api/stream?v=short');
  check('GET /api/stream bad id -> 400', badId.status === 400);

  const badApi = await req(port, '/api/nope');
  check('unknown api -> 404 json', badApi.status === 404 && JSON.parse(badApi.body).code === 'NOT_FOUND');

  const warmBad = await req(port, '/api/warm/xx');
  check('GET /api/warm bad id -> 400', warmBad.status === 400);

  // --- HotChunks: full-file RAM serve (shorts instant path)
  const { hotChunks } = require('../server/media');
  const hs = http.createServer((rq, rs) => {
    if (!hotChunks.serveIfHot('dQw4w9WgXcQ', '18', rq, rs)) { rs.writeHead(404); rs.end(); }
  });
  await new Promise((r) => hs.listen(0, '127.0.0.1', r));
  const hp = hs.address().port;

  // full でない状態: Range 先頭だけ 206
  hotChunks.map.set('dQw4w9WgXcQ:18', { buf: Buffer.alloc(256 * 1024, 7), exp: Date.now() + 60000, full: false });
  const part = await req(hp, '/v', { Range: 'bytes=0-1023' });
  check('hot partial range -> 206 + X-Hot-Cache', part.status === 206 && part.headers['x-hot-cache'] === 'HIT' && part.body.length === 1024);
  const noRangePartial = await req(hp, '/v');
  check('hot non-full + no Range -> passthrough (404 here)', noRangePartial.status === 404);

  // full 状態: Range 無しフル GET も RAM から 200
  hotChunks.map.set('dQw4w9WgXcQ:18', { buf: Buffer.alloc(256 * 1024, 9), exp: Date.now() + 60000, full: true });
  const whole = await req(hp, '/v');
  check('hot FULL no-range -> 200 complete file', whole.status === 200 && whole.headers['x-hot-cache'] === 'FULL' && whole.body.length === 256 * 1024);
  const tail = await req(hp, '/v', { Range: 'bytes=999999-' });
  check('hot range beyond buffer -> passthrough', tail.status === 404);

  hs.close();
  server.close();

  // --- innertube exports sanity
  const it = require('../server/innertube');
  check('innertube exports intact', ['search', 'getVideoFull', 'player', 'getStreamUrl', 'refreshStreamMap', 'getVisitorId'].every(k => typeof it[k] === 'function'));

  const { gocore } = require('../server/gocore');
  const { hedge } = require('../server/hedge');
  check('gocore fallback is quiet without CORE_ORIGIN', gocore.available() === false);
  check('node hedge export is a function', typeof hedge === 'function');

  if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
  console.log('\nall smoke checks passed');
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });

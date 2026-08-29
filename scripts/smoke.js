'use strict';
/**
 * Local smoke test for Vandal — no external network required.
 * Boots the express app on an ephemeral port and verifies the HTTP surface,
 * plus a focused unit test of the HotChunks full-file RAM serve path.
 *
 * Run: npm run test:api
 * Vandal Project — independent open project.
 */
process.env.VANDAL_NO_PROXY = '1'; // smoke は外部プロキシを触らない

const http = require('node:http');
const assert = require('node:assert');
const app = require('..');

async function req(port, path, headers = {}, method = 'GET', body = null) {
  return new Promise((resolve, reject) => {
    const r = http.request({ host: '127.0.0.1', port, path, headers, method }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    r.on('error', reject);
    r.end(body);
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
  const healthJson = JSON.parse(health.body);
  check('GET /api/health -> 200 ok', health.status === 200 && healthJson.ok === true);
  check('/api/health に CPU/メモリ/プロキシ/メッシュ/ティアが載る', !!(healthJson.cpu && healthJson.memory && healthJson.proxies && healthJson.mesh && healthJson.tier && healthJson.eventLoop));

  const healthz = await req(port, '/healthz');
  check('GET /healthz -> 200 {ok}', healthz.status === 200 && JSON.parse(healthz.body).ok === true);

  const healthHtml = await req(port, '/health', { Accept: 'text/html' });
  check('GET /health -> HTML ダッシュボード', healthHtml.status === 200 && healthHtml.body.toString().includes('Vandal instance health'));

  const meshState = await req(port, '/api/mesh/state');
  const meshJson = JSON.parse(meshState.body);
  check('GET /api/mesh/state -> ティア/自己/ピア構造', meshState.status === 200 && !!(meshJson.tiers && meshJson.self && Array.isArray(meshJson.peers) && typeof meshJson.shareProxies === 'number'));

  const settings0 = await req(port, '/api/settings');
  const s0 = JSON.parse(settings0.body);
  check('GET /api/settings -> mesh 設定が既定で有効', settings0.status === 200 && s0.meshEnabled === true && s0.meshPrivate === false && s0.meshDelegate === true);

  const setPriv = await req(port, '/api/settings', { 'Content-Type': 'application/json' }, 'POST', JSON.stringify({ meshPrivate: true }));
  check('POST /api/settings {meshPrivate} -> 永続化して mode:private', setPriv.status === 200 && JSON.parse(setPriv.body).mesh.mode === 'private');
  await req(port, '/api/settings', { 'Content-Type': 'application/json' }, 'POST', JSON.stringify({ meshPrivate: false }));

  const delegNoTok = await req(port, '/api/mesh/delegate/player', { 'Content-Type': 'application/json' }, 'POST', JSON.stringify({ videoId: 'dQw4w9WgXcQ' }));
  check('POST delegate (トークン無し) -> 403 MESH_FORBIDDEN', delegNoTok.status === 403 && JSON.parse(delegNoTok.body).code === 'MESH_FORBIDDEN');

  const delegBadTok = await req(port, '/api/mesh/delegate/player', { 'Content-Type': 'application/json', 'X-Vandal-Mesh': 'x' }, 'POST', JSON.stringify({ videoId: 'short' }));
  check('POST delegate (不正トークン) -> 403', delegBadTok.status === 403);

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

  // --- コメント無限取得バグの回帰（ルートシャドウ + トークン正規化）
  const cnextMissing = await req(port, '/api/comments/next');
  check('/api/comments/next (c 無し) -> 400 c required（:id に捕捉されない）', cnextMissing.status === 400 && JSON.parse(cnextMissing.body).error === 'c required');
  const cnextBad = await req(port, '/api/comments/next?c=tooshort');
  check('/api/comments/next (不正トークン) -> 200 ended:true で静かに停止（400 ループ不再）', cnextBad.status === 200 && JSON.parse(cnextBad.body).ended === true);
  // 二重 % エンコードされたトークン（実ログの %253D%253D 再現）も commentsNext の
  // 正規化で有効な base64 に復元されるため、上流へ 400 を持ち込まない。
  // （ネットワーク不要の検証: 不正形は ended:true で静かに停止することを上で確認済み）

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

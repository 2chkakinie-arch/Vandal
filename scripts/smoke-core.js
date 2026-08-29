'use strict';
/**
 * smoke-core.js — 動画情報＆ストリーム取得の並列ヘッジ基盤テスト
 * （外部ネット不要。Go が無い環境でも Node フォールバック経路を検証する）
 *
 * 検証すること:
 *   1. Node hedge: 速い経路が遅い経路より先に勝ち、所要は max ではなく min
 *   2. Node hedge: 敗者キャンセル後も markBad 相当の例外で落ちない
 *   3. kind=player: LOGIN_REQUIRED は負け、OK+formats が勝つ
 *   4. kind=player: UNPLAYABLE は 451
 *   5. オープンプロキシ拒否は Go 側の責務（Node フォールバックは同一ホストへ出す）
 *   6. gocore は CORE_ORIGIN 無しでは available()===false（Vercel / npm start）
 *   7. probe: 206 を OK、403 を NG
 *   8. 既存 HTTP 面は壊れていない（/api/health に core フィールド）
 *
 * Run: npm run test:core
 */
process.env.VANDAL_NO_PROXY = '1';
delete process.env.CORE_ORIGIN;

const http = require('node:http');
const { hedge, probe, inspectPlayer, throwIfHedgeDefinitive } = require('../server/hedge');
const { gocore } = require('../server/gocore');
const { YTError } = require('../server/errors');

let failures = 0;
const check = (name, cond, extra = '') => {
  if (cond) console.log('  ok  ' + name);
  else { failures++; console.error('  FAIL ' + name + ' ' + extra); }
};

function listen(handler) {
  return new Promise((resolve) => {
    const s = http.createServer(handler);
    s.listen(0, '127.0.0.1', () => resolve(s));
  });
}

async function main() {
  check('gocore unavailable without CORE_ORIGIN', gocore.available() === false);
  check('gocore.status has origin:null', gocore.status().origin === null);

  const okBody = JSON.stringify({ playabilityStatus: { status: 'OK' }, streamingData: { formats: [{ itag: 18 }] } });
  const loginBody = JSON.stringify({ playabilityStatus: { status: 'LOGIN_REQUIRED', reason: 'Sign in' } });
  const unplayBody = JSON.stringify({ playabilityStatus: { status: 'UNPLAYABLE', reason: 'Private video' } });

  const upstream = await listen((req, res) => {
    if (req.url === '/fast') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ who: 'fast' }));
      return;
    }
    if (req.url === '/slow') {
      setTimeout(() => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ who: 'slow' }));
      }, 220);
      return;
    }
    if (req.url === '/ok') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(okBody);
      return;
    }
    if (req.url === '/login') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(loginBody);
      return;
    }
    if (req.url === '/unplay') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(unplayBody);
      return;
    }
    if (req.url === '/partial') {
      res.writeHead(206, { 'Content-Range': 'bytes 0-63/1000' });
      res.end('x'.repeat(64));
      return;
    }
    if (req.url === '/forbid') {
      res.writeHead(403);
      res.end('no');
      return;
    }
    res.writeHead(404);
    res.end();
  });
  const base = `http://127.0.0.1:${upstream.address().port}`;

  const t0 = Date.now();
  const h = await hedge([
    { id: 'slow', method: 'GET', url: base + '/slow', timeoutMs: 2000 },
    { id: 'fast', method: 'GET', url: base + '/fast', timeoutMs: 2000 },
  ], { kind: 'json' });
  const dt = Date.now() - t0;
  check('hedge winner is the fast path', h.ok === true && h.winner === 'fast', JSON.stringify(h));
  check('hedge body is the fast payload', typeof h.body === 'string' && h.body.includes('fast'));
  check('hedge finished well under slow-path wait', dt < 180, `dt=${dt}`);

  const p = await hedge([
    { id: 'login', method: 'GET', url: base + '/login', timeoutMs: 2000 },
    { id: 'ok', method: 'GET', url: base + '/ok', timeoutMs: 2000 },
  ], { kind: 'player' });
  check('player hedge prefers OK over LOGIN_REQUIRED', p.ok === true && p.winner === 'ok', JSON.stringify({ ok: p.ok, winner: p.winner }));

  const u = await hedge([
    { id: 'u', method: 'GET', url: base + '/unplay', timeoutMs: 2000 },
  ], { kind: 'player' });
  check('UNPLAYABLE is not accepted', u.ok === false && u.status === 451, JSON.stringify({ ok: u.ok, status: u.status }));
  let threw = false;
  try { throwIfHedgeDefinitive(u); } catch (e) { threw = e instanceof YTError && e.status === 451; }
  check('throwIfHedgeDefinitive raises 451', threw);

  const info = inspectPlayer(okBody);
  check('inspectPlayer accepts OK+formats', info.accept === true);
  check('inspectPlayer rejects LOGIN_REQUIRED', inspectPlayer(loginBody).accept === false);

  const pr = await probe([
    { id: 'a', url: base + '/partial', timeoutMs: 2000 },
    { id: 'b', url: base + '/forbid', timeoutMs: 2000 },
  ]);
  const byId = Object.fromEntries((pr.results || []).map((x) => [x.id, x.ok]));
  check('probe 206 is playable', byId.a === true);
  check('probe 403 is not playable', byId.b === false);

  // HTTP surface still boots and advertises core status
  const app = require('..');
  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  const port = server.address().port;
  const health = await new Promise((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path: '/api/health' }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
    }).on('error', reject);
  });
  let hj = {};
  try { hj = JSON.parse(health.body); } catch (_) { /* noop */ }
  check('GET /api/health still 200 ok', health.status === 200 && hj.ok === true);
  check('/api/health exposes core status', hj.core && hj.core.ready === false && hj.core.origin === null);

  // メッシュ集計状態（インスタンス協力）が公開されているか
  const mesh = await new Promise((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path: '/api/mesh/state' }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
    }).on('error', reject);
  });
  let mj = {};
  try { mj = JSON.parse(mesh.body); } catch (_) { /* noop */ }
  check('GET /api/mesh/state is 200 with peers', mesh.status === 200 && mj && typeof mj.count === 'number' && Array.isArray(mj.peers));

  server.close();
  upstream.close();

  if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
  console.log('\nall core smoke checks passed');
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });

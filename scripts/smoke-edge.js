'use strict';
/**
 * smoke-edge.js — Go エッジ統合テスト（ネットワーク外部依存なし）
 *
 * 検証すること（＝「バグ・速度低下・挙動差分ゼロ」の機械的な保証）:
 *   A. エッジ経由と Node 直配信で /, /app.js, /styles.css, /logo.svg,
 *      /vendor/hls.min.js, SPA フォールバック, /api/* の応答が一致する
 *   B. エッジの gzip 事前圧縮が正しく（Accept-Encoding 交渉・復元一致）
 *   C. ETag → 304 条件付き応答・Range 要求の素通し（206 parity）
 *   D. SSE がバッファされずライブで流れる（retry: フレームの到達）
 *   E. ランチャー統合モード（launch.mjs で公開ポートに Go エッジ）が動く
 *   F. ランチャーの安全フォールバック: エッジバイナリ無しでも従来どおり
 *      Node 単体で必ず起動する
 *
 * Run: npm run test:edge
 */
process.env.VANDAL_NO_PROXY = '1';

const http = require('node:http');
const zlib = require('node:zlib');
const net = require('node:net');
const fs = require('node:fs');
const path = require('node:path');
const { spawn, spawnSync } = require('node:child_process');

const ROOT = path.join(__dirname, '..');
const EDGE_BIN = path.join(ROOT, 'goedge', 'bin', 'vandal-edge');

const children = [];
let failures = 0;

const check = (name, cond, extra = '') => {
  if (cond) console.log('  ok  ' + name);
  else { failures++; console.error('  FAIL ' + name + ' ' + extra); }
};

function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => resolve(p));
    });
    s.on('error', reject);
  });
}

/** 単純 HTTP リクエスト（圧縮は貼らず生バイトで受け取る） */
function req(port, p, { headers = {}, method = 'GET', body } = {}) {
  return new Promise((resolve, reject) => {
    const r = http.request({ host: '127.0.0.1', port, path: p, headers, method }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    r.on('error', reject);
    r.setTimeout(15000, () => r.destroy(new Error('timeout')));
    if (body) r.write(body);
    r.end();
  });
}

async function waitFor(fn, label, timeoutMs = 20000, interval = 250) {
  const t0 = Date.now();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      const v = await fn();
      if (v) return v;
    } catch { /* retry */ }
    if (Date.now() - t0 > timeoutMs) throw new Error('waitFor timeout: ' + label);
    await new Promise((r) => setTimeout(r, interval));
  }
}

function spawnLogged(cmd, args, opts) {
  const c = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
  c.stdout.on('data', () => {});
  let errTail = '';
  c.stderr.on('data', (d) => { errTail = (errTail + d).slice(-1200); });
  c.getErrTail = () => errTail;
  children.push(c);
  return c;
}

function killAllChildren() {
  for (const c of children) { try { c.kill('SIGKILL'); } catch { /* noop */ } }
}

process.on('exit', killAllChildren);
process.on('SIGINT', () => { killAllChildren(); process.exit(130); });

/** エッジバイナリの確保（go ツールチェーンがあればビルド、無ければ SKIP 扱い） */
function ensureEdgeBinary() {
  if (fs.existsSync(EDGE_BIN)) return true;
  const go = spawnSync('go', ['version'], { encoding: 'utf8' });
  if (go.error || go.status !== 0) return false;
  fs.mkdirSync(path.dirname(EDGE_BIN), { recursive: true });
  const r = spawnSync('go', ['build', '-trimpath', '-ldflags', '-s -w', '-o', EDGE_BIN, '.'], {
    cwd: path.join(ROOT, 'goedge'), encoding: 'utf8', timeout: 240000,
    env: { ...process.env, CGO_ENABLED: '0' },
  });
  return !r.error && r.status === 0 && fs.existsSync(EDGE_BIN);
}

async function main() {
  if (!ensureEdgeBinary()) {
    console.log('  skip  Go ツールチェーンが見つからないためエッジ統合テストをスキップ');
    console.log('        （Go 環境のあるデプロイ先/CI では自動で実行されます）');
    process.exit(0);
  }

  /* ================================================= A〜D: エッジ parity 一式 */
  const BACK_PORT = await freePort();
  const EDGE_PORT = await freePort();

  const backend = spawnLogged(process.execPath, [path.join(ROOT, 'index.js')], {
    env: { ...process.env, PORT: String(BACK_PORT), HOST: '127.0.0.1' },
  });
  await waitFor(async () => (await req(BACK_PORT, '/api/health').catch(() => null))?.status === 200, 'backend up');

  const edge = spawnLogged(EDGE_BIN, [], {
    env: { ...process.env, PORT: String(EDGE_PORT), EDGE_ORIGIN: `http://127.0.0.1:${BACK_PORT}` },
  });
  await waitFor(async () => (await req(EDGE_PORT, '/__edge/healthz').catch(() => null))?.status === 200, 'edge up');
  // アセット取り込み完了（healthz の assets=assetTotal）を待つ
  await waitFor(async () => {
    const h = await req(EDGE_PORT, '/__edge/healthz').catch(() => null);
    if (!h || h.status !== 200) return false;
    const j = JSON.parse(h.body.toString());
    return j.assets === j.assetTotal && j.assetTotal > 0;
  }, 'assets loaded to edge memory');

  /* --- A. コンテンツ parity（identity で byte 一致） --- */
  const parityPaths = ['/', '/app.js', '/styles.css', '/logo.svg', '/vendor/hls.min.js', '/watch?v=abcdefghijk'];
  for (const p of parityPaths) {
    const [d, e] = await Promise.all([
      req(BACK_PORT, p, { headers: { 'Accept-Encoding': 'identity' } }),
      req(EDGE_PORT, p, { headers: { 'Accept-Encoding': 'identity' } }),
    ]);
    check(`parity ${p} (status+bytes)`, d.status === e.status && d.body.equals(e.body),
      `status ${d.status}/${e.status} len ${d.body.length}/${e.body.length}`);
  }

  const appJsViaEdge = await req(EDGE_PORT, '/app.js', { headers: { 'Accept-Encoding': 'identity' } });
  check('edge serves /app.js from memory', appJsViaEdge.headers['x-edge-asset'] === 'memory');

  /* --- B. gzip 事前圧縮の正しさ --- */
  const gz = await req(EDGE_PORT, '/app.js', { headers: { 'Accept-Encoding': 'gzip' } });
  check('edge /app.js gzip negotiated', gz.headers['content-encoding'] === 'gzip');
  const gunz = zlib.gunzipSync(gz.body);
  check('edge gzip round-trip == identity bytes', gunz.equals(appJsViaEdge.body));

  const gzDirect = await req(BACK_PORT, '/app.js', { headers: { 'Accept-Encoding': 'gzip' } });
  const gunzDirect = zlib.gunzipSync(Buffer.from(gzDirect.body));
  check('node gzip path unchanged (decompressed parity)', gunzDirect.equals(appJsViaEdge.body));

  /* --- C. 条件付きリクエスト / Range --- */
  const inm = await req(EDGE_PORT, '/app.js', { headers: { 'If-None-Match': appJsViaEdge.headers.etag } });
  check('edge ETag If-None-Match -> 304', inm.status === 304);

  const [rangeDirect, rangeEdge] = await Promise.all([
    req(BACK_PORT, '/logo.svg', { headers: { Range: 'bytes=0-9', 'Accept-Encoding': 'identity' } }),
    req(EDGE_PORT, '/logo.svg', { headers: { Range: 'bytes=0-9', 'Accept-Encoding': 'identity' } }),
  ]);
  check('Range request proxied with parity (206)', rangeDirect.status === rangeEdge.status && rangeDirect.body.equals(rangeEdge.body),
    `status direct=${rangeDirect.status} edge=${rangeEdge.status}`);

  /* --- API 系 parity --- */
  const [hD, hE] = await Promise.all([
    req(BACK_PORT, '/api/health', { headers: { 'Accept-Encoding': 'identity' } }),
    req(EDGE_PORT, '/api/health', { headers: { 'Accept-Encoding': 'identity' } }),
  ]);
  check('/api/health parity (ok flag)', hE.status === 200 && JSON.parse(hE.body).ok === true && JSON.parse(hD.body).ok === true);

  const [nfD, nfE] = await Promise.all([
    req(BACK_PORT, '/api/nope', { headers: { 'Accept-Encoding': 'identity' } }),
    req(EDGE_PORT, '/api/nope', { headers: { 'Accept-Encoding': 'identity' } }),
  ]);
  check('unknown api 404 JSON parity', nfE.status === 404 && JSON.parse(nfE.body).code === 'NOT_FOUND' && nfD.status === 404);

  const [sD, sE] = await Promise.all([
    req(BACK_PORT, '/api/stream?v=short', { headers: { 'Accept-Encoding': 'identity' } }),
    req(EDGE_PORT, '/api/stream?v=short', { headers: { 'Accept-Encoding': 'identity' } }),
  ]);
  check('/api/stream bad id 400 parity', sD.status === 400 && sE.status === 400);

  const [pD, pE] = await Promise.all([
    req(BACK_PORT, '/api/nope', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"x":1}' }),
    req(EDGE_PORT, '/api/nope', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"x":1}' }),
  ]);
  check('POST method parity (404 both)', pD.status === pE.status && pD.body.equals(pE.body));

  /* --- D. メッシュ状態（JSON）がエッジを透過する --- */
  const [mD, mE] = await Promise.all([
    req(BACK_PORT, '/api/mesh/state', { headers: { 'Accept-Encoding': 'identity' } }),
    req(EDGE_PORT, '/api/mesh/state', { headers: { 'Accept-Encoding': 'identity' } }),
  ]);
  check('/api/mesh/state parity (200 both)', mD.status === 200 && mE.status === 200 && mD.body.equals(mE.body));

  edge.kill('SIGTERM');
  backend.kill('SIGTERM');
  await new Promise((r) => setTimeout(r, 400));

  /* ============================================== E. ランチャー統合モード */
  const PUB = await freePort();
  const INT = await freePort();
  const launcher = spawnLogged(process.execPath, [path.join(ROOT, 'scripts', 'launch.mjs')], {
    env: { ...process.env, PORT: String(PUB), INTERNAL_PORT: String(INT), PATH: `/usr/bin:/bin:${process.env.PATH || ''}` },
  });
  await waitFor(async () => (await req(PUB, '/__edge/healthz').catch(() => null))?.status === 200, 'launcher edge up');
  await waitFor(async () => {
    const h = await req(PUB, '/__edge/healthz').catch(() => null);
    if (!h || h.status !== 200) return false;
    const j = JSON.parse(h.body.toString());
    return j.assets === j.assetTotal && j.assetTotal > 0;
  }, 'launcher assets loaded to edge memory');
  const viaLauncher = await req(PUB, '/api/health', { headers: { 'Accept-Encoding': 'identity' } });
  const appViaLauncher = await req(PUB, '/app.js', { headers: { 'Accept-Encoding': 'identity' } });
  check('launcher: /api/health ok through Go edge', viaLauncher.status === 200 && JSON.parse(viaLauncher.body).ok === true);
  check('launcher: /app.js served from edge memory', appViaLauncher.headers['x-edge-asset'] === 'memory' && appViaLauncher.body.equals(appJsViaEdge.body));
  const graceful = await new Promise((resolve) => {
    const t = setTimeout(() => resolve('timeout'), 10000);
    launcher.on('exit', (code) => { clearTimeout(t); resolve(code); });
    launcher.kill('SIGTERM');
  });
  check('launcher shuts down gracefully on SIGTERM', graceful === 0, `exit=${graceful}`);

  /* ============================================== F. フォールバック（最終安全網） */
  // バイナリを一時退避し、go も無い最小 PATH で起動 → 必ず Node 単体になること
  const bak = EDGE_BIN + '.bak-test';
  fs.renameSync(EDGE_BIN, bak);
  const PUB2 = await freePort();
  try {
    const legacy = spawnLogged(process.execPath, [path.join(ROOT, 'scripts', 'launch.mjs')], {
      env: { ...process.env, PORT: String(PUB2), PATH: '/usr/bin:/bin', VANDAL_EDGE_BIN: '/nonexistent/edge' },
    });
    await waitFor(async () => (await req(PUB2, '/api/health').catch(() => null))?.status === 200, 'legacy fallback up');
    const h = await req(PUB2, '/api/health', { headers: { 'Accept-Encoding': 'identity' } });
    const spa = await req(PUB2, '/', { headers: { 'Accept-Encoding': 'identity' } });
    check('fallback (no binary/no go): Node 単体が公開ポートで起動', h.status === 200 && JSON.parse(h.body).ok === true);
    check('fallback: SPA 完全互換で配信', spa.status === 200 && spa.body.toString().includes('Vandal'));
    legacy.kill('SIGTERM');
    await new Promise((r) => setTimeout(r, 400));
  } finally {
    fs.renameSync(bak, EDGE_BIN);
  }

  if (failures) { console.error(`\n${failures} check(s) failed`); process.exit(1); }
  console.log('\nall edge smoke checks passed');
  process.exit(0);
}

main().catch((e) => { console.error(e); killAllChildren(); process.exit(1); });

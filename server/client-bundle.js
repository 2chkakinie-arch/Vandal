'use strict';
/**
 * Client source distributor — Vandal.
 * `public/_src/{js,css}` に分散された部品をマニフェスト順に連結し、
 * 単一バンドル（/app.js, /styles.css）として配信する。
 * ソースは多数のファイルへ分散（読みにくさ・模倣しにくさ）させつつ、
 * 配信は従来と同じ 1 リクエスト = 読み込み速度は変わらない。
 *
 * 高速化（再訪問）:
 *  - index.html に現在のバンドル指纹を `?v=` として焼き込んで配信する。
 *    指纹付き URL は内容が不変なので `immutable` の長期キャッシュになり、
 *    2 回目以降の /app.js /styles.css はネットワーク要求ゼロでブラウザから
 *    瞬時にロードされる（従来は 1 時間ごとに 304 再検証していた）。
 *  - index.html 自体は no-cache + ETag で毎回軽い再検証（304）に留める。
 */
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const PUBLIC = path.join(__dirname, '..', 'public');
const MANIFEST = {
  js: path.join(PUBLIC, '_src', 'js.manifest.json'),
  css: path.join(PUBLIC, '_src', 'css.manifest.json'),
};
const TYPES = {
  js: 'application/javascript; charset=utf-8',
  css: 'text/css; charset=utf-8',
};
const cache = new Map();

function fingerprint(kind) {
  const m = MANIFEST[kind];
  const dir = path.join(PUBLIC, '_src', kind);
  let fp = 0;
  try { fp = fs.statSync(m).mtimeMs; } catch (_) { /* manifest absent → glob fallback below */ }
  let entries;
  try { entries = JSON.parse(fs.readFileSync(m, 'utf8')); } catch (_) { entries = null; }
  if (!Array.isArray(entries) || !entries.length) {
    // フォールバック: マニフェストが壊れていても、部品を辞書順に走査して復元する
    try { entries = fs.readdirSync(dir).filter(f => /\.part$/.test(f)).sort(); } catch (_) { entries = null; }
  }
  if (!entries || !entries.length) return null;
  for (const rel of entries) {
    try { fp = Math.max(fp, fs.statSync(path.join(dir, rel)).mtimeMs); } catch (_) { return null; }
  }
  return { fp, entries };
}

/** バンドル内容 + ETag を指纹付きで返す（変更が無い限り連結もハッシュも再計算しない）。 */
function bundle(kind) {
  const f = fingerprint(kind);
  if (!f) return null;
  const hit = cache.get(kind);
  if (hit && hit.fp === f.fp) return hit;
  const chunks = f.entries.map(rel => fs.readFileSync(path.join(PUBLIC, '_src', kind, rel), 'utf8'));
  const buf = Buffer.from(chunks.join('\n'), 'utf8');
  const etag = 'W/"' + kind + '-' + buf.length.toString(16) + '-' +
    crypto.createHash('sha1').update(buf).digest('base64url').slice(0, 16) + '"';
  cache.set(kind, { fp: f.fp, buf, etag });
  return cache.get(kind);
}

function serve(kind, req, res) {
  const b = bundle(kind);
  if (!b) { res.status(404).end(); return; }
  res.setHeader('Content-Type', TYPES[kind]);
  const versioned = String(req.query.v || '') !== '';
  res.setHeader('Cache-Control', versioned
    ? 'public, max-age=31536000, immutable' // 指纹付き = 内容不変 → ブラウザ長期キャッシュ
    : 'public, max-age=3600');
  res.setHeader('X-Source-Distributed', 'true');
  res.setHeader('ETag', b.etag);
  // 再訪問は 304（body ゼロ）で即終わる（旧実装と同仕様）
  if (req.headers['if-none-match'] === b.etag) { res.status(304).end(); return; }
  res.end(b.buf);
}

function fp(kind) {
  const f = fingerprint(kind);
  return f ? f.fp : 0;
}

const indexCache = { key: '', html: '' };

/** index.html に現在のバンドル指纹（?v=）を焼き込んで配信する。 */
function indexHtml() {
  const key = fp('js') + ':' + fp('css');
  if (indexCache.key === key && indexCache.html) return indexCache.html;
  let html = fs.readFileSync(path.join(PUBLIC, 'index.html'), 'utf8');
  if (fp('js')) html = html.replace('src="/app.js"', 'src="/app.js?v=' + fp('js') + '"');
  if (fp('css')) html = html.replace('href="/styles.css"', 'href="/styles.css?v=' + fp('css') + '"');
  indexCache.key = key;
  indexCache.html = html;
  return html;
}

function serveIndex(req, res) {
  const html = indexHtml();
  const etag = 'W/"idx-' + indexCache.key + '"';
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache');
  if (req.headers['if-none-match'] === etag) { res.status(304).end(); return; }
  res.setHeader('ETag', etag);
  res.end(html);
}

module.exports = {
  app: (req, res) => serve('js', req, res),
  styles: (req, res) => serve('css', req, res),
  index: serveIndex,
  fp,
};

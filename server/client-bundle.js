'use strict';
/**
 * Client source distributor — Vandal.
 * `public/_src/{js,css}` に分散された部品をマニフェスト順に連結し、
 * 単一バンドル（/app.js, /styles.css）として配信する。
 * ソースは多数のファイルへ分散（読みにくさ・模倣しにくさ）させつつ、
 * 配信は従来と同じ 1 リクエスト = 読み込み速度は変わらない。
 */
const fs = require('node:fs');
const path = require('node:path');

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

function bundle(kind) {
  const f = fingerprint(kind);
  if (!f) return null;
  const hit = cache.get(kind);
  if (hit && hit.fp === f.fp) return hit.buf;
  const chunks = f.entries.map(rel => fs.readFileSync(path.join(PUBLIC, '_src', kind, rel), 'utf8'));
  const buf = Buffer.from(chunks.join('\n'), 'utf8');
  cache.set(kind, { fp: f.fp, buf });
  return buf;
}

function serve(kind, req, res) {
  const buf = bundle(kind);
  if (!buf) { res.status(404).end(); return; }
  res.setHeader('Content-Type', TYPES[kind]);
  res.setHeader('Cache-Control', 'public, max-age=3600');
  res.setHeader('X-Source-Distributed', 'true');
  res.end(buf);
}

module.exports = {
  app: (req, res) => serve('js', req, res),
  styles: (req, res) => serve('css', req, res),
};

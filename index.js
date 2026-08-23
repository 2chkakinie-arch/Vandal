'use strict';
/**
 * llytpr-wl.v01nh — ultra-fast YouTube frontend.
 * InnerTube API + rotating free-proxy transport + cipher solver.
 *
 * Runs standalone (node index.js, PORT env) and on Vercel (@vercel/node picks
 * up the exported express app).
 *
 * Made by Kakinie with llytpr-wl.v01nh TEAM. V1
 */
const path = require('node:path');
const express = require('express');
const compression = require('compression');

const { router } = require('./server/routes');
const { router: diagRouter } = require('./server/diag');
const { proxyManager } = require('./server/proxies');
const it = require('./server/innertube');
const { logbus } = require('./server/logbus');

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);

// gzip 圧縮（転送量 ~7割減）。動画中継・サムネ等のバイナリは除外する
// （再圧縮できない上に CPU 消費と初バイト遅延だけが増えるため）。
// 高速化: level 1 — JSON は level 6 と比べて圧縮率が ~5% 落ちるだけで
// CPU 時間が 1/3 以下になり、多数の小さな API 応答の初バイトが速くなる。
// /api/diag/logs (SSE) は圧縮するとバッファリングされてライブ性が死ぬため除外。
app.use(compression({
  level: 1,
  filter(req, res) {
    if (req.path.startsWith('/api/diag/')) return false; // SSE・テストは生で流す
    if (/^\/api\/(stream|thumb)\b/.test(req.path)) return false; // 動画/画像リレーは生で流す
    return compression.filter(req, res);
  },
}));

// API
app.use(diagRouter);
app.use(router);

// Static SPA assets
const pub = path.join(__dirname, 'public');
app.use(express.static(pub, {
  index: 'index.html',
  maxAge: '1h',
  setHeaders(res, filePath) {
    if (/\.(js|css)$/.test(filePath)) res.setHeader('Cache-Control', 'public, max-age=3600');
    if (/index\.html$/.test(filePath)) res.setHeader('Cache-Control', 'no-cache');
  },
}));

// SPA fallback for any other GET (hash routing anyway, but keep URLs pretty)
app.use((req, res, next) => {
  if (req.method !== 'GET' || req.path.startsWith('/api/')) return next();
  res.sendFile(path.join(pub, 'index.html'));
});

// JSON 404 for unknown API routes
app.use('/api', (req, res) => res.status(404).json({ error: 'not found', code: 'NOT_FOUND' }));

// Central error handler — never leak stack traces, never crash
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  if (res.headersSent) return;
  const status = err.status && err.status < 600 ? err.status : 500;
  res.status(status).json({ error: err.message || 'internal error', code: err.code || 'INTERNAL' });
});

process.on('unhandledRejection', (e) => {
  console.error('[unhandledRejection]', e?.message || e);
});
process.on('uncaughtException', (e) => {
  console.error('[uncaughtException]', e?.message || e);
});

module.exports = app;

/* istanbul ignore next */
if (require.main === module) {
  const port = Number(process.env.PORT) || 3000;
  // HOST env（既定 '0.0.0.0' = 従来どおり）。Go エッジ配下ではランチャーが
  // 127.0.0.1 を指定してローカル専用バインドにする（外部へは公開されない）。
  const host = process.env.HOST || '0.0.0.0';
  app.listen(port, host, () => {
    console.log(`[llytpr-wl.v01nh] listening on ${host}:${port}`);
    console.log('[llytpr-wl.v01nh] Made by Kakinie with llytpr-wl.v01nh TEAM. V1');
    // 高速化（初回待ち短縮）: 起動と同時に裏で3つ同時に暖機する —
    //   1. プロキシプール（L1 スキャン → L2/L3 認定へ直行）
    //   2. ホームフィード（最初の /api/home がいきなりキャッシュヒット）
    //   3. visitorData（検索・watch の初回リクエストに同梱できる状態にする）
    // ユーザーの最初のリクエストが来る頃には、重い初期化が全部終わっている。
    proxyManager.refresh().catch(() => {});
    it.home('all').catch(() => {});
    it.getVisitorId().catch(() => {});
    logbus.info('engine', 'サーバー起動 — バックグラウンド暖機開始（プロキシ/ホーム/visitor）');
  });
}

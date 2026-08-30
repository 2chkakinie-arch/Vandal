'use strict';
/**
 * Vandal — ultra-fast, self-contained YouTube frontend.
 * InnerTube API + rotating free-proxy transport + cipher solver +
 * instance cooperation mesh (shared living-proxy pool).
 *
 * Runs standalone (node index.js, PORT env) and on Vercel (@vercel/node picks
 * up the exported express app).
 *
 * Vandal Project — an independent open project.
 */
const path = require('node:path');
const express = require('express');
const compression = require('compression');

const { router } = require('./server/routes');
const { proxyManager } = require('./server/proxies');
const { mesh } = require('./server/mesh');
const { health } = require('./server/health');
const it = require('./server/innertube');
const { logbus } = require('./server/logbus');

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);

// gzip 圧縮（転送量 ~7割減）。動画中継・サムネ等のバイナリは除外する
// （再圧縮できない上に CPU 消費と初バイト遅延だけが増えるため）。
// 高速化: level 1 — JSON は level 6 と比べて圧縮率が ~5% 落ちるだけで
// CPU 時間が 1/3 以下になり、多数の小さな API 応答の初バイトが速くなる。
app.use(compression({
  level: 1,
  filter(req, res) {
    if (/^\/api\/(stream|thumb)\b/.test(req.path)) return false; // 動画/画像リレーは生で流す
    return compression.filter(req, res);
  },
}));

// テレメトリ（同時処理数 / RPS → /health とメッシュの hello に乗る）
app.use((req, res, next) => health.track(req, res, next));
// インスタンス協力メッシュ: 公開リクエストの Host から自インスタンスの公開 URL を
// 自動学習する（「デプロイして URL ができた時点で自動でメッシュリストに追加」の本体）。
app.use((req, res, next) => mesh.learnFromRequest(req, res, next));

// API
app.use(router);

// Static SPA assets（index.html は fingerprint 焼き込みテンプレートで配信 — 下記参照）
const pub = path.join(__dirname, 'public');
const bundle = require('./server/client-bundle');
app.use(express.static(pub, {
  index: false,
  maxAge: '1h',
  setHeaders(res, filePath) {
    if (/\.(js|css|svg)$/.test(filePath)) res.setHeader('Cache-Control', 'public, max-age=86400');
  },
}));

// SPA fallback for any other GET — index.html にはバンドル指纹（?v=）を焼き込んで
// 配信し、再訪問の JS/CSS をブラウザ長期キャッシュでゼロ往復にする
app.use((req, res, next) => {
  if (req.method !== 'GET' || req.path.startsWith('/api/')) return next();
  // 拡張子付きの未存在パス（削除済みアセットの古いキャッシュ参照など）は
  // SPA フォールバックに混ぜず 404 を返す（HTML 誤配信でブラウザを壊さない）
  if (/\.(png|jpe?g|gif|webp|svg|ico|js|css|woff2?|ttf|map|json|txt|html?)$/i.test(req.path)) return next();
  bundle.index(req, res);
});

// JSON 404 for unknown API routes
app.use('/api', (req, res) => res.status(404).json({ error: 'not found', code: 'NOT_FOUND' }));

// 未存在の静的アセットはプレーン 404
app.use((req, res) => res.status(404).type('text/plain').send('not found'));

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
  const server = app.listen(port, host, () => {
    console.log(`[vandal] listening on ${host}:${port}`);
    console.log('[vandal] Vandal Project — independent open project');
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
  // インスタンス協力メッシュ（WebSocket）— **デフォルトで自動参加**。
  // VANDAL_MESH_URL / VANDAL_MESH_PEERS は不要（任意の明示設定として残置）。
  //   - 自 URL はデプロイ環境変数 or 最初の公開リクエストから自動検出・永続化
  //   - ピアはゴシップ + 永続化レジストリで自動発見・自動再接続
  //   - data/config.json の meshPrivate:true / VANDAL_MESH_PRIVATE=1 で匿名参加
  //   - meshEnabled:false / VANDAL_MESH=0 で完全単独運用に戻せる
  mesh.attach(server);
  mesh.start();
}

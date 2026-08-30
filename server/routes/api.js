'use strict';
/** Core API routes (home / search / watch / comments / channel / playlist). */
const express = require('express');
const { wrap, warmDefault, warmComments } = require('./helpers');
const { proxyManager } = require('../proxies');
const { logbus } = require('../logbus');
const { engineConfig } = require('../config');
const it = require('../innertube');
const { mesh } = require('../mesh');

const router = express.Router();

// インスタンス協力メッシュの集計状況（インスタンスリスト / 共有プロキシ）。
// URL は既定で仮名化表示（デプロイ者の URL をそのまま訪問者へ晒さない）。
// ループバックからの ?full=1 のみ完全表示（デプロイ者自身の確認用）。
// ※ trust proxy 環境下の req.ip は X-Forwarded-For で偽装可能なので、
//   ソケットの実アドレスで判定する（強化）。
router.get('/api/mesh/state', (req, res) => {
  const full = require('../admin').isLoopback(req) && String(req.query.full || '') === '1';
  res.json(mesh.state({ full }));
});
router.get('/api/proxies', (req, res) => res.json(proxyManager.status()));
router.post('/api/proxies/refresh', wrap(async (req, res) => {
  const pool = await proxyManager.refresh({ force: true });
  res.json({ pool: pool.length });
}));
router.get('/api/home', wrap(async (req, res) => {
  const chip = String(req.query.chip || 'all');
  homeHits.set(chip, Date.now());
  const t0 = Date.now();
  const out = await it.home(chip);
  logbus.info('http', 'GET /api/home', { chip, ms: Date.now() - t0, items: out.items?.length || 0 });
  res.json(out);
}));

/**
 * 高速化（ホーム常時暖機）: 誰かが最近見たチップのキャッシュは、期限が切れる
 * 少し前に裏で再構築しておく。利用者がいる間は初回待ちが一度も復帰しない。
 * （見られていないチップは再構築しない = 無駄なプロキシ消費ゼロ）
 */
const homeHits = new Map(); // chip -> last request ts
setInterval(() => {
  if (!engineConfig.get('homeKeepWarm')) return;
  const now = Date.now();
  for (const [chip, ts] of [...homeHits]) {
    if (now - ts > 10 * 60 * 1000) { homeHits.delete(chip); continue; }
    const e = it.caches.api.map.get('home:' + chip);
    const expiring = !e || (e.exp !== 0 && e.exp - now < 2 * 60 * 1000);
    if (expiring) {
      it.caches.api.delete('home:' + chip);
      logbus.debug('engine', 'ホーム再暖機', { chip });
      it.home(chip).catch(() => {});
    }
  }
}, 60 * 1000).unref?.();

router.post('/api/home/personal', wrap(async (req, res) => {
  const p = req.body && typeof req.body === 'object' ? req.body : {};
  res.json(await it.personal(p));
}));

router.get('/api/shorts', wrap(async (req, res) => {
  res.json(await it.shortsFeed());
}));

router.post('/api/player/refresh', wrap(async (req, res) => {
  const v = String((req.body?.v ?? req.query.v) || '');
  if (!/^[\w-]{11}$/.test(v)) { res.status(400).json({ error: 'bad id' }); return; }
  it.invalidateVideo(v);
  const full = await it.getVideoFull(v, { fresh: true });
  res.json({ ok: true, playable: full.playable, playability: full.playability, source: full.streams?.source || null });
}));
router.get('/api/search', wrap(async (req, res) => {
  const q = String(req.query.q || '').slice(0, 200);
  if (!q) { res.status(400).json({ error: 'q required' }); return; }
  res.json(await it.search(q, { sp: req.query.sp ? String(req.query.sp) : undefined }));
}));
router.get('/api/search/next', wrap(async (req, res) => {
  const c = String(req.query.c || '');
  if (!c) { res.status(400).json({ error: 'c required' }); return; }
  res.json(await it.searchNext(c));
}));
router.get('/api/watch/:id', wrap(async (req, res) => {
  const id = String(req.params.id || '');
  if (!/^[\w-]{11}$/.test(id)) { res.status(400).json({ error: 'bad id' }); return; }
  const list = String(req.query.list || '');
  const t0 = Date.now();
  const data = await it.getVideoFull(id, list && /^[\w-]{1,64}$/.test(list) ? { playlistId: list } : {});
  res.json(data);
  logbus.info('http', 'GET /api/watch/:id', {
    v: id, ms: Date.now() - t0, playable: !!data.playable,
    source: data.streams?.source || null, direct: !!data.streams?.direct?.url,
  });
  // speculative warm: by the time the user presses play, the first bytes
  // of the default 360p stream are already in RAM.
  if (data.playable) warmDefault(id);
  // コメント先行取得（高速化: コメント欄オープン時はキャッシュヒットで即応答）
  warmComments(id, data.commentsToken);
}));

router.get('/api/warm/:id', wrap(async (req, res) => {
  const id = String(req.params.id || '');
  if (!/^[\w-]{11}$/.test(id)) { res.status(400).json({ error: 'bad id' }); return; }
  warmDefault(id);
  res.json({ ok: true });
}));
// 【重要】/next は /:id より先に登録する。Express は登録順にマッチするため、
// 旧実装のように /:id を先に置くと /api/comments/next が :id='next' として
// 捕捉され「bad id」の 400 になっていた = コメントの無限取得が必ず 400 で
// 死ぬ本バグの根だった（トークンの二重エンコードは二次的な問題）。
router.get('/api/comments/next', wrap(async (req, res) => {
  const c = String(req.query.c || '');
  if (!c) { res.status(400).json({ error: 'c required' }); return; }
  // commentsNext 内で % エンコード剥がし・形検査・失効時の静かな終端化まで済む
  res.json(await it.commentsNext(c));
}));
router.get('/api/comments/:id', wrap(async (req, res) => {
  const id = String(req.params.id || '');
  if (!/^[\w-]{11}$/.test(id)) { res.status(400).json({ error: 'bad id' }); return; }
  // 高速化: watch 応答に同梱された commentsToken を再利用すると、トークン発見の
  // 往復が丸ごと消えてコメント取得が約半分のレイテンシになる。
  const token = req.query.token ? String(req.query.token).slice(0, 8000) : undefined;
  const t0 = Date.now();
  const out = await it.comments(id, token);
  logbus.info('http', 'GET /api/comments/:id', { v: id, ms: Date.now() - t0, n: (out.comments || []).length, tokenReuse: !!token });
  res.json(out);
}));
router.get('/api/channel/:id', wrap(async (req, res) => {
  const raw = req.params.id || '';
  res.json(await it.channel(raw, {
    params: req.query.params ? String(req.query.params) : undefined,
    continuation: req.query.c ? String(req.query.c) : undefined,
  }));
}));
// 【重要】/next・/panel-next は /:id より先に登録（/api/comments/next と同一根のバグ防止）。
router.get('/api/playlist/next', wrap(async (req, res) => {
  const c = String(req.query.c || '');
  if (!c) { res.status(400).json({ error: 'c required' }); return; }
  res.json(await it.playlistNext(c));
}));

router.get('/api/playlist/panel-next', wrap(async (req, res) => {
  const c = String(req.query.c || '');
  if (!c) { res.status(400).json({ error: 'c required' }); return; }
  res.json(await it.panelNext(c));
}));
router.get('/api/playlist/:id', wrap(async (req, res) => {
  res.json(await it.playlist(String(req.params.id || '').replace(/[^\w-]/g, '')));
}));
router.get('/api/suggest', wrap(async (req, res) => {
  const q = String(req.query.q || '').slice(0, 100);
  if (!q) { res.json({ suggestions: [] }); return; }
  res.json(await it.suggest(q));
}));
router.get('/api/resolve/:target', wrap(async (req, res) => {
  res.json({ id: await it.resolveChannelId(String(req.params.target)) });
}));

/* ------------------------------------------------------- settings (settings.json 相当) */

/** デプロイ後もコードを触らずに変えられる実行時設定。data/config.json に永続化。
 *
 * 【所有権の修正】旧実装は POST が無認証で、**任意の訪問者**が
 * meshPrivate（プライベート化）/ meshEnabled（メッシュ脱退）/ proxyMode='direct'
 * （プロキシ全停止）などを書き換えられ、永続化されていた。インスタンスの
 * 運用方針はデプロイ者のもの — 書き込みは adminGate（ループバック /
 * VANDAL_ADMIN_TOKEN / 初回クラームトークン）で保護し、訪問者は読み取り専用。
 * 環境変数 VANDAL_MESH_PRIVATE=1 / VANDAL_MESH=0 も引き続き初期値として効く。 */
const adminGate = require('../admin');

function settingsMeshSummary() {
  return {
    mode: mesh.isPrivate ? 'private' : 'public',
    enabled: !!engineConfig.get('meshEnabled'),
    aliveCount: mesh.peerCount(),
    healthyPeers: mesh.healthyPeerCount(),
    shareProxies: proxyManager.shareCount(40),
    selfUrl: mesh.selfUrl || null,
  };
}

router.get('/api/settings', (req, res) => {
  const c = engineConfig.get();
  res.json({
    ...c,
    admin: adminGate.isAdmin(req),          // このクライアントが設定を書けるか
    adminStatus: adminGate.status(),        // 未クラーム / env 運用 / クラーム済み
    mesh: settingsMeshSummary(),           // 軽量サマリ（旧: 全ピア一覧を毎回構築していた）
  });
});
router.post('/api/settings', wrap(async (req, res) => {
  if (!adminGate.isAdmin(req)) {
    res.status(403).json({ error: 'この設定を変更できるのはインスタンスの管理者だけです', code: 'ADMIN_REQUIRED' });
    return;
  }
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  // mesh 系の変更はメッシュへ即反映（private 化した瞬間から URL を外へ出さない）
  const out = engineConfig.set(body);
  if (body.meshPrivate !== undefined || body.meshEnabled !== undefined) {
    logbus.info('mesh', 'メッシュ設定を変更', { private: out.meshPrivate, enabled: out.meshEnabled });
    mesh.nudge?.();
  }
  res.json({ ...out, admin: true, mesh: settingsMeshSummary() });
}));

/* ---------------------------------------------------- admin claim / unlock */

/** 初回クラーム: まだ誰も管理していない場合のみトークンを発行（1 回限り）。
 *  以降は POST /api/admin/verify でトークン照合（X-Vandal-Admin ヘッダ用）。 */
router.post('/api/admin/claim', wrap(async (req, res) => {
  const out = adminGate.claim();
  if (out.ok) {
    logbus.info('engine', '管理権が初期化されました（初回クラーム）');
    res.json({ ok: true, token: out.token });
    return;
  }
  res.status(409).json({ ok: false, error: out.reason === 'env' ? 'このインスタンスは VANDAL_ADMIN_TOKEN で運用されています' : '既に誰かが管理権を取得済みです', code: out.reason === 'env' ? 'ADMIN_ENV' : 'ADMIN_CLAIMED' });
}));
router.post('/api/admin/verify', wrap(async (req, res) => {
  const token = String(req.body?.token || '');
  res.json({ ok: adminGate.verify(token) });
}));

/* ------------------------------------------- mesh delegate（ピアからの作業委譲を受ける側） */

/**
 * 上位ティアのインスタンスが「メタ情報取得（player 発行）」を委譲されるときの受口。
 *  - X-Vandal-Mesh ヘッダ（メッシュの hello で交換済みのケイパビリティトークン）が必須
 *    → インターネットからの野良 POST を弾く（メッシュ参加者だけが使える）。
 *  - 同時実行とレートを制限して、自分のユーザー体験を犠牲にしない。
 */
const _delegateJobs = { live: 0, windowStart: 0, accepted: 0 };
router.post('/api/mesh/delegate/player', wrap(async (req, res) => {
  if (!mesh.checkDelegateToken(req)) { res.status(403).json({ error: 'not a mesh peer', code: 'MESH_FORBIDDEN' }); return; }
  if (!engineConfig.get('meshDelegate')) { res.status(503).json({ error: 'delegate disabled', code: 'DELEGATE_OFF' }); return; }
  const v = String(req.body?.videoId || '');
  if (!/^[\w-]{11}$/.test(v)) { res.status(400).json({ error: 'bad id', code: 'BAD_ID' }); return; }
  const now = Date.now();
  if (now - _delegateJobs.windowStart > 60000) { _delegateJobs.windowStart = now; _delegateJobs.accepted = 0; }
  if (_delegateJobs.accepted >= 30 || _delegateJobs.live >= 4) {
    res.status(429).json({ error: 'busy', code: 'DELEGATE_BUSY' });
    return;
  }
  _delegateJobs.accepted += 1;
  _delegateJobs.live += 1;
  try {
    const p = await it.player(v);
    res.json({
      ok: true,
      player: {
        videoId: v,
        title: p.title, author: p.author, channelId: p.channelId,
        viewCount: p.viewCount, lengthSeconds: p.lengthSeconds, isLive: p.isLive, isShort: p.isShort,
        publishDate: p.publishDate, uploadDate: p.uploadDate, category: p.category, keywords: p.keywords,
        progressive: p.progressive, videos: p.videos, audios: p.audios,
        hls: p.hls, expiresInSeconds: p.expiresInSeconds,
        __urlMap: p.__urlMap,
        source: 'peer',
      },
      proxies: proxyManager.exportProxies(40),
    });
  } finally {
    _delegateJobs.live -= 1;
  }
}));

module.exports = { router };

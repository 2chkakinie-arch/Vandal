'use strict';
/** Core API routes (home / search / watch / comments / channel / playlist). */
const express = require('express');
const { wrap, warmDefault, warmComments } = require('./helpers');
const { proxyManager } = require('../proxies');
const { logbus } = require('../logbus');
const { engineConfig } = require('../config');
const it = require('../innertube');
const { gocore } = require('../gocore');
const { mesh } = require('../mesh');

const router = express.Router();

router.get('/api/health', (req, res) => res.json({ ok: true, ts: Date.now(), proxies: proxyManager.pool.length, core: gocore.status(), mesh: mesh.isActive }));

// インスタンス協力メッシュの集計状況（公開 URL / 共有プロキシ）。
router.get('/api/mesh/state', (req, res) => res.json(mesh.state()));
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
router.get('/api/comments/next', wrap(async (req, res) => {
  const c = String(req.query.c || '');
  if (!c) { res.status(400).json({ error: 'c required' }); return; }
  res.json(await it.commentsNext(c));
}));
router.get('/api/channel/:id', wrap(async (req, res) => {
  const raw = req.params.id || '';
  res.json(await it.channel(raw, {
    params: req.query.params ? String(req.query.params) : undefined,
    continuation: req.query.c ? String(req.query.c) : undefined,
  }));
}));
router.get('/api/playlist/:id', wrap(async (req, res) => {
  res.json(await it.playlist(String(req.params.id || '').replace(/[^\w-]/g, '')));
}));
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
router.get('/api/suggest', wrap(async (req, res) => {
  const q = String(req.query.q || '').slice(0, 100);
  if (!q) { res.json({ suggestions: [] }); return; }
  res.json(await it.suggest(q));
}));
router.get('/api/resolve/:target', wrap(async (req, res) => {
  res.json({ id: await it.resolveChannelId(String(req.params.target)) });
}));

module.exports = { router };

'use strict';
/**
 * InnerTube engine for Vandal.
 * Raw youtubei/v1 client with rotating free-proxy transport, visitor-data
 * handling, cipher repair via the vendored yt-dlp solver, and tolerant parsers
 * for every surface the app needs (search / watch / comments / channel /
 * playlist / home). Every response shape is probed, not assumed — YouTube
 * changes payload layouts constantly.
 *
 * Vandal Project — independent open project.
 */
const { proxyManager } = require('./proxies');
const { sigSolver } = require('./solver');
const { piped } = require('./piped');
const { logbus } = require('./logbus');
const { engineConfig } = require('./config');
const { caches, CACHE_MIN } = require('./caches');
const { YTError } = require('./errors');
const { API_KEY, HOST_WEB, CLIENTS, PLAYER_CHAIN, probePlayable, sleep, rawFetch, transports, transportsForUrls, callApi, ENDPOINT_LOG_CH, tryParse, fetchText, getVisitorId, getVisitorIdFast } = require('./transport');
const { deepFind, textOf, bestThumb, extractItems } = require('./parse');
const { gocore } = require('./gocore');
const { throwIfHedgeDefinitive } = require('./hedge');






/* ---------------- runtime persistence (cold-boot acceleration) ----------------
 * goodCombo / stream maps / 直結判定(pd) をディスクに退避し、プロセス再起動や
 * Vercel コールドブートでも「初回だけ遅い」を消す。期限切れは読み出し時に捨てる。 */
const fs = require('node:fs');
const nodePath = require('node:path');
const RT_DIR = process.env.VERCEL ? '/tmp/vandal-data' : nodePath.join(__dirname, '..', 'data');
const RT_FILE = nodePath.join(RT_DIR, 'runtime-cache.json');
const rt = { goodCombo: null, streams: {}, home: {} }; // streams['m:'+id]={e,exp} / ['p:'+id]={e,exp}
try { fs.mkdirSync(RT_DIR, { recursive: true }); } catch (_) { /* noop */ }
try {
  const raw = JSON.parse(fs.readFileSync(RT_FILE, 'utf8'));
  if (raw && typeof raw === 'object') {
    if (raw.goodCombo) rt.goodCombo = raw.goodCombo;
    if (raw.streams && typeof raw.streams === 'object') {
      const now = Date.now();
      for (const [k, v] of Object.entries(raw.streams)) {
        if (v && typeof v.exp === 'number' && v.exp > now) rt.streams[k] = v;
      }
    }
    // 高速化: ホームの前回表示内容をディスクから復元（stale-while-revalidate）。
    // コールドブート直後の初回 /api/home が「プロキシ温まるまで白画面」だったのを、
    // 最大30分前のスナップショットで即描画し、裏で最新化が走る。
    if (raw.home && typeof raw.home === 'object') {
      const now = Date.now();
      for (const [chip, snap] of Object.entries(raw.home)) {
        if (snap && Array.isArray(snap.data?.items) && snap.data.items.length
          && now - (snap.savedAt || 0) < 30 * 60 * 1000) {
          rt.home[chip] = snap;
          caches.api.set('home:' + chip, snap.data, 8 * 60 * 1000); // 短めの再検証TTL
        }
      }
    }
  }
} catch (_) { /* first boot */ }
let _rtTimer = null;
function rtSave() {
  clearTimeout(_rtTimer);
  _rtTimer = setTimeout(() => {
    try {
      const keys = Object.keys(rt.streams);
      if (keys.length > 500) { // 古い順に間引く
        keys.sort((a, b) => (rt.streams[a].exp - rt.streams[b].exp));
        for (const k of keys.slice(0, keys.length - 500)) delete rt.streams[k];
      }
      const homeKeys = Object.keys(rt.home);
      if (homeKeys.length > 8) { // チップ別スナップショットは最大8種保持
        homeKeys.sort((a, b) => (rt.home[b].savedAt || 0) - (rt.home[a].savedAt || 0));
        for (const k of homeKeys.slice(8)) delete rt.home[k];
      }
      fs.writeFileSync(RT_FILE, JSON.stringify({ savedAt: Date.now(), goodCombo: rt.goodCombo, streams: rt.streams, home: rt.home }));
    } catch (_) { /* read-only */ }
  }, 700);
}
function streamMapGet(id) {
  const m = caches.streams.get('map:' + id);
  if (m) return m;
  const d = rt.streams['m:' + id];
  if (d && d.exp > Date.now()) { caches.streams.set('map:' + id, d.e, d.exp - Date.now()); return d.e; }
  return null;
}
function streamMapSet(id, entry) {
  caches.streams.set('map:' + id, entry, entry.ttlMs);
  rt.streams['m:' + id] = { e: entry, exp: Date.now() + entry.ttlMs };
  rtSave();
  if (entry?.map) {
    gocore.pin({
      videoId: id,
      map: entry.map,
      proxyUrl: entry.proxyUrl || '',
      exp: Date.now() + (entry.ttlMs || 5 * 3600 * 1000),
      warmItag: '18',
    }).catch(() => {});
  }
}
function pdGet(id) {
  const m = caches.streams.get('pd:' + id);
  if (m) return m;
  const d = rt.streams['p:' + id];
  if (d && d.exp > Date.now()) { caches.streams.set('pd:' + id, d.e, d.exp - Date.now()); return d.e; }
  return null;
}
function pdSet(id, pd, ttlMs) {
  caches.streams.set('pd:' + id, pd, ttlMs);
  rt.streams['p:' + id] = { e: pd, exp: Date.now() + ttlMs };
  rtSave();
}
function streamInvalidate(id) {
  caches.streams.delete('map:' + id);
  caches.streams.delete('pd:' + id);
  delete rt.streams['m:' + id];
  delete rt.streams['p:' + id];
  rtSave();
  gocore.unpin(id).catch(() => {});
}









/** HLS manifest URL for live streams (transport-pinned, cached). */
async function getHls(videoId) {
  const key = 'hls:' + videoId;
  const cached = caches.streams.get(key) || caches.streams.getStale(key);
  if (cached?.url) return cached;
  const p = await player(videoId);
  const entry = {
    url: p.hls || null,
    proxyUrl: p.__transport?.kind === 'proxy' ? p.__transport.url : null,
  };
  caches.streams.set(key, entry, 2 * 3600 * 1000);
  return entry;
}



/* ------------------------------------------------------------------- search */

async function search(query, { sp, hl = 'ja', gl = 'JP', fresh = false } = {}) {
  const key = `s:${query}:${sp || ''}:${hl}${gl}`;
  if (fresh) caches.api.delete(key);
  return caches.api.wrap(key, 10 * CACHE_MIN, async () => {
    const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
    const spanDone = logbus.span('meta', 'search', { q: String(query).slice(0, 60) });
    const payload = { query };
    if (sp) payload.params = sp;
    let lastSearchError = null;
    try {
      const res = await callApi('search', payload, CLIENTS.WEB, { visitorId });
      const root = res?.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents
        || res?.onResponseReceivedCommands?.[0]?.appendContinuationItemsAction?.continuationItems
        || res;
      const { items, continuation } = extractItems(root);
      if (items.length || sp) { spanDone({ ok: true, items: items.length, via: 'innertube' }); return { query, items, continuation }; }
    } catch (e) {
      // fall through to the youtube-search-api backup below
      lastSearchError = e;
    }
    // ---- backup path: youtube-search-api (scrapes the public results page).
    // Handy when the datacenter IP is served bot-walls for youtubei search.
    try {
      const yts = require('youtube-search-api');
      const out = await yts.GetListByKeyword(query, false, 20, [{ type: 'video' }]);
      const items = (out?.items || []).map(v => v?.id ? ({
        kind: 'video',
        id: v.id,
        url: '/watch?v=' + v.id,
        title: v.title || '',
        thumb: v.thumbnail?.thumbnails?.slice(-1)?.[0]?.url || `https://i.ytimg.com/vi/${v.id}/hqdefault.jpg`,
        duration: v.length?.simpleText || '',
        channel: v.channelTitle || '',
        metaTop: [],
        metaBottom: [],
      }) : null).filter(Boolean);
      if (items.length) return { query, items, continuation: null, via: 'youtube-search-api' };
    } catch (_) { /* both paths failed */ }
    throw lastSearchError || new YTError('search unavailable', 502);
  });
}

async function searchNext(continuation) {
  return caches.api.wrap('sn:' + continuation, 10 * CACHE_MIN, async () => {
    const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
    const res = await callApi('search', { continuation }, CLIENTS.WEB, { visitorId });
    const roots = res?.onResponseReceivedCommands || res?.onResponseReceivedEndpoints || [];
    const items = [];
    let cont = null;
    for (const ep of roots) {
      const bag = ep.appendContinuationItemsAction?.continuationItems
        || ep.reloadContinuationItemsCommand?.continuationItems
        || ep.appendContinuationItemsAction?.continuationItems || [];
      const r = extractItems(bag);
      items.push(...r.items);
      if (r.continuation) cont = r.continuation;
    }
    return { items, continuation: cont };
  });
}

/* -------------------------------------------------------------------- watch */

function parsePrimaryInfo(c) {
  const pri = deepFind(c, 'videoPrimaryInfoRenderer', 1)?.[0] || {};
  const sec = deepFind(c, 'videoSecondaryInfoRenderer', 1)?.[0] || {};
  const owner = deepFind(sec, 'videoOwnerRenderer', 1)?.[0] || {};
  const title = textOf(pri.title);
  const viewCount = textOf(pri.viewCount?.videoViewCountRenderer?.viewCount);
  const dateText = textOf(pri.dateText);
  // likes: the count rides in an accessibilityText like
  // "他 109,633 人もこの動画を高く評価しました" (entity-driven in 2026 payloads)
  let likeCount = '';
  try {
    const blob = JSON.stringify(c);
    const m = blob.match(/"accessibilityText":\s*"(?:[^"]*?\s)?([\d,\.万億千百]+)\s*(?:人もこの動画を高く評価|件の高評価)/)
      || blob.match(/"defaultText":\s*\{[^}]*"content":\s*"([\d,\.万億]+)"[^}]*\}[^}]*"accessibilityText":\s*"[^"]*高評価/);
    if (m) likeCount = m[1];
  } catch (_) { /* optional metric */ }
  const channelId = owner?.navigationEndpoint?.browseEndpoint?.browseId
    || owner?.title?.runs?.[0]?.navigationEndpoint?.browseEndpoint?.browseId || '';
  // description
  let description = textOf(sec.attributedDescription?.content)
    || textOf(deepFind(sec, 'expandableVideoDescriptionBodyRenderer', 1)?.[0]?.descriptionBodyText)
    || '';
  return {
    title, viewCount, dateText, likeCount, description,
    channel: {
      id: channelId,
      name: textOf(owner.title),
      subs: textOf(owner.subscriberCountText),
      avatar: bestThumb(owner.thumbnail?.thumbnails),
      verified: !!deepFind(owner, 'badge', 4)?.some?.(b => /CHECK|BADGE_STYLE_TYPE_VERIFIED/.test(JSON.stringify(b))),
    },
  };
}

async function watchNext(videoId, { hl = 'ja', gl = 'JP', playlistId, fresh = false } = {}) {
  const cacheKey = 'w:' + videoId + hl + gl + (playlistId ? ':' + playlistId : '');
  if (fresh) caches.api.delete(cacheKey);
  return caches.api.wrap(cacheKey, 10 * CACHE_MIN, async () => {
    const spanDone = logbus.span('meta', 'watchNext (動画メタ)', { v: videoId });
    const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
    const payload = { videoId, contentCheckOk: true, racyCheckOk: true };
    if (playlistId) payload.playlistId = playlistId;
    const res = await callApi('next', payload, CLIENTS.WEB, { visitorId });
    const watch = res?.contents?.twoColumnWatchNextResults || {};
    const results = watch?.results?.results?.contents || [];
    const meta = parsePrimaryInfo(results);
    // comments entry
    let commentsToken = null;
    let commentsCount = '';
    const panels = res?.engagementPanels || [];
    for (const p of panels) {
      const r = p?.engagementPanelSectionListRenderer;
      if (r?.panelIdentifier === 'engagement-panel-comments-section') {
        commentsToken = deepFind(r, 'continuationCommand', 4)?.map(x => x.token).find(Boolean) || null;
      }
    }
    const hd = deepFind(res, 'commentsEntryPointHeaderRenderer', 1)?.[0];
    if (hd) commentsCount = textOf(hd.commentCount);
    // related
    const sec = watch?.secondaryResults?.secondaryResults?.results || [];
    const { items: related, continuation: relatedContinuation } = extractItems(sec);
    // autoplay target
    const autoplay = deepFind(watch?.autoplay?.autoplay, 'watchEndpoint', 1)?.[0]?.videoId || null;
    // 本家同等のプレイリストパネル (mix含む)
    // - 通常プレイリスト: playlistPanelRenderer ラッパあり
    // - ミックス (RD…): contents.twoColumnWatchNextResults.playlist.playlist に素の contents が直置き (2026〜)
    const panelR = deepFind(watch?.playlist || res, 'playlistPanelRenderer', 1)?.[0]
      || (Array.isArray(watch?.playlist?.playlist?.contents) ? watch.playlist.playlist : null);
    let panel = null;
    if (panelR) {
      const pr = extractItems(panelR.contents || []);
      const panelVideos = pr.items.filter(i => i.kind === 'video' && i.id);
      panel = {
        title: textOf(panelR.titleText) || textOf(panelR.title) || '',
        owner: textOf(panelR.ownerName) || textOf(panelR.shortBylineText) || textOf(panelR.longBylineText) || '',
        totalText: textOf(panelR.totalVideosText) || '',
        items: panelVideos,
        continuation: pr.continuation,
        currentIndex: Number.isInteger(panelR.currentIndex) ? panelR.currentIndex : panelVideos.findIndex(i => i.selected),
        isInfinite: !!panelR.isInfinite,
      };
      if (panel.currentIndex < 0) panel.currentIndex = 0;
    }
    spanDone({ ok: true, related: related.length, hasCommentsToken: !!commentsToken, panel: !!panel });
    return {
      videoId,
      ...meta,
      related,
      relatedContinuation,
      commentsToken,
      commentsCount,
      autoplay,
      panel,
    };
  });
}

/** continuation of the watch playlist panel (goes through the `next` endpoint) */
async function panelNext(continuation) {
  // キーは全文（末尾24文字はトークン中盤の共通パディングで衝突し得る）
  return caches.api.wrap('pn:' + continuation, 10 * CACHE_MIN, async () => {
    const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
    const res = await callApi('next', { continuation }, CLIENTS.WEB, { visitorId });
    const eps = res?.onResponseReceivedEndpoints || res?.onResponseReceivedCommands || [];
    let items = [], cont = null;
    for (const ep of eps) {
      const bag = ep.appendContinuationItemsAction?.continuationItems || ep.reloadContinuationItemsCommand?.continuationItems || [];
      const r = extractItems(bag);
      items = items.concat(r.items.filter(i => i.kind === 'video' && i.id));
      if (r.continuation) cont = r.continuation;
    }
    return { items, continuation: cont };
  });
}

/* ------------------------------------------------------------------ streams */

function normalizeFormat(f, videoId) {
  const mime = f.mimeType || '';
  const isVideo = mime.startsWith('video/');
  const isAudio = mime.startsWith('audio/');
  return {
    itag: f.itag,
    mime: mime.split(';')[0],
    codecs: (mime.match(/codecs="([^"]+)"/) || [])[1] || '',
    qualityLabel: f.qualityLabel || (isAudio ? (f.audioQuality || '').replace('AUDIO_QUALITY_', '').toLowerCase() : ''),
    width: f.width, height: f.height, fps: f.fps,
    bitrate: f.bitrate, audioQuality: f.audioQuality,
    contentLength: Number(f.contentLength) || 0,
    initRange: f.initRange || null,
    indexRange: f.indexRange || null,
    isVideo, isAudio,
    hasUrl: !!f.url,
    cipher: f.signatureCipher || null,
    url: f.url || null,
  };
}

function buildStreamMaps(videoId, formats, playerResponse) {
  const normalized = formats.map(f => normalizeFormat(f, videoId));
  const withUrl = normalized.filter(f => f.hasUrl);
  const map = {};
  for (const f of withUrl) map[f.itag] = f.url;
  const progressive = withUrl.filter(f => f.isVideo && f.mime === 'video/mp4' && /avc1|vp9/.test(f.codecs) && (f.audioQuality || f.itag === 18 || f.itag === 22))
    .filter(f => f.itag === 18 || f.itag === 22 || f.itag === 59)
    .sort((a, b) => (b.height || 0) - (a.height || 0));
  const videos = withUrl.filter(f => f.isVideo && !progressive.includes(f))
    .sort((a, b) => (b.height || 0) * (b.fps || 0) - (a.height || 0) * (a.fps || 0) || (a.mime === 'video/mp4' ? -1 : 1));
  const audios = withUrl.filter(f => f.isAudio)
    .sort((a, b) => ((a.mime === 'audio/mp4' ? 0 : 1) - (b.mime === 'audio/mp4' ? 0 : 1)) || (b.bitrate || 0) - (a.bitrate || 0));
  return { map, progressive, videos, audios };
}

async function solveCiphers(formats) {
  // formats with signatureCipher -> need base.js transforms
  const need = formats.filter(f => !f.url && f.cipher);
  if (!need.length) return formats;
  await sigSolver.ensure((u) => fetchText(u));
  const sigChallenges = [];
  const nChallenges = [];
  const parsed = need.map(f => {
    const p = new URLSearchParams(f.cipher);
    const base = p.get('url');
    const sp = p.get('sp') || 'sig';
    const s = p.get('s');
    let u = null;
    let n = null;
    try { u = new URL(base); n = u.searchParams.get('n'); } catch (_) { /* malformed */ }
    if (s) sigChallenges.push(s);
    if (n) nChallenges.push(n);
    return { f, u, sp, s, n };
  });
  const sigMap = sigChallenges.length ? sigSolver.solve('sig', sigChallenges) : {};
  const nMap = nChallenges.length ? sigSolver.solve('n', nChallenges) : {};
  for (const ent of parsed) {
    try {
      if (!ent.u) continue;
      if (ent.n && nMap[ent.n]) ent.u.searchParams.set('n', nMap[ent.n]);
      if (ent.s && sigMap[ent.s]) ent.u.searchParams.set(ent.sp, sigMap[ent.s]);
      ent.f.url = ent.u.toString();
    } catch (_) { /* leave url-less */ }
  }
  return formats;
}

/**
 * player() — hardened multi-client × multi-transport rotation.
 *
 * Strategy ("絶対に読み込める"):
 *  1. Remember the last combo (client+transport) that worked and try it first
 *     — after the first success of the session everything is one request away.
 *  2. Walk PLAYER_CHAIN (4 verified-alive client configs); for the first two
 *     clients fan out across direct + up to 4 rotating proxies, later clients
 *     use fewer transports (they rarely differ per-IP).
 *  3. Accept the FIRST response that is playabilityStatus OK and carries at
 *     least one directly playable URL (or an HLS manifest). OK-but-SABR-only
 *     responses are kept as a fallback but never stop the rotation.
 *  4. Nothing worked → Piped public instances as the last resort (their
 *     proxied URLs are not IP-bound, so the browser can even play them
 *     directly).
 */
let goodCombo = rt.goodCombo || null; // {clientKey, params, transportKind, transportUrl} — ディスク永続化で冷起動も一発

async function player(videoId, { hl = 'ja', gl = 'JP' } = {}) {
  const deadline = Date.now() + 45000; // global budget; success usually in <2s
  const used = new Set();              // "client|params|url" dedupe
  let lastErr = null;
  let lastStatus = null;
  let sabrFallback = null;             // OK response without usable urls
  const spanAll = logbus.span('player', 'player 発行', { v: videoId });

  /**
   * 1 波を Go（または Node フォールバック）ヘッジで同時発射。
   * 最初の playability=OK が勝った瞬間に敗者をキャンセルする。
   * 応答形は旧 tryOnce と同一 {res, transport, usable, clientKey, params}。
   */
  const buildPlayerReq = (clientKey, params, transport, gl2 = gl) => {
    const client = CLIENTS[clientKey];
    const payload = { videoId, contentCheckOk: true, racyCheckOk: true };
    if (params) payload.params = params;
    return {
      id: `${clientKey}|${params || '-'}|${transport?.url || 'direct'}|${gl2}`,
      method: 'POST',
      url: `${HOST_WEB}/youtubei/v1/player?key=${client.key}`,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': client.ua,
        'X-YouTube-Client-Name': client.clientNameHeader,
        'X-YouTube-Client-Version': client.ctx.clientVersion,
        Origin: HOST_WEB,
      },
      body: JSON.stringify({ ...payload, context: { client: { hl, gl: gl2, ...client.ctx } } }),
      proxy: transport?.kind === 'proxy' ? transport.url : '',
      timeoutMs: 8000,
      _spec: { clientKey, params, transport, gl: gl2 },
    };
  };

  const applyAttemptEffects = (reqs, result) => {
    for (const a of result?.attempts || []) {
      const spec = reqs.find((r) => r.id === a.id)?._spec;
      if (!spec) continue;
      if (a.playability) lastStatus = { status: a.playability, reason: a.reason || '' };
      if (a.playability === 'LOGIN_REQUIRED' && spec.transport?.kind === 'proxy') {
        proxyManager.markIssuerBad(spec.transport.url);
      }
      if (a.accept && spec.transport?.kind === 'proxy') proxyManager.markGood(spec.transport.url, a.ms);
      else if (!a.accept && a.err !== 'canceled' && spec.transport?.kind === 'proxy'
        && (!a.playability || a.playability === 'OK') && (a.status >= 500 || a.status === 0)) {
        proxyManager.markBad(spec.transport.url);
      }
    }
  };

  const runWave = async (specs, gl2 = gl) => {
    const reqs = [];
    for (const s of specs) {
      const dedupeKey = `${s.clientKey}|${s.params || '-'}|${s.transport?.url || 'direct'}|${gl2}`;
      if (used.has(dedupeKey)) continue;
      used.add(dedupeKey);
      reqs.push(buildPlayerReq(s.clientKey, s.params, s.transport, gl2));
    }
    if (!reqs.length) return null;
    const result = await gocore.hedge(reqs, { kind: 'player' });
    applyAttemptEffects(reqs, result);
    if (result?.status === 451) throwIfHedgeDefinitive(result);
    if (!result?.ok || !result.body) {
      const regional = (result?.attempts || []).some((a) => /国|地域|region|country|お住まい/i.test(String(a.reason || a.err || '')));
      if (regional && gl2 !== 'US') return runWave(specs, 'US');
      lastErr = lastErr || new YTError('wave failed', 502);
      return null;
    }
    const spec = reqs.find((r) => r.id === result.winner)?._spec;
    let res;
    try { res = JSON.parse(result.body); } catch (_) { lastErr = new YTError('bad json', 502); return null; }
    lastStatus = res.playabilityStatus || lastStatus;
    const sd = res.streamingData || {};
    const fmt = sd.formats || [];
    const af = sd.adaptiveFormats || [];
    if (!fmt.length && !af.length && !sd.hlsManifestUrl) { lastErr = new YTError('no formats', 502); return null; }
    try { await solveCiphers([...fmt, ...af]); } catch (_) { /* keep url'd ones */ }
    const usable = [...fmt, ...af];
    if (!usable.filter((f) => f.url).length && !sd.hlsManifestUrl) {
      sabrFallback = sabrFallback || { res, transport: spec?.transport, usable };
      lastErr = new YTError('no usable formats (SABR-only)', 502);
      return null;
    }
    return { res, transport: spec?.transport || null, usable, clientKey: spec?.clientKey, params: spec?.params };
  };

  const specsOf = (steps) => steps.flatMap((s) => (s.transports || []).map((t) => ({ clientKey: s.clientKey, params: s.params, transport: t })));

  // ---- assemble attempts: good combo first, then the full chain
  const directT = { kind: 'direct', dispatcher: undefined };
  // 発行は issuer-grade（YouTube非BAN実測済み）プロキシを最優先で選ぶ。
  // 無ければ従来プール（pickIssuer 内部で自動フォールバック）。
  const proxyUrls = (n) => { const out = []; const seen = new Set(); for (let i = 0; i < n * 2 && out.length < n; i++) { const u = proxyManager.pickIssuer(out); if (u && !seen.has(u)) { seen.add(u); out.push(u); } else break; } return out; };

  const step0 = PLAYER_CHAIN[0];
  const wave1 = [];
  if (goodCombo) {
    wave1.push({ clientKey: goodCombo.clientKey, params: goodCombo.params, transports: [goodCombo.transportKind === 'direct' ? directT : { kind: 'proxy', url: goodCombo.transportUrl, dispatcher: proxyManager.dispatcherFor(goodCombo.transportUrl) }] });
  }
  // wave 1: 定番 + 先頭クライアント (direct × 1 + issuer プロキシ × 2) の同時レース
  const head = { clientKey: step0.client, params: step0.params, transports: [directT, ...transportsForUrls(proxyUrls(goodCombo ? 2 : 3))] };
  wave1.push(head);

  let win = null;
  try {
    win = await runWave(specsOf(wave1));
  } catch (e) {
    if (e && e.status === 451) { lastErr = e; win = null; }
    else lastErr = e;
  }
  logbus.debug('player', 'wave1 (並列レース) 終了', { v: videoId, win: !!win });

  // wave 2+: 残りのクライアント群をステップごとに並列ヘッジで総当たり
  // （wave1 で試した direct×1 + issuer×2〜3 は tryOnce の used チェックで自動重複排除）
  if (!win && Date.now() < deadline && (!lastErr || lastErr.status !== 451)) {
    outer:
    for (let idx = 0; idx < PLAYER_CHAIN.length; idx++) {
      const step = PLAYER_CHAIN[idx];
      if (Date.now() > deadline) break outer;
      const n = idx === 0 ? 4 : idx === 1 ? 3 : 2; // proxy fan-out per client
      const waveTs = [directT, ...transportsForUrls(proxyUrls(n))];
      try {
        const r = await runWave(waveTs.map(t => ({ clientKey: step.client, params: step.params, transport: t })));
        if (r) { win = r; break outer; }
      } catch (e) {
        if (e && e.status === 451) { lastErr = e; break outer; } // definitive unplayable
        lastErr = e;
        if (Date.now() > deadline) break outer;
      }
    }
  }

  // ---- 緊急ローテーション: 全経路が LOGIN_REQUIRED/ERROR/network で潰れた時、
  // 「bot 確認画面」をユーザーに見せないため、プロキシプールを強制総入替して
  // フレッシュな egress でもう一周だけ戦う（IP BAN の時間的クラスタを回避）。
  if (!win && !sabrFallback) {
    try {
      await proxyManager.refresh({ force: true });            // 300候補から新バッチ
      proxyManager.certify({ force: true }).catch(() => {});  // 裏で issuer 認定
      goodCombo = null;
      const rescue = [];
      for (const stepI of [0, 1]) { // 先頭2クライアントで広めに fan-out
        const step = PLAYER_CHAIN[stepI];
        rescue.push({ clientKey: step.client, params: step.params, transports: [directT, ...transportsForUrls(proxyManager.pickMany(5))] });
      }
      const rescueDeadline = Date.now() + 30000;
      for (const step of rescue) {
        if (Date.now() > rescueDeadline) break;
        try {
          // 高速化: 緊急時こそ直列総当たりは致命的に遅い — 各ステップの全経路を
          // 同時レースにして、最初に生き残った egress で即確定する
          const r = await runWave(step.transports.map(t => ({ clientKey: step.clientKey, params: step.params, transport: t })));
          if (r) { win = r; break; }
        } catch (e) {
          if (e && e.status === 451) { lastErr = e; break; }
          lastErr = e;
        }
      }
    } catch (_) { /* rescue is best-effort; Piped が後に控える */ }
  }

  if (!win && sabrFallback) win = { ...sabrFallback, clientKey: 'ANDROID', params: null };

  if (win) {
    const { res, transport, usable, clientKey, params } = win;
    goodCombo = {
      clientKey, params,
      transportKind: transport?.kind || 'direct',
      transportUrl: transport?.kind === 'proxy' ? transport.url : null,
    };
    rt.goodCombo = goodCombo;
    rtSave();
    const urlCount = usable.filter(f => f.url).length;
    spanAll({
      ok: true, client: clientKey, via: transport?.kind === 'proxy' ? `proxy ${transport.url}` : 'direct',
      formats: urlCount, hls: !!res.streamingData?.hlsManifestUrl,
    });
    logbus.info('player', '発行成功', {
      v: videoId, client: clientKey,
      via: transport?.kind === 'proxy' ? 'proxy ' + transport.url : 'direct',
      formats: urlCount,
    });
    const sd = res.streamingData || {};
    const fmt = sd.formats || [];
    const maps = buildStreamMaps(videoId, usable, res);
    const vd = res.videoDetails || {};
    const mf = res.microformat?.playerMicroformatRenderer || {};
    return {
      videoId,
      __transport: transport || null,
      __client: clientKey,
      source: 'innertube',
      title: vd.title || '',
      author: vd.author || '',
      channelId: vd.channelId || '',
      viewCount: vd.viewCount || '',
      lengthSeconds: Number(vd.lengthSeconds) || 0,
      isLive: !!vd.isLiveContent,
      isShort: vd.isShortsEligible || (Number(vd.lengthSeconds) > 0 && Number(vd.lengthSeconds) <= 180 && /\bshorts\b/i.test(vd.title || '')) || false,
      publishDate: mf.publishDate || '',
      uploadDate: mf.uploadDate || '',
      category: mf.category || '',
      keywords: vd.keywords || [],
      progressive: maps.progressive.map(stripUrl),
      videos: maps.videos.map(stripUrl),
      audios: maps.audios.map(stripUrl),
      hls: sd.hlsManifestUrl || null,
      expiresInSeconds: Number(fmt[0]?.expiresInSeconds) || 21540,
      __urlMap: maps.map,
    };
  }

  goodCombo = null;
  if (rt.goodCombo) { rt.goodCombo = null; rtSave(); }

  // ---- LAST RESORT: public Piped instances (proxied, NOT IP-bound)
  try {
    const p = await piped.getStreams(videoId);
    if (p && (p.progressive.length || p.videos.length)) {
      spanAll({ ok: true, client: 'piped:' + p.host, formats: p.progressive.length + p.videos.length });
      logbus.warn('player', '全経路失敗 → Piped で救済', { v: videoId, host: p.host });
      return {
        videoId,
        __transport: null,
        __client: 'piped:' + p.host,
        source: 'piped',
        title: p.title,
        author: p.author,
        channelId: p.channelId,
        viewCount: p.viewCount,
        lengthSeconds: p.lengthSeconds,
        isLive: false, isShort: false,
        publishDate: '', uploadDate: '', category: '', keywords: [],
        progressive: p.progressive.map(stripUrl),
        videos: p.videos.map(stripUrl),
        audios: p.audios.map(stripUrl),
        hls: null,
        expiresInSeconds: 21540,
        __urlMap: Object.fromEntries([...p.progressive, ...p.videos, ...p.audios].map(f => [f.itag, f.url])),
        __piped: true,
      };
    }
  } catch (_) { /* fall through to error */ }

  const err = lastErr || new YTError('player failed', 502);
  err.statusHint = lastStatus?.status;
  err.reason = lastStatus?.reason;
  spanAll({ __error: true, status: err.statusHint || err.code, reason: err.reason || err.message });
  logbus.error('player', '発行失敗', { v: videoId, status: err.statusHint || err.code, reason: err.reason || err.message });
  throw err;
}

function stripUrl(f) {
  const { url, ...rest } = f;
  return rest;
}

/**
 * pickDirect — raw googlevideo (or Piped-proxy) URL the browser may try to
 * play WITHOUT our relay. When the URL is IP-bound (pin != null) a browser
 * hit will 403 and the client falls back to the relay instantly; Piped-proxy
 * URLs are not bound and play directly from anywhere.
 */
function pickDirect(p) {
  if (!p || !p.__urlMap) return null;
  const prog = p.progressive?.[0];
  const itag = prog?.itag ?? (p.__urlMap[18] ? 18 : Number(Object.keys(p.__urlMap)[0]));
  const url = p.__urlMap[itag];
  if (!url) return null;
  const pin = p.__piped ? null : (p.__transport?.kind === 'proxy' ? p.__transport.url : null);
  return {
    itag, url,
    pin,                                   // null -> not IP-bound (or server-direct)
    bound: p.__piped ? false : true,       // innertube URLs are IP-bound to the pin
    source: p.__piped ? 'piped' : 'innertube',
    height: prog?.height || 360,
  };
}

/**
 * getVideoFull — 90秒 read-through キャッシュ付き。
 * 成功（playable）した応答だけをキャッシュする: LOGIN_REQUIRED などの失敗を
 * 抱え込んで返し続ける事故を防ぐ（失敗は必ずライブで再挑戦する）。
 */
const _vfPending = new Map();
async function getVideoFull(videoId, opts = {}) {
  const key = 'vf:' + videoId + (opts.playlistId ? ':' + opts.playlistId : '');
  if (!opts.fresh) {
    const hit = caches.api.get(key);
    if (hit !== undefined) return hit;
    const inflight = _vfPending.get(key);
    if (inflight) return inflight;
  }
  const p = getVideoFullUncached(videoId, opts)
    .then((out) => {
      _vfPending.delete(key);
      if (out?.playable) caches.api.set(key, out, 90 * 1000); // 成功のみ 90 秒
      return out;
    })
    .catch((e) => { _vfPending.delete(key); throw e; });
  if (!opts.fresh) _vfPending.set(key, p);
  return p;
}

async function getVideoFullUncached(videoId, opts = {}) {
  const [pl, nx] = await Promise.allSettled([player(videoId, opts), watchNext(videoId, opts)]);
  if (pl.status === 'rejected' && nx.status === 'rejected') throw pl.reason;
  const p = pl.status === 'fulfilled' ? pl.value : null;
  const n = nx.status === 'fulfilled' ? nx.value : null;

  // ---- map エントリを先に確定（直結判定とリレー修復の両方がこれを使う）
  let mapEntry = null;
  if (p?.__urlMap) {
    mapEntry = streamMapGet(videoId);
    if (!mapEntry || mapEntry.map !== p.__urlMap) {
      mapEntry = buildMapEntry(p);
      streamMapSet(videoId, mapEntry);
    }
  }

  // ---- Vandal 直結判定は応答を待たせない（初速高速化の要）:
  // かつては Range 実測（プロキシ経由で最大 9 秒）とピン修復の完了を await して
  // から watch 応答を返しており、コールド視聴が最悪数秒遅かった。
  // 新方式:
  //   (1) キャッシュ済み判定 (pd) だけを同期的に載せる
  //   (2) 未検証なら実測をすべてバックグラウンドで続行し、次回リクエスト以降に反映
  //   (3) 生 URL (directUrls) は常時クライアントへ渡し、可否はフロントの
  //       「影武者プローブ」がユーザー網で実測する（リレーを妨げないので失敗も無害）
  // これにより「サーバー側 egress は 403 でもユーザー網から直結できる」動画が
  // プロキシされ続けるバグも同時に根治される。
  let pd = pdGet(videoId);
  if (p && !pd) {
    if (p.__piped) {
      pd = { playDirect: true, hdDirect: true }; // Piped プロキシURLは IP 非バインド（実測済み）
      pdSet(videoId, pd, 55 * CACHE_MIN);
    } else if (p.__urlMap) {
      probeDirectness(videoId, p).catch(() => {});  // 裏で実測 → pd キャッシュへ反映
      if (!mapEntry?.pinnedVerified) {
        ensureWorkingPin(videoId).catch(() => {});  // 裏でリレー用ピンを修復
      }
    }
  }
  const direct = pickDirect(p);
  if (direct) {
    direct.verified = !!pd?.playDirect;
    // リレー時は修復済みピンを使う（発行 egress が 403 の動画を救う）
    if (!p.__piped && mapEntry?.pinnedVerified) direct.pin = mapEntry.proxyUrl;
  }
  const out = {
    videoId,
    title: n?.title || p?.title || '',
    description: n?.description || '',
    viewCount: n?.viewCount || (p?.viewCount ? Number(p.viewCount).toLocaleString('ja-JP') + ' 回視聴' : ''),
    dateText: n?.dateText || '',
    likeCount: n?.likeCount || '',
    publishDate: p?.publishDate || '',
    category: p?.category || '',
    isLive: p?.isLive || false,
    lengthSeconds: p?.lengthSeconds || 0,
    channel: n?.channel?.id ? n.channel : { id: p?.channelId || '', name: p?.author || '', subs: '', avatar: '' },
    related: n?.related || [],
    relatedContinuation: n?.relatedContinuation || null,
    commentsToken: n?.commentsToken || null,
    commentsCount: n?.commentsCount || '',
    autoplay: n?.autoplay || null,
    panel: n?.panel || null,
    streams: p ? {
      progressive: p.progressive,
      videos: p.videos,
      audios: p.audios,
      hls: p.hls,
      direct,
      source: p.source || 'innertube',
      // Vandal 直結エンジン: 生 URL は常時一式返す（可否はクライアントの影武者
      // プローブ＋サーバー裏実測の両輪で判定）。MSE は CORS の都合で従来どおりリレー。
      // playDirect/hdDirect は true（実測済）/false（実測失敗）/null（未検証）。
      playDirect: pd ? !!pd.playDirect : null,
      hdDirect: pd ? !!pd.hdDirect : null,
      directUrls: p.__urlMap,
    } : null,
    playable: !!p && (p.progressive.length > 0 || p.videos.length > 0 || !!p.hls),
    playability: p ? null : { status: pl.reason?.statusHint || 'ERROR', reason: pl.reason?.reason || pl.reason?.message || 'この動画は再生できません' },
  };
  if (n && !out.title) out.title = n.videoId;
  return out;
}

/** googlevideo へトンネルできるプロキシ。grow 認定済み(pool.gvOkTs)を優先、
 *  冷機時のみ従来の並列スキャンで補う（10分キャッシュ）。 */
let tunnelCache = { ts: 0, urls: [] };
async function getTunnelProxies(k = 2) {
  const certified = (proxyManager.pool || [])
    .filter(p => p.gvOkTs && Date.now() - p.gvOkTs < 2 * 3600 * 1000)
    .sort((a, b) => a.latency - b.latency)
    .map(p => p.url);
  if (certified.length >= k) return certified.slice(0, k);
  if (Date.now() - tunnelCache.ts < 10 * CACHE_MIN && tunnelCache.urls.length) {
    return [...new Set([...certified, ...tunnelCache.urls])].slice(0, k);
  }
  const cand = (proxyManager.pool || []).slice(0, 8);
  const results = await Promise.all(cand.map(async (p) => {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), 6000);
    try {
      const res = await fetch('https://rr5---sn-nx57ynsl.googlevideo.com/generate_204', { dispatcher: proxyManager.dispatcherFor(p.url), signal: ac.signal });
      try { res.body?.cancel?.(); } catch (_) { /* noop */ }
      return res.status < 500 ? p.url : null;
    } catch (_) { return null; }
    finally { clearTimeout(t); }
  }));
  tunnelCache = { ts: Date.now(), urls: results.filter(Boolean) };
  return [...new Set([...certified, ...tunnelCache.urls])].slice(0, k);
}

function buildMapEntry(p) {
  return {
    map: p.__urlMap,
    source: p.source || 'innertube',
    proxyUrl: p.__piped ? null : (p.__transport?.kind === 'proxy' ? p.__transport.url : null),
    ttlMs: Math.min(5 * 3600 * 1000, Math.max(300 * 1000, ((p.expiresInSeconds || 21600) - 300) * 1000)),
    pinnedVerified: false,
  };
}

/**
 * probeDirectness — 生 URL の直結可否を実測し pd キャッシュへ反映する
 * （getVideoFull の応答は待たない: 呼び出し側は fire-and-forget で使う）。
 * 「発信元 egress では 403 / 別 egress では 206」（ノード別 IP レピュテーション）が
 * 実在するため、自 egress とトンネル可能プロキシを並列で試し、どこか一箇所でも
 * 通れば ip 強制なし = ユーザのブラウザからも直結できると判定する。
 * 結果（pd）は 55 分メモ化され、次回の watch 応答と HD 直結判定に使われる。
 * 実行中の重複実行は 1 本に束ねる（ホバー先読み＋視聴＋warm が同時に走り得るため）。
 */
const _pdPending = new Map();
function probeDirectness(videoId, p) {
  const running = _pdPending.get(videoId);
  if (running) return running;
  const job = (async () => {
    const dualProbe = async (url, tag) => {
      if (!url) return false;
      const tps = await getTunnelProxies(2);
      const jobs = [
        { id: `${tag}-self`, url, proxy: '', timeoutMs: 6000 },
        ...tps.map((px, i) => ({ id: `${tag}-t${i}`, url, proxy: px, timeoutMs: 9000 })),
      ];
      const r = await gocore.probe(jobs);
      return (r?.results || []).some((x) => x.ok);
    };
    const progItag = p.progressive?.[0]?.itag ?? (p.__urlMap[18] ? 18 : Number(Object.keys(p.__urlMap)[0]));
    const vidTrack = p.videos.find(v => (v.height || 0) <= 720 && (v.fps || 30) <= 60) || p.videos[0];
    const audTrack = p.audios[0];
    const [progOk, vOk, aOk] = await Promise.all([
      dualProbe(p.__urlMap[progItag], 'p'),
      dualProbe(p.__urlMap[vidTrack?.itag], 'v'),
      dualProbe(p.__urlMap[audTrack?.itag], 'a'),
    ]);
    const pd = { playDirect: progOk, hdDirect: !!(vOk && aOk) };
    pdSet(videoId, pd, 55 * CACHE_MIN);
    return pd;
  })().finally(() => { _pdPending.delete(videoId); });
  _pdPending.set(videoId, job);
  return job;
}

/**
 * ensureWorkingPin — 2026-08 実測で判明した「URL 発行 egress ≠ 再生可能 egress」
 * 問題（googlevideo ノードの発信元 IP レピュテーション制御）を自動修復する。
 * 発行 egress / 自 egress / トンネル可能プロキシの順に Range 実測し、
 * 実際に 206 が返る egress を map エントリにピン留めする。
 */
async function ensureWorkingPin(videoId) {
  let entry = streamMapGet(videoId);
  if (!entry) {
    const p = await player(videoId);
    entry = buildMapEntry(p);
    streamMapSet(videoId, entry);
  }
  if (entry.pinnedVerified) return entry;
  const url = entry.map?.[18] || Object.values(entry.map || {})[0];
  if (!url) return entry;
  const cands = [];
  const addCand = (px) => { const key = px || null; if (!cands.includes(key)) cands.push(key); };
  addCand(null);               // 自 egress（最速リレー）を優先
  addCand(entry.proxyUrl);     // 発行 egress
  for (const t of await getTunnelProxies(2)) addCand(t);
  const results = await Promise.all(cands.map(px =>
    // 無料プロキシ経由は RTT が大きいため広めのタイムアウトを与える
    probePlayable(url, { proxyUrl: px || '', timeout: px ? 9000 : 6000 })
  ));
  const idx = results.findIndex(Boolean);
  if (idx >= 0) {
    entry.proxyUrl = cands[idx];
    entry.pinnedVerified = true;
    streamMapSet(videoId, entry);
  }
  return entry;
}

async function getStreamUrl(videoId, itag, { verify = false } = {}) {
  let entry = streamMapGet(videoId);
  if (!entry || (itag && !entry.map?.[itag])) {
    const p = await player(videoId);
    entry = buildMapEntry(p);
    streamMapSet(videoId, entry);
    // 高速化（純粋な読み込み速度）: getVideoFullUncached 経路では既に
    // ensureWorkingPin がバックグラウンド発火済みのため、ここでは重複起動
    // だけを避ける。直接 /api/stream へ来た場合（ホットリンク等）は
    // ここで初めてバックグラウンド検証を開始し、初回セグメント要求を
    // 待たせない（verify=false と同じく発行 egress の URL を即返す）。
    const e = streamMapGet(videoId) || entry;
    if (!e.pinnedVerified && !e.pinChecking) {
      e.pinChecking = true;
      ensureWorkingPin(videoId).finally(() => {
        const e2 = streamMapGet(videoId);
        if (e2) { e2.pinChecking = false; }
      });
    }
  }
  // verify=true のときだけ egress 実測＆ピン修復（ホットパス初手は軽量を優先。
  // 実測自体は watch 応答時に並行済みのことが多く、その場合はここでも即時ヒット）
  if (verify) {
    try { entry = await ensureWorkingPin(videoId); } catch (_) { /* keep heuristic pin */ }
  }
  return { url: entry.map?.[itag] || entry.map?.[18] || Object.values(entry.map || {})[0] || null, proxyUrl: entry.proxyUrl, source: entry.source };
}

/** 同一動画への同時 refresh を 1 本に束ねる。かつては rescue が連打されると
 * そのたびに player() （最大数十秒・全クライアント×全トランスポート）が
 * 並行して走り、無料プロキシを押し潰してさらに失敗する悪循環になっていた。 */
const _mapPending = new Map();
function refreshStreamMap(videoId) {
  const inflight = _mapPending.get(videoId);
  if (inflight) return inflight;
  const job = (async () => {
    streamInvalidate(videoId);
    caches.api.deletePrefix('vf:' + videoId); // 古い URL を抱えたフル応答も破棄
    const p = await player(videoId);
    const entry = buildMapEntry(p);
    streamMapSet(videoId, entry);
    return entry;
  })().finally(() => { _mapPending.delete(videoId); });
  _mapPending.set(videoId, job);
  return job;
}

/** Hard invalidation for the client-side "rescue" flow. */
function invalidateVideo(videoId) {
  streamInvalidate(videoId);
  caches.api.delete('w:' + videoId + 'jaJP');
  caches.api.deletePrefix('vf:' + videoId);
  for (const k of _vfPending.keys()) if (k.startsWith('vf:' + videoId)) _vfPending.delete(k);
  caches.streams.delete('hls:' + videoId);
}

/* ----------------------------------------------------------------- comments */

function buildCommentEntityMap(res) {
  const map = new Map();
  const muts = res?.frameworkUpdates?.entityBatchUpdate?.mutations || [];
  for (const m of muts) {
    const p = m?.payload?.commentEntityPayload;
    if (p && m.entityKey) map.set(m.entityKey, p);
  }
  return map;
}

function parseCommentThread(ctr, entities) {
  const vm = ctr?.commentViewModel?.commentViewModel || ctr?.commentViewModel || {};
  const replies = ctr?.replies?.commentRepliesRenderer;
  const repliesToken = replies ? deepFind(replies, 'continuationCommand', 2)?.[0]?.token || null : null;
  // 2025+ entity payload (authoritative)
  const payload = vm.commentKey ? entities?.get(vm.commentKey) : null;
  if (payload) {
    return {
      id: payload.properties?.commentId || '',
      author: payload.author?.displayName || '',
      authorChannelId: payload.author?.channelId || '',
      avatar: payload.author?.avatarThumbnailUrl || '',
      text: payload.properties?.content?.content || '',
      likes: String(payload.toolbar?.likeCountNotliked || payload.toolbar?.likeCountLiked || ''),
      published: payload.properties?.publishedTime || '',
      repliesToken,
      replyCount: String(payload.toolbar?.replyCount || ''),
    };
  }
  // legacy commentRenderer fallback
  const cr = ctr?.comment?.commentRenderer;
  if (cr) {
    return {
      id: cr.commentId || '',
      author: textOf(cr.authorText),
      authorChannelId: cr.authorEndpoint?.browseEndpoint?.browseId || '',
      avatar: bestThumb(cr.authorThumbnail?.thumbnails),
      text: textOf(cr.contentText),
      likes: String(cr.likeCount ?? ''),
      published: textOf(cr.publishedTimeText),
      repliesToken,
      replyCount: textOf(replies?.moreText) || '',
    };
  }
  // vm-only legacy variant
  if (vm.commentId || vm.authorName) {
    return {
      id: vm.commentId || '',
      author: vm.authorName || '',
      authorChannelId: vm.authorChannelId || '',
      avatar: vm.avatarImageUrl || '',
      text: typeof vm.commentText === 'string' ? vm.commentText : textOf(vm.commentText),
      likes: String(vm.likeCount ?? ''),
      published: typeof vm.publishedTimeText === 'string' ? vm.publishedTimeText : textOf(vm.publishedTimeText),
      repliesToken,
      replyCount: textOf(replies?.moreText) || '',
    };
  }
  return null;
}

function parseCommentPage(res) {
  const entities = buildCommentEntityMap(res);
  const eps = res?.onResponseReceivedEndpoints || res?.onResponseReceivedCommands || [];
  const comments = [];
  let continuation = null;
  let count = '';
  for (const ep of eps) {
    for (const k of ['reloadContinuationItemsCommand', 'appendContinuationItemsCommand', 'appendContinuationItemsAction']) {
      const bag = ep[k]?.continuationItems;
      if (!bag) continue;
      for (const it of bag) {
        if (it.commentsHeaderRenderer) count = textOf(it.commentsHeaderRenderer.countText) || textOf(it.commentsHeaderRenderer.commentsCount);
        else if (it.commentThreadRenderer) { const c = parseCommentThread(it.commentThreadRenderer, entities); if (c && (c.text || c.author)) comments.push(c); }
        else if (it.commentRenderer) { const c = parseCommentThread({ comment: { commentRenderer: it.commentRenderer } }, entities); if (c && (c.text || c.author)) comments.push(c); }
        else if (it.continuationItemRenderer) {
          const t = it.continuationItemRenderer.continuationEndpoint?.continuationCommand?.token
            || deepFind(it.continuationItemRenderer, 'token', 1)?.[0];
          if (t) continuation = t;
        }
      }
    }
  }
  return { comments, continuation, count };
}

/**
 * comments — コメント取得。
 *
 * 高速化（コメントが遅い問題の根治）:
 *   旧実装は ①next{videoId} でトークン発見 → ②next{continuation} で本文取得、
 *   と 2 往復が**直列**に走っており、プロキシ経由だと 1 往復数百ms〜数秒で
 *   合計で数秒待たされていた。
 *   新実装:
 *     (1) 呼び出し側が watch 応答に同梱の commentsToken を渡せる（?token=）→
 *         ① を丸ごとスキップし 1 往復で済む（実測で半分以下に短縮）。
 *     (2) サーバーは /api/watch 応答後にコメントを先行取得（prefetch）して
 *         5 分キャッシュへ載せる。ユーザーがコメント欄を開く頃には完成している。
 *     (3) single-flight により prefetch と実際のリクエストが同時でも 1 往復に束ねる。
 */
async function comments(videoId, tokenIn) {
  return caches.api.wrap('c0:' + videoId, 5 * CACHE_MIN, async () => {
    const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストを待たせない
    const spanDone = logbus.span('comments', 'コメント取得', { v: videoId, tokenGiven: !!tokenIn });
    let token = tokenIn && String(tokenIn).length > 20 ? String(tokenIn) : null;
    let entryCount = '';
    if (!token) {
      const res = await callApi('next', { videoId, contentCheckOk: true }, CLIENTS.WEB, { visitorId });
      const panels = res?.engagementPanels || [];
      for (const p of panels) {
        const r = p?.engagementPanelSectionListRenderer;
        if (r?.panelIdentifier === 'engagement-panel-comments-section') {
          token = deepFind(r, 'continuationCommand', 4)?.map(x => x.token).find(Boolean) || null;
        }
      }
      const hd = deepFind(res, 'commentsEntryPointHeaderRenderer', 1)?.[0];
      entryCount = hd ? (textOf(hd.commentCount) || '') : '';
    }
    if (!token) {
      spanDone({ disabled: true });
      return { comments: [], continuation: null, count: entryCount, disabled: true };
    }
    const page1 = await callApi('next', { continuation: token }, CLIENTS.WEB, { visitorId });
    const parsed = parseCommentPage(page1);
    spanDone({ got: parsed.comments.length, count: parsed.count || entryCount });
    return { ...parsed, count: parsed.count || entryCount, disabled: false };
  });
}

async function commentsNext(continuation) {
  return caches.api.wrap('cx:' + continuation, 5 * CACHE_MIN, async () => {
    const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
    const res = await callApi('next', { continuation }, CLIENTS.WEB, { visitorId });
    return parseCommentPage(res);
  });
}

/* ------------------------------------------------------------------ channel */

function parseChannelHeader(res) {
  const phv = deepFind(res, 'pageHeaderViewModel', 1)?.[0];
  if (!phv) {
    const c4 = deepFind(res, 'c4TabbedHeaderRenderer', 1)?.[0];
    if (c4) return {
      name: textOf(c4.title), handle: '', subs: textOf(c4.subscriberCountText),
      videos: textOf(c4.videosCountText),
      avatar: bestThumb(c4.avatar?.thumbnails),
      banner: bestThumb(c4.banner?.thumbnails),
    };
    return null;
  }
  const md = phv.metadata?.contentMetadataViewModel;
  const rows = (md?.metadataRows || []).map(r => (r.metadataParts || []).map(p => textOf(p.text)).filter(Boolean));
  const flat = rows.flat();
  const avatarSources = phv.image?.decoratedAvatarViewModel?.avatar?.avatarViewModel?.image?.sources
    || deepFind(phv.image || {}, 'sources', 1)?.[0];
  const bannerVM = phv.banner?.imageBannerViewModel || deepFind(phv, 'imageBannerViewModel', 1)?.[0];
  return {
    name: textOf(phv.title) || textOf(phv.pageTitle) || '',
    handle: flat.find(t => t.startsWith('@')) || '',
    subs: flat.find(t => /登録者/.test(t)) || '',
    videos: flat.find(t => /本の動画|動画/.test(t) && !/登録者/.test(t)) || '',
    description: textOf(phv.description?.descriptionPreviewViewModel?.description),
    avatar: bestThumb(avatarSources),
    banner: bestThumb(bannerVM?.image?.sources || bannerVM?.image?.images?.sources),
    country: '',
  };
}

function parseChannelTabs(res) {
  const tabs = res?.contents?.twoColumnBrowseResultsRenderer?.tabs || [];
  return tabs.map(t => {
    const tr = t.tabRenderer || t.expandableTabRenderer;
    if (!tr || !tr.title) return null;
    return {
      title: tr.title,
      selected: !!tr.selected,
      params: tr.endpoint?.browseEndpoint?.params || null,
    };
  }).filter(Boolean);
}

async function channel(idOrHandle, { params, continuation, hl = 'ja', gl = 'JP' } = {}) {
  const browseId = await resolveChannelId(idOrHandle);
  const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
  const payload = { browseId };
  if (params) payload.params = params;
  if (continuation) {
    const res = await callApi('browse', { continuation }, CLIENTS.WEB, { visitorId });
    const eps = res?.onResponseReceivedEndpoints || [];
    let items = [];
    let cont = null;
    for (const ep of eps) {
      const bag = ep.appendContinuationItemsAction?.continuationItems || ep.reloadContinuationItemsCommand?.continuationItems || [];
      const r = extractItems(bag);
      items = items.concat(r.items);
      if (r.continuation) cont = r.continuation;
    }
    return { items, continuation: cont };
  }
  const res = await callApi('browse', payload, CLIENTS.WEB, { visitorId });
  const header = parseChannelHeader(res);
  const tabs = parseChannelTabs(res);
  // content of selected tab
  const sel = (res?.contents?.twoColumnBrowseResultsRenderer?.tabs || [])
    .map(t => t.tabRenderer || t.expandableTabRenderer).find(t => t && t.selected);
  const content = sel?.content || null;
  const { items, continuation: cont } = extractItems(content || res);
  // sections (home tab) - keep flat
  return {
    id: browseId,
    ...header,
    tabs,
    items,
    continuation: cont,
  };
}

async function resolveChannelId(idOrHandle) {
  if (!idOrHandle) throw new YTError('channel required', 400);
  let id = String(idOrHandle).trim();
  if (/^UC[\w-]{20,}$/.test(id)) return id;
  let handle = id;
  if (id.startsWith('@')) handle = id.slice(1);
  else if (id.startsWith('channel/')) id = id.slice(8);
  if (/^UC[\w-]{20,}$/.test(id)) return id;
  const cacheKey = 'rh:' + handle.toLowerCase();
  const cached = caches.api.get(cacheKey);
  if (cached) return cached;
  // 1) HTML page via transport — target the *owner* id specifically
  try {
    const html = await fetchText(`${HOST_WEB}/@${encodeURIComponent(handle)}`);
    const jsUrl = html.match(/"PLAYER_JS_URL":"([^"]+)"/)?.[1];
    if (jsUrl) sigSolver.notePlayerUrl(jsUrl.replace(/\\u0026/g, '&'));
    const hEsc = handle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m =
      html.match(new RegExp(`"browseId":"(UC[\\w-]+)","canonicalBaseUrl":"/@${hEsc}"`, 'i'))
      || html.match(/"externalId":"(UC[\w-]+)"/)
      || html.match(/<meta property="og:url" content="https:\/\/www\.youtube\.com\/channel\/(UC[\w-]+)"/)
      || html.match(/<link rel="canonical" href="https:\/\/www\.youtube\.com\/channel\/(UC[\w-]+)"/)
      || html.match(/"ownerUrls":\[[^\]]*"https?:\/\/www\.youtube\.com\/channel\/(UC[\w-]+)"/);
    if (m) { caches.api.set(cacheKey, m[1], 3600e3); return m[1]; }
  } catch (_) { /* fall through to search */ }
  // 2) search fallback — prefer exact @handle match
  const res = await search('@' + handle.replace(/^@/, ''));
  const exact = res.items.find(i => i.kind === 'channel' && i.handle && i.handle.toLowerCase() === '@' + handle.toLowerCase());
  const ch = exact || res.items.find(i => i.kind === 'channel');
  const found = ch?.id?.startsWith('UC') ? ch.id : null;
  if (!found) throw new YTError('チャンネルが見つかりません', 404, 'NOT_FOUND');
  caches.api.set(cacheKey, found, 3600e3);
  return found;
}

/* ----------------------------------------------------------------- playlist */

async function playlist(listId) {
  let raw = String(listId);
  // 「VLRD…」など、ブラウザ由来のプレイリストURL形: 存在しない VLRD への browse は
  // 例外になりページ全体が落ちていた → ミックス実体 (RD…) へ正規化してフォールスルー
  if (/^VL(?=RD|OLAK5uy)/.test(raw)) raw = raw.slice(2);
  const mixLike = /^(RD|OLAK5uy)/.test(raw);
  return caches.api.wrap('pl:' + raw, 10 * CACHE_MIN, async () => {
  const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない

  // ミックス/自動生成リストは browse 系では空 or 例外 → 初めから next 系へ振り分け
  const buildFromNext = async () => {
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const nx = await callApi('next', { playlistId: raw, contentCheckOk: true, racyCheckOk: true }, CLIENTS.WEB, { visitorId, timeout: 12000, transportCount: 2 });
        const panelR = deepFind(nx, 'playlistPanelRenderer', 1)?.[0]
          || (Array.isArray(nx?.contents?.singleColumnWatchNextResults?.playlist?.playlist?.contents) ? nx.contents.singleColumnWatchNextResults.playlist.playlist : null)
          || (Array.isArray(nx?.contents?.twoColumnWatchNextResults?.playlist?.playlist?.contents) ? nx.contents.twoColumnWatchNextResults.playlist.playlist : null);
        const pr = extractItems(panelR?.contents || []);
        const panelItems = pr.items.filter(i => i.kind === 'video' && i.id);
        if (panelItems.length) {
          return {
            id: raw,
            title: textOf(panelR?.titleText) || textOf(panelR?.title) || 'ミックスリスト',
            description: '',
            views: '',
            channelId: '',
            channelName: textOf(panelR?.ownerName) || 'YouTube',
            items: panelItems,
            continuation: pr.continuation,
            panelNext: true,
            isInfinite: !!panelR?.isInfinite,
          };
        }
      } catch (_) { /* rotate & retry */ }
      await new Promise(r => setTimeout(r, 350));
    }
    throw new YTError('ミックスリストを構成できませんでした', 502, 'PLAYLIST_EMPTY');
  };
  if (mixLike) return buildFromNext();

  // 通常プレイリスト: browse。例外時は next 系へフォールスルー（ページを落とさない）
  let res = null;
  try {
    res = await callApi('browse', { browseId: raw.startsWith('VL') ? raw : 'VL' + raw }, CLIENTS.WEB, { visitorId });
  } catch (_) {
    return buildFromNext();
  }
  const title = textOf(deepFind(res, 'playlistSidebarRenderer', 1)?.[0]?.title) || textOf(deepFind(res, 'pageTitle', 1)?.[0]);
  const sub = deepFind(res, 'playlistSidebarRenderer', 1)?.[0] || {};
  const { items, continuation } = extractItems(res?.contents?.twoColumnBrowseResultsRenderer || res);
  const ownerCh = deepFind(sub, 'playlistOwnerEndpoint', 1)?.[0];
  const videos = items.filter(i => i.kind === 'video' || i.kind === 'short');
  if (!videos.length) return buildFromNext(); // 空=生成系 → next パネルで構成
  return {
    id: raw,
    title,
    description: textOf(sub.description),
    views: textOf(sub.viewCountText),
    channelId: ownerCh?.browseEndpoint?.browseId || '',
    channelName: textOf(deepFind(sub, 'playlistOwnerText', 1)?.[0]),
    items: videos,
    continuation,
  };
  });
}

async function playlistNext(continuation) {
  return caches.api.wrap('pln:' + continuation, 10 * CACHE_MIN, async () => {
  const visitorId = getVisitorIdFast(); // 高速化: 未取得なら並行取得し、今回のリクエストは待たせない
  const res = await callApi('browse', { continuation }, CLIENTS.WEB, { visitorId });
  const eps = res?.onResponseReceivedEndpoints || [];
  let items = [], cont = null;
  for (const ep of eps) {
    const bag = ep.appendContinuationItemsAction?.continuationItems || [];
    const r = extractItems(bag);
    items = items.concat(r.items);
    if (r.continuation) cont = r.continuation;
  }
  return { items: items.filter(i => i.kind === 'video'), continuation: cont };
  });
}

/* --------------------------------------------------------------------- home */

const HOME_PRESETS = {
  all: [
    { q: '今人気の動画 2026' },
    { q: '急上昇 音楽' },
    { q: '話題の動画' },
    { q: 'バズった' },
  ],
  music: [{ q: '最新曲 ランキング' }, { q: 'ミュージックビデオ 人気' }, { q: 'カラオケ 人気曲' }],
  gaming: [{ q: 'ゲーム実況 人気' }, { q: 'ゲーム 新作' }],
  news: [{ q: 'ニュース 最新' }, { q: 'ニュースライブ' }],
  anime: [{ q: 'アニメ 話題' }, { q: 'アニメ 2026' }],
  cooking: [{ q: '料理 レシピ 人気' }, { q: '簡単レシピ' }],
  sports: [{ q: 'スポーツ ハイライト' }, { q: 'スポーツ 名場面' }],
  tech: [{ q: 'テクノロジー 解説' }, { q: 'ガジェット レビュー' }],
  asmr: [{ q: 'ASMR 人気' }, { q: 'ASMR 睡眠' }],
};

async function home(chip = 'all') {
  return caches.api.wrap('home:' + chip, 15 * CACHE_MIN, async () => {
    const spanDone = logbus.span('meta', 'ホーム構築', { chip });
    const presets = HOME_PRESETS[chip] || HOME_PRESETS.all;
    const pages = await Promise.allSettled(presets.map(p => search(p.q)));
    const items = [];
    const seen = new Set();
    for (const pg of pages) {
      if (pg.status !== 'fulfilled') continue;
      for (const it of pg.value.items) {
        const k = (it.kind === 'video' || it.kind === 'short') ? it.id : null;
        if (!k || seen.has(k)) continue;
        seen.add(k);
        items.push(it);
      }
    }
    // interleave-ish shuffle for variety but stable within TTL
    items.sort((a, b) => ((a.id || '').charCodeAt(2) || 0) - ((b.id || '').charCodeAt(2) || 0));
    const out = { chip, items, continuation: null };
    // ディスクスナップショット保存（次回コールドブートの即時ホーム描画用）
    if (items.length >= 8) {
      rt.home[chip] = { savedAt: Date.now(), data: out };
      rtSave();
    }
    spanDone({ items: items.length, okPages: pages.filter(p => p.status === 'fulfilled').length });
    return out;
  });
}

/* ------------------------------------------------ recommendation engine */

/** Curated shorts-only queries for the dedicated Shorts surfaces. */
const SHORTS_QUERIES = ['ショート おもしろ', 'shorts バズ', 'コント ショート', 'shorts かわいい 動物'];

/**
 * shortsFeed — dedicated vertical-video collection, separate from horizontal
 * content (ユーザーの「ショートは専用欄にまとめて」要件). Cached for 15min.
 */
async function shortsFeed() {
  return caches.api.wrap('shortz:feed', 15 * CACHE_MIN, async () => {
    const pages = await Promise.allSettled(SHORTS_QUERIES.map(q => search(q)));
    const shorts = [];
    const seen = new Set();
    for (const pg of pages) {
      if (pg.status !== 'fulfilled') continue;
      for (const it of pg.value.items) {
        if (it.kind !== 'short' || !it.id || seen.has(it.id)) continue;
        seen.add(it.id);
        shorts.push(it);
      }
    }
    return { items: shorts };
  });
}

/**
 * personal — builds a recommendation home from the user's local profile.
 * profile = { queries: string[], relatedOf: videoId[] }
 * Signals come from watch history / likes / recent searches that the client
 * keeps in localStorage; nothing personal is persisted server-side.
 */
async function personal(profile = {}) {
  const queries = (Array.isArray(profile.queries) ? profile.queries : [])
    .map(s => String(s).slice(0, 80)).filter(Boolean).slice(0, 4);
  const relatedOf = (Array.isArray(profile.relatedOf) ? profile.relatedOf : [])
    .filter(s => /^[\w-]{11}$/.test(s)).slice(0, 3);

  const budget = Date.now() + 7000; // never let the home hang
  const jobs = [];
  for (const q of queries) jobs.push(search(q).then(r => ({ from: 'q:' + q, items: r.items })).catch(() => null));
  for (const id of relatedOf) jobs.push(
    (async () => {
      let w = caches.api.get('w:' + id + 'jaJP');
      if (!w) {
        try {
          w = await Promise.race([
            watchNext(id),
            (async () => { while (Date.now() < budget) { await sleep(120); const hit = caches.api.get('w:' + id + 'jaJP'); if (hit) return hit; } return null; })(),
          ]);
        } catch (_) { w = null; }
      }
      return w?.related?.length ? { from: 'r:' + id, items: w.related } : null;
    })()
  );
  const pages = (await Promise.all(jobs)).filter(Boolean);

  // round-robin interleave so every preference signal is represented
  const videos = [];
  const shorts = [];
  const seen = new Set();
  const queues = pages.map(p => [...p.items]);
  for (let guard = 0; guard < 400; guard++) {
    let added = false;
    for (const q of queues) {
      const it = q.shift();
      if (!it) continue;
      if ((it.kind === 'video' || it.kind === 'short') && it.id) {
        if (seen.has(it.id)) continue;
        seen.add(it.id);
        (it.kind === 'short' ? shorts : videos).push(it);
        added = true;
      } else if (it.kind === 'playlist') {
        const k = 'pl:' + it.id;
        if (seen.has(k)) continue;
        seen.add(k);
        videos.push(it);
        added = true;
      }
    }
    if (!added) break;
  }

  // top the shorts shelf up so it always feels alive
  if (shorts.length < 8) {
    try {
      const feed = await shortsFeed();
      for (const it of feed.items) {
        if (shorts.length >= 14) break;
        if (seen.has(it.id)) continue;
        seen.add(it.id);
        shorts.push(it);
      }
    } catch (_) { /* optional */ }
  }
  return { items: videos.slice(0, 48), shorts: shorts.slice(0, 16), personalized: true };
}

/* ------------------------------------------------------------------ suggest */

async function suggest(q) {
  return caches.api.wrap('sg:' + q, 10 * CACHE_MIN, async () => {
    const url = `https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&hl=ja&gl=jp&q=${encodeURIComponent(q)}`;
    let lastErr = null;
    for (const t of transports('direct')) {
      try {
        const res = await rawFetch(url, { dispatcher: t.dispatcher, timeout: 5000, headers: { 'User-Agent': CLIENTS.WEB.ua } });
        if (!res.ok) { lastErr = new Error('HTTP ' + res.status); if (t.kind === 'proxy') proxyManager.markBad(t.url); continue; }
        const json = await res.json();
        if (t.kind === 'proxy') proxyManager.markGood(t.url);
        return { q, suggestions: Array.isArray(json?.[1]) ? json[1] : [] };
      } catch (e) {
        lastErr = e;
        if (t.kind === 'proxy') proxyManager.markBad(t.url);
      }
    }
    throw lastErr || new YTError('suggest failed', 502);
  });
}

module.exports = {
  search, searchNext, watchNext, getVideoFull, player, getStreamUrl, refreshStreamMap, ensureWorkingPin,
  comments, commentsNext, channel, playlist, playlistNext, panelNext, home, personal, shortsFeed, suggest,
  resolveChannelId, getVisitorId, getHls, fetchText, invalidateVideo, YTError, caches,
  // test hooks (not used by the app runtime)
  __test: { CLIENTS, PLAYER_CHAIN, transportsForUrls, resetCombos: () => { goodCombo = null; rt.goodCombo = null; }, probePlayable },
};

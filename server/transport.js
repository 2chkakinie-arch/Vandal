'use strict';
/**
 * InnerTube transport layer — llytpr-wl.v01nh.
 * youtubei POST with rotating free-proxy transport + visitor data + cipher probe.
 */
const { proxyManager } = require('./proxies');
const { logbus } = require('./logbus');
const { engineConfig } = require('./config');
const { YTError } = require('./errors');
const { caches, CACHE_MIN } = require('./caches');
const { gocore } = require('./gocore');
const { throwIfHedgeDefinitive } = require('./hedge');

const API_KEY = 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8';
const HOST_WEB = 'https://www.youtube.com';
/**
 * probePlayable — llytpr++ 直結エンジンの心臓。
 * 2026-08 実測: ANDROID/IOS 系プリ署名 googlevideo URL は `ip=` パラメータを含むが
 * サーバー側で強制されず、任意 egress・任意 UA で 206 が返る（= ブラウザが直接再生可能）。
 * これは Invidious の「署名を自前で解いた生 URL をクライアントへ渡す」方式や
 * zernio(getlate) のダウンローダが生 URL を返すのと同じ仕組みであり、
 * 本サーバーは動画ごとに Range 実測してから「直結可能」判定をフロントへ返す。
 */
async function probePlayable(url, { timeout = 4500, dispatcher, proxyUrl } = {}) {
  if (!url) return false;
  // Prefer the (Go or Node) hedge probe. dispatcher-only callers still work
  // via the historical fetch path so existing unit hooks stay valid.
  if (proxyUrl !== undefined || dispatcher === undefined) {
    try {
      const r = await gocore.probe([{ id: 'p', url, proxy: proxyUrl || '', timeoutMs: timeout }]);
      if (r && Array.isArray(r.results)) return !!r.results[0]?.ok;
    } catch (_) { /* fall through */ }
  }
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeout);
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        Range: 'bytes=0-63',
        Accept: '*/*',
        'User-Agent': 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip',
      },
      dispatcher, // ★ egress を必ず指定どおりに（省略=自 egress）
      signal: ac.signal,
      redirect: 'follow',
    });
    const ok = res.status === 200 || res.status === 206;
    try { res.body?.cancel?.(); } catch (_) { /* noop */ }
    return ok;
  } catch (_) {
    return false;
  } finally {
    clearTimeout(t);
  }
}
const CLIENTS = {
  WEB: {
    key: API_KEY,
    ctx: { clientName: 'WEB', clientVersion: '2.20260708.00.00' },
    clientNameHeader: '1',
    ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
  },
  ANDROID: {
    key: API_KEY,
    ctx: {
      clientName: 'ANDROID', clientVersion: '21.26.364', androidSdkVersion: 30,
      osName: 'Android', osVersion: '11',
    },
    clientNameHeader: '3',
    ua: 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip',
  },
  ANDROID_OLD: {
    key: API_KEY,
    ctx: {
      clientName: 'ANDROID', clientVersion: '20.10.38', androidSdkVersion: 30,
      osName: 'Android', osVersion: '11',
    },
    clientNameHeader: '3',
    ua: 'com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip',
  },
  ANDROID_VR: {
    key: API_KEY,
    ctx: {
      clientName: 'ANDROID_VR', clientVersion: '1.65.10', androidSdkVersion: 33,
      osName: 'Android', osVersion: '14', deviceMake: 'Oculus', deviceModel: 'Quest 3',
    },
    clientNameHeader: '28',
    ua: 'com.google.android.apps.youtube.vr.oculus/1.65.10 (Linux; U; Android 14; Quest 3) gzip',
  },
  IOS: {
    key: 'AIzaSyB-63vPrdThhKuerbB2N_l7Kwwcxj6yUAc',
    ctx: {
      clientName: 'IOS', clientVersion: '21.26.3', deviceMake: 'Apple',
      deviceModel: 'iPhone17,5', osName: 'iOS', osVersion: '26.0.0.23A344',
    },
    clientNameHeader: '5',
    ua: 'com.google.ios.youtube/21.26.3 (iPhone17,5; U; CPU iOS 26_0 like Mac OS X; ja_JP)',
  },
};
/**
 * Verified-alive player client chain (probe-tested 2026-08). `params:'2AMB'`
 * makes YouTube return direct pre-signed URLs instead of SABR-only streams.
 * Order matters: richest & most reliable first.
 */
const PLAYER_CHAIN = [
  { client: 'ANDROID', params: '2AMB' },
  { client: 'ANDROID_OLD', params: '2AMB' },
  { client: 'ANDROID_VR', params: '2AMB' },
  { client: 'IOS', params: '2AMB' },
  { client: 'ANDROID', params: null },   // final: often SABR-only, solver may repair
];
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function rawFetch(url, { method = 'GET', headers = {}, body, dispatcher, timeout = 9000 } = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeout);
  try {
    const res = await fetch(url, { method, headers, body, dispatcher, signal: ac.signal, redirect: 'follow' });
    return res;
  } finally {
    clearTimeout(t);
  }
}
function transports(preferProxy, count = 2) {
  const list = [];
  const seen = new Set();
  const addProxy = () => {
    const u = proxyManager.pick([...seen]);
    if (u) { seen.add(u); list.push({ kind: 'proxy', url: u, dispatcher: proxyManager.dispatcherFor(u) }); }
  };
  const direct = { kind: 'direct', dispatcher: undefined };
  // 設定ページの proxyMode が最優先（direct = プロキシ完全不使用）
  const mode = engineConfig.get('proxyMode');
  if (mode === 'direct') return [direct];
  if (preferProxy === 'direct') return [direct];
  if (mode === 'proxy' || preferProxy === 'proxy') {
    for (let i = 0; i < count + 1; i++) addProxy();
    list.push(direct); // 絶命時の保険だけは残す
    return list;
  }
  // auto: proxies first (block-safe), direct as the safety net
  for (let i = 0; i < count; i++) addProxy();
  list.push(direct);
  return list;
}
function transportsForUrls(urls) {
  return (urls || []).map(u => u ? ({ kind: 'proxy', url: u, dispatcher: proxyManager.dispatcherFor(u) }) : ({ kind: 'direct', dispatcher: undefined }));
}
/**
 * POST to youtubei with automatic proxy rotation + direct fallback.
 * HTTP 4xx from YouTube is a *content* error (bad argument / login required),
 * not a transport error, so it is returned as-is (callers decide).
 */
async function callApi(endpoint, payload, client = CLIENTS.WEB, { hl = 'ja', gl = 'JP', visitorId, preferProxy, timeout = 9000, ret, transport, transportCount } = {}) {
  const body = JSON.stringify({
    ...payload,
    context: {
      ...(payload.context || {}),
      client: { hl, gl, ...client.ctx, ...((payload.context || {}).client || {}) },
    },
  });
  const url = `${HOST_WEB}/youtubei/v1/${endpoint}?key=${client.key}`;
  const headers = {
    'Content-Type': 'application/json',
    'User-Agent': client.ua,
    'X-YouTube-Client-Name': client.clientNameHeader,
    'X-YouTube-Client-Version': client.ctx.clientVersion,
    'Origin': HOST_WEB,
    ...(visitorId ? { 'X-Goog-Visitor-Id': visitorId } : {}),
  };
  const chain = transport ? [transport] : transports(preferProxy, transportCount);
  const ch = ENDPOINT_LOG_CH[endpoint] || 'meta';
  const reqs = chain.map((t, i) => ({
    id: String(i),
    method: 'POST',
    url,
    headers,
    body,
    proxy: t.kind === 'proxy' ? t.url : '',
    timeoutMs: timeout,
  }));
  const start = Date.now();
  const result = await gocore.hedge(reqs, { kind: 'json' });
  try { throwIfHedgeDefinitive(result); } catch (e) {
    logbus.warn(ch, `${endpoint} HTTP ${result?.status}`, { ms: Date.now() - start });
    throw e;
  }
  if (result?.ok && result.body) {
    const idx = Number(result.winner);
    const t = chain[Number.isFinite(idx) ? idx : 0] || chain[0];
    const via = t.kind === 'proxy' ? `proxy ${t.url}` : 'direct';
    if (t.kind === 'proxy') proxyManager.markGood(t.url, result.ms || (Date.now() - start));
    if (ret) ret.transport = t;
    const json = tryParse(result.body);
    if (!json) throw new YTError('bad json', 502);
    logbus.debug(ch, `${endpoint} ✓`, {
      via, ms: result.ms || (Date.now() - start),
      client: client.ctx.clientName,
      playability: json?.playabilityStatus?.status || undefined,
      core: gocore.available() ? 'go' : 'node',
    });
    return json;
  }
  // No winner: mark failed proxies (skip canceled / aborted — those are losers of a won race)
  let lastErr = null;
  for (const a of result?.attempts || []) {
    const t = chain[Number(a.id)];
    if (!t || t.kind !== 'proxy') continue;
    if (a.err === 'canceled' || a.accept) continue;
    proxyManager.markBad(t.url);
    lastErr = lastErr || new YTError(a.err || 'upstream unreachable', 502);
    logbus.warn(ch, `${endpoint} 試行失敗 → 経路ローテーション`, { via: 'proxy ' + t.url, err: a.err });
  }
  throw lastErr || new YTError('upstream unreachable', 502);
}
/** endpoint → ログチャンネル割当（設定ページのフィルタ用） */
const ENDPOINT_LOG_CH = {
  player: 'player',
  next: 'meta',
  search: 'meta',
  browse: 'meta',
};
function tryParse(s) { try { return JSON.parse(s); } catch (_) { return null; } }
/** GET a text document (base.js, watch page) through the transport chain. */
async function fetchText(url, { preferProxy, timeout = 12000, ret } = {}) {
  let lastErr = null;
  for (const t of transports(preferProxy)) {
    try {
      const res = await rawFetch(url, {
        dispatcher: t.dispatcher,
        timeout,
        headers: { 'User-Agent': CLIENTS.WEB.ua, 'Accept-Language': 'ja,en;q=0.9' },
      });
      if (!res.ok) { if (t.kind === 'proxy') proxyManager.markBad(t.url); lastErr = new YTError('HTTP ' + res.status, 502); continue; }
      if (t.kind === 'proxy') proxyManager.markGood(t.url);
      if (ret) ret.transport = t;
      return await res.text();
    } catch (e) {
      if (t.kind === 'proxy') proxyManager.markBad(t.url);
      lastErr = e;
    }
  }
  throw lastErr || new YTError('fetch failed', 502);
}
/* ------------------------------------------------------------- visitor data */

let _vdJob = null; // 同時多発の visitor 取得を 1 本に束ねる
async function getVisitorId() {
  let vd = caches.visitor.get('vd');
  if (vd) return vd;
  if (_vdJob) return _vdJob;
  _vdJob = (async () => {
    try {
      const res = await callApi('search', { query: 'youtube' }, CLIENTS.WEB);
      const v = decodeURIComponent(res?.responseContext?.visitorData || '');
      if (v) caches.visitor.set('vd', v, 20 * CACHE_MIN);
    } catch (_) { /* stays undefined; most endpoints work without */ }
    finally { _vdJob = null; }
  })();
  return _vdJob;
}

/**
 * 高速化: visitorData が無い場合も現在のリクエストを待たせない。
 * かつては await getVisitorId()（最大1 RTT = プロキシ経由で数百ms〜数秒）が
 * 検索/視聴/コメント等の初回リクエスト前に直列に挟まっていた。
 * 大半のエンドポイントは visitorId 無しで動作するため、初回は即座に発行し、
 * visitor 取得は並行で回して次回リクエストから使えばよい。
 */
function getVisitorIdFast() {
  const vd = caches.visitor.get('vd');
  if (!vd) getVisitorId().catch(() => {});
  return vd || undefined;
}

module.exports = {
  API_KEY, HOST_WEB, CLIENTS, PLAYER_CHAIN, probePlayable, sleep, rawFetch,
  transports, transportsForUrls, callApi, ENDPOINT_LOG_CH, tryParse, fetchText,
  getVisitorId, getVisitorIdFast,
};

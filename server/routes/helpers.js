'use strict';
/** Shared HTTP helpers + relay state for the llytpr-wl.v01nh route modules. */
const { request: undiciRequest } = require('undici');
const { proxyManager } = require('../proxies');
const { piped } = require('../piped');
const { hotChunks } = require('../media');
const { logbus } = require('../logbus');
const { engineConfig } = require('../config');
const it = require('../innertube');
const { gocore } = require('../gocore');

/** fetch the pinned itag-18 URL and pre-buffer its first bytes in RAM. */
async function warmDefault(v) {
  try {
    // getStreamUrl → streamMapSet → Go にピン（Go 側で 768KB 保温）。
    const { url, proxyUrl } = await it.getStreamUrl(v, 18);
    if (gocore.available()) return; // Node の二重 fetch を避ける（帯域も初速も損しない）
    if (url) hotChunks.warm(v, 18, url, proxyUrl);
  } catch (_) { /* warm is best-effort */ }
}
/**
 * 高速化: /api/watch 応答後にコメントを先行取得（prefetch）。
 * ユーザーがコメント欄をスクロールで開く頃には 5 分キャッシュへ載っていて、
 * /api/comments は一瞬で返る。設定 (commentsPrefetch) で ON/OFF 可能。
 */
function warmComments(v, token) {
  if (!engineConfig.get('commentsPrefetch')) return;
  it.comments(v, token || undefined).catch(() => {});
}
const wrap = (fn) => (req, res) => {
  Promise.resolve(fn(req, res)).catch((e) => {
    if (res.headersSent) { try { res.end(); } catch (_) {} return; }
    const status = e.status && e.status < 600 ? e.status : 502;
    res.status(status).json({
      error: e.message || 'upstream error',
      code: e.code || 'UPSTREAM',
      status: e.statusHint,
    });
  });
};
/* ------------------------------------------------------------- media proxy */

/**
 * 高速化: 同一 (video, itag, Range) の並行リクエストを 1 本の上流フェッチに束ねる。
 * ブラウザ（特に MSE の DashLite と二重 <video>/<audio> 構成）は同じセグメントを
 * ほぼ同時に 2〜3 回要求してくることがあり、束ねると無料プロキシ帯域が
 * リクエスト数分ではなく 1 本で済み、全クライアントの初バイトが速くなる。
 * （巨大な full-file GET をメモリに溜めないよう、明示 Range 8MB 以下のみ対象）
 */
const _streamJobs = new Map(); // "v|itag|range" -> Promise<buffered|null>
async function fetchStreamBytes(url, headers, dispatcher) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), 25000);
  try {
    const upstream = await undiciRequest(url, {
      method: 'GET', headers, signal: ac.signal, dispatcher,
      maxRedirections: 2, headersTimeout: 20000,
    });
    if (upstream.statusCode >= 400) { upstream.body.dump().catch(() => {}); return null; }
    const pass = ['content-type', 'content-length', 'content-range', 'accept-ranges', 'cache-control'];
    const out = {};
    for (const k of pass) { const val = upstream.headers[k]; if (val) out[k] = val; }
    const chunks = [];
    for await (const c of upstream.body) chunks.push(c);
    return { status: upstream.statusCode === 206 ? 206 : 200, headers: out, buf: Buffer.concat(chunks) };
  } catch (_) {
    return null;
  } finally {
    clearTimeout(t);
  }
}
const PIPED_HOSTS = new Set(piped.instances);
function isGoogleVideo(u) {
  try {
    const h = new URL(u).hostname;
    if (/(^|\.)googlevideo\.com$/.test(h) || /(^|\.)youtube\.com$/.test(h) || /(^|\.)ytimg\.com$/.test(h) || /(^|\.)googleapis\.com$/.test(h) || h === 'suggestqueries.google.com') return true;
    // piped proxy hosts (only the hardcoded provider list — no open relay)
    const bare = h.replace(/^pipedproxy\./, '').replace(/^proxy\./, '');
    return PIPED_HOSTS.has(h) || PIPED_HOSTS.has(bare) || /(^|\.)piped\./.test(h);
  } catch (_) { return false; }
}
/* ---------------- hls (live) proxy with playlist rewriting ---------------- */
const hlsPins = new Map(); // videoId -> {proxyUrl}
async function pipeUpstream(url, headers, req, res, { dispatcher } = {}) {
  const ac = new AbortController();
  req.on('close', () => ac.abort());
  const upstream = await undiciRequest(url, {
    method: 'GET',
    headers,
    signal: ac.signal,
    dispatcher,
    maxRedirections: 2,
    headersTimeout: 20000,
  });
  if ([403, 410].includes(upstream.statusCode)) {
    upstream.body.dump().catch(() => {});
    throw new Error('expired ' + upstream.statusCode);
  }
  if (upstream.statusCode >= 400) {
    upstream.body.dump().catch(() => {});
    throw new Error('upstream ' + upstream.statusCode);
  }
  const pass = ['content-type', 'content-length', 'content-range', 'accept-ranges', 'cache-control'];
  const out = {};
  for (const k of pass) {
    const val = upstream.headers[k];
    if (val) out[k] = val;
  }
  out['Access-Control-Allow-Origin'] = '*';
  out['Cache-Control'] = out['Cache-Control'] || 'private, max-age=3600';
  res.writeHead(upstream.statusCode === 206 ? 206 : 200, out);
  try {
    for await (const chunk of upstream.body) {
      if (!res.write(chunk)) await new Promise(r => res.once('drain', r));
    }
  } catch (e) {
    // client aborted / mid-stream failure: headers already sent, just close
    try { res.destroy(); } catch (_) {}
    return;
  }
  res.end();
}

module.exports = { warmDefault, warmComments, wrap, fetchStreamBytes, isGoogleVideo, pipeUpstream, _streamJobs, hlsPins, PIPED_HOSTS };

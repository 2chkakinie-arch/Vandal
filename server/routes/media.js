'use strict';
/** Media relay routes (/api/stream, /api/hls, /api/hotstat, /api/thumb). */
const express = require('express');
const { request: undiciRequest } = require('undici');
const { wrap, fetchStreamBytes, isGoogleVideo, pipeUpstream, _streamJobs, hlsPins } = require('./helpers');
const { proxyManager } = require('../proxies');
const { hotChunks } = require('../media');
const { logbus } = require('../logbus');
const it = require('../innertube');

const router = express.Router();

router.get('/api/stream', wrap(async (req, res) => {
  const v = String(req.query.v || '');
  const itag = String(req.query.itag || '18');
  const rawRaw = req.query.raw ? String(req.query.raw) : null;
  if (!rawRaw && !/^[\w-]{11}$/.test(v)) { res.status(400).json({ error: 'bad id' }); return; }

  // HOT PATH: pre-buffered first bytes -> answer straight from RAM
  if (!rawRaw && hotChunks.serveIfHot(v, itag, req, res)) {
    logbus.trace('stream', 'RAM 即応答 (hot-cache HIT)', { v, itag, range: req.headers.range || null });
    return;
  }

  // 重複排除対象: 明示 Range かつ 8MB 以下のセグメント系リクエストのみ
  const rangeHdr = req.headers.range ? String(req.headers.range) : null;
  const rm = rangeHdr ? /^bytes=(\d+)-(\d+)$/.exec(rangeHdr) : null;
  const dedupe = !rawRaw && !!rm && (Number(rm[2]) - Number(rm[1])) <= 8 * 1024 * 1024;

  let attempt = 0;
  let lastErr = null;
  while (attempt < 2) {
    attempt++;
    try {
      let url, proxyUrl;
      if (rawRaw) {
        url = rawRaw;
        const p = req.query.p ? String(req.query.p) : null;
        // only accept proxies that are currently in our verified pool
        proxyUrl = p && proxyManager.pool.some(x => x.url === p) ? p : (hlsPins.get(v)?.proxyUrl || null);
      } else {
        // attempt 2 以降は egress 実測＆ピン修復つき（発行 egress が 403 の動画を救う）
        ({ url, proxyUrl } = await it.getStreamUrl(v, itag, { verify: attempt > 1 }));
      }
      if (!url || !isGoogleVideo(url)) throw new Error('no stream');
      const headers = {
        'User-Agent': 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip',
        'Accept': '*/*',
      };
      if (req.headers.range) headers.Range = String(req.headers.range);
      // stream URLs are IP-bound to whichever egress fetched them: reuse it.
      // 高速化: 直 egress でもリレー専用の keep-alive Agent を使い、
      // セグメント毎の TCP/TLS ハンドシェイクを排除する。
      const dispatcher = proxyManager.dispatcherForRelay(proxyUrl);
      if (dedupe) {
        const key = v + '|' + itag + '|' + headers.Range;
        let job = _streamJobs.get(key);
        if (!job) {
          job = fetchStreamBytes(url, headers, dispatcher).finally(() => _streamJobs.delete(key));
          _streamJobs.set(key, job);
          logbus.debug('stream', '上流フェッチ（並行リクエスト束ね）', { v, itag, range: headers.Range, via: proxyUrl ? 'proxy' : 'direct' });
        }
        const hit = await job;
        if (!hit) throw new Error('upstream');
        if (res.headersSent || req.destroyed) return;
        res.writeHead(hit.status, {
          ...hit.headers,
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': hit.headers['cache-control'] || 'private, max-age=3600',
        });
        res.end(hit.buf);
        return;
      }
      logbus.debug('stream', '上流リレー', { v, itag, range: headers.Range || '(full)', via: proxyUrl ? 'proxy' : 'direct', raw: !!rawRaw });
      await pipeUpstream(url, headers, req, res, { dispatcher });
      return;
    } catch (e) {
      lastErr = e;
      if (res.headersSent) return;
      // URL probably expired -> rebuild the map once
      if (!rawRaw) { try { await it.refreshStreamMap(v); } catch (_) {} }
      else break;
    }
  }
  if (!res.headersSent) res.status(502).json({ error: lastErr?.message || 'stream failed' });
}));

router.get('/api/hls', wrap(async (req, res) => {
  const v = String(req.query.v || '');
  let url = req.query.raw ? String(req.query.raw) : null;
  let pin = hlsPins.get(v)?.proxyUrl || null;
  if (!url) {
    if (!/^[\w-]{11}$/.test(v)) { res.status(400).json({ error: 'bad id' }); return; }
    const h = await it.getHls(v);
    if (!h.url) { res.status(404).json({ error: 'no hls' }); return; }
    url = h.url;
    pin = h.proxyUrl;
    hlsPins.set(v, { proxyUrl: pin });
    if (hlsPins.size > 500) hlsPins.delete(hlsPins.keys().next().value);
  }
  if (!isGoogleVideo(url)) { res.status(400).json({ error: 'bad url' }); return; }
  const dispatcher = pin ? proxyManager.dispatcherFor(pin) : undefined;
  const ac = new AbortController();
  req.on('close', () => ac.abort());
  const upstream = await undiciRequest(url, {
    method: 'GET',
    headers: { 'User-Agent': 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip' },
    dispatcher, signal: ac.signal, headersTimeout: 12000,
  });
  if (upstream.statusCode >= 400) {
    upstream.body.dump().catch(() => {});
    res.status(502).json({ error: 'hls upstream ' + upstream.statusCode });
    return;
  }
  const text = await upstream.body.text();
  const base = new URL(url);
  const pinQ = pin ? '&p=' + encodeURIComponent(pin) : '';
  const rewritten = text.split('\n').map(line => {
    const t = line.trim();
    if (t && !t.startsWith('#')) {
      const abs = new URL(t, base).toString();
      return /\.m3u8(\?|$)/.test(abs)
        ? `/api/hls?raw=${encodeURIComponent(abs)}${v ? '&v=' + v : ''}${pinQ}`
        : `/api/stream?raw=${encodeURIComponent(abs)}${v ? '&v=' + v : ''}${pinQ}`;
    }
    return line.replace(/URI="([^"]+)"/g, (_m, u) => {
      let abs;
      try { abs = new URL(u, base).toString(); } catch (_) { return _m; }
      const target = /\.m3u8(\?|$)/.test(abs)
        ? `/api/hls?raw=${encodeURIComponent(abs)}${v ? '&v=' + v : ''}${pinQ}`
        : `/api/stream?raw=${encodeURIComponent(abs)}${v ? '&v=' + v : ''}${pinQ}`;
      return `URI="${target}"`;
    });
  }).join('\n');
  res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(rewritten);
}));
router.get('/api/hotstat', (req, res) => res.json(hotChunks.status()));
// thumbnail proxy (avoids mixed-content issues & i.ytimg blocks on some networks)
router.get('/api/thumb', wrap(async (req, res) => {
  const u = String(req.query.u || '');
  if (!isGoogleVideo(u)) { res.status(400).json({ error: 'bad url' }); return; }
  await pipeUpstream(u, { 'User-Agent': 'Mozilla/5.0' }, req, res);
}));

module.exports = { router };

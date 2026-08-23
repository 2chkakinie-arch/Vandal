'use strict';
/**
 * HotChunks — "instant first frame" media warmer for llytpr-wl.v01nh.
 *
 * The 360p progressive start path is optimized to the limit:
 *  - As soon as the server knows a video's stream map (watch API hit or card
 *    hover), the FIRST BYTES of the itag-18 file are fetched in the
 *    background through the same transport that minted the URL and kept in
 *    RAM for a few minutes.
 *  - /api/stream then answers any Range request fully covered by those bytes
 *    from memory (sub-millisecond), turning time-to-first-frame into a pure
 *    local-memory read instead of a proxied round trip.
 *  - Pending warm jobs are deduplicated, so hover storms cost nothing.
 *
 * No external APIs are involved — this is pure transport/cache engineering.
 *
 * Made by Kakinie with llytpr-wl.v01nh TEAM. V1
 */
const { request: undiciRequest } = require('undici');
const { proxyManager } = require('./proxies');
const { logbus } = require('./logbus');
const { engineConfig } = require('./config');

// 設定ページから変更可能（KB 指定）。既定 768KB = 360p 冒頭数秒分。
const warmBytes = () => Math.max(64, Number(engineConfig.get('warmBytes')) || 768) * 1024;
const MAX_ENTRIES = 48;             // ~36 MB worst-case RAM
const TTL = 6 * 60 * 1000;

class HotChunks {
  constructor() {
    this.map = new Map();      // key "v:itag" -> {buf:Buffer, exp}
    this.warming = new Set();  // keys currently being fetched
  }

  _key(v, itag) { return v + ':' + itag; }

  /** Returns Buffer slice when `start` is cached (partial tail allowed), else null. */
  get(v, itag, start, end) {
    const e = this.map.get(this._key(v, itag));
    if (!e) return null;
    if (e.exp < Date.now()) { this.map.delete(this._key(v, itag)); return null; }
    const s = Number(start);
    if (!(s >= 0) || s >= e.buf.length) return null;
    const en = end == null ? e.buf.length - 1 : Math.min(Number(end), e.buf.length - 1);
    if (en < s) return null;
    this.map.delete(this._key(v, itag)); this.map.set(this._key(v, itag), e); // recency
    return e.buf.subarray(s, en + 1);
  }

  /** Serve-from-memory if possible. Returns true when the response was fully handled. */
  serveIfHot(v, itag, req, res) {
    const m = /^bytes=(\d+)-(\d*)$/.exec(String(req.headers.range || ''));
    const k = this._key(v, itag);
    const entry = this.map.get(k);
    const fresh = entry && entry.exp >= Date.now() ? entry : null;
    if (!fresh) { if (entry) this.map.delete(k); return false; }
    // 高速化: ファイル全体が RAM に載っている（ショート等の小動画）場合は
    // Range 無しのフル GET も含めて上流へ渡さず即完了させる
    // （Content-Length が正確に載るのでプログレッシブ再生も正しく終端判定できる）
    if (!m) {
      if (!fresh.full) return false;
      this.map.delete(k); this.map.set(k, fresh); // recency
      res.writeHead(200, {
        'Content-Type': 'video/mp4',
        'Content-Length': fresh.buf.length,
        'Accept-Ranges': 'bytes',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=3600',
        'X-Hot-Cache': 'FULL',
      });
      res.end(fresh.buf);
      return true;
    }
    const start = Number(m[1]);
    if (m[2]) {
      // explicit end (MSE segment math!): only serving a FULLY covered range
      // is safe — a truncated segment would corrupt the append pipeline.
      if (Number(m[2]) > fresh.buf.length - 1) return false;
    }
    const end = m[2] ? Number(m[2]) : null;
    const buf = this.get(v, itag, start, end);
    if (!buf) return false;
    res.writeHead(206, {
      'Content-Type': 'video/mp4',
      'Content-Length': buf.length,
      'Content-Range': `bytes ${start}-${start + buf.length - 1}/${fresh.full ? fresh.buf.length : '*'}`,
      'Accept-Ranges': 'bytes',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=3600',
      'X-Hot-Cache': 'HIT',
    });
    res.end(buf);
    return true;
  }

  /**
   * Background warm. url/proxyUrl must come from the (already IP-pinned)
   * stream map — we never re-resolve here.
   */
  warm(v, itag, url, proxyUrl) {
    if (!url) return;
    const key = this._key(v, itag);
    const e = this.map.get(key);
    if ((e && e.exp > Date.now()) || this.warming.has(key)) return;
    this.warming.add(key);
    (async () => {
      const ac = new AbortController();
      const t = setTimeout(() => ac.abort(), 15000);
      const spanDone = logbus.span('stream', 'ホットキャッシュ先読み', { v, itag, kb: Math.round(warmBytes() / 1024), via: proxyUrl ? 'proxy' : 'direct' });
      try {
        const dispatcher = proxyManager.dispatcherForRelay(proxyUrl);
        const up = await undiciRequest(url, {
          method: 'GET',
          headers: {
            'User-Agent': 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip',
            'Range': `bytes=0-${warmBytes() - 1}`,
            'Accept': '*/*',
          },
          dispatcher,
          signal: ac.signal,
          headersTimeout: 12000,
        });
        if (up.statusCode >= 400) { up.body.dump().catch(() => {}); return; }
        // Content-Range から総ファイルサイズを読む（"bytes 0-786431/1234567"）
        const crHeader = String(up.headers['content-range'] || '');
        const totalMatch = /^bytes \d+-\d+\/(\d+)$/.exec(crHeader);
        const total = totalMatch ? Number(totalMatch[1]) : 0;
        const chunks = [];
        for await (const c of up.body) {
          chunks.push(c);
          let len = 0; for (const x of chunks) len += x.length;
          if (len >= warmBytes()) break;
        }
        ac.abort(); // we took what we need
        const WB = warmBytes();
        const buf = Buffer.concat(chunks).subarray(0, WB);
        if (buf.length >= 64 * 1024) {
          // full=true: ファイル全体（終端まで）が RAM に載っている状態。
          // ショート動画など小さいファイルはこれで完全サーブ＝上流往復ゼロ。
          this.map.set(key, { buf, exp: Date.now() + TTL, full: total > 0 && total <= buf.length });
          while (this.map.size > MAX_ENTRIES) this.map.delete(this.map.keys().next().value);
          spanDone({ ok: true, got: buf.length, full: total > 0 && total <= buf.length });
        } else {
          spanDone({ __warn: true, note: 'too small', got: buf.length });
        }
      } catch (e) {
        /* warming is best-effort — the relay path still works without it */
        spanDone({ __warn: true, err: e?.message || 'aborted' });
      } finally {
        clearTimeout(t);
        this.warming.delete(key);
      }
    })();
  }

  status() {
    return {
      entries: this.map.size,
      warming: this.warming.size,
      bytes: [...this.map.values()].reduce((a, e) => a + e.buf.length, 0),
    };
  }
}

const hotChunks = new HotChunks();
module.exports = { hotChunks };

'use strict';
/**
 * Piped last-resort provider for Vandal.
 * Used ONLY when every InnerTube client × transport combination fails
 * (the "LOGIN_REQUIRED wall" scenario). Public Piped instances proxy
 * googlevideo through their own hosts, so the URLs they return are NOT
 * IP-bound — the browser can play them directly, which also keeps our
 * server bandwidth at zero for this path.
 *
 * Resiliency: instances are raced in parallel, the first healthy answer
 * wins, the winner is cached, and repeated failures open a circuit
 * breaker so we never stall the player path on dead infrastructure.
 *
 * Vandal Project — independent open project.
 */

const DEFAULT_INSTANCES = [
  'api.piped.private.coffee',
  'pipedapi.adminforge.de',
  'pipedapi.kavin.rocks',
  'pipedapi.reallyaweso.me',
];

const REQUEST_TIMEOUT = 6000;
const BREAKER_TIME = 10 * 60 * 1000;

class PipedProvider {
  constructor() {
    this.instances = (process.env.VANDAL_PIPED || process.env.LLY_PIPED || '').split(',').map(s => s.trim()).filter(Boolean)
      .concat(DEFAULT_INSTANCES);
    this.instances = [...new Set(this.instances)];
    this.goodInstance = null;      // cached winner
    this.failures = 0;             // consecutive failures
    this.brokenUntil = 0;          // circuit breaker deadline
  }

  _blocked() {
    return this.brokenUntil && Date.now() < this.brokenUntil;
  }

  async _fetchInstance(host, videoId) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), REQUEST_TIMEOUT);
    try {
      const res = await fetch(`https://${host}/streams/${encodeURIComponent(videoId)}`, {
        signal: ac.signal,
        redirect: 'follow',
        headers: { 'User-Agent': 'Vandal/1.0', 'Accept': 'application/json' },
      });
      if (!res.ok) throw new Error('http ' + res.status);
      const j = await res.json();
      if (!j || !Array.isArray(j.videoStreams)) throw new Error('bad payload');
      return { host, j };
    } finally {
      clearTimeout(t);
    }
  }

  /**
   * Returns { progressive:[], videos:[], audios:[], title, author, host } or null.
   */
  async getStreams(videoId) {
    if (this._blocked()) return null;
    const order = [...this.instances];
    if (this.goodInstance) order.unshift(this.goodInstance);
    try {
      const win = await new Promise((resolve, reject) => {
        let pending = order.length;
        let settled = false;
        const errors = [];
        // 修正: 旧実装は「HTTP 成功だが videoStreams が空」の応答を pending カウント
        // から漏らしており、全インスタンスが空を返すと Promise が永久に未解決の
        // ままハングしていた（last-resort 経路全体が固まる重大バグ）。
        const checkDone = () => { if (!settled && pending <= 0) reject(errors[0] || new Error('all instances failed')); };
        for (const host of order) {
          this._fetchInstance(host, videoId)
            .then((r) => {
              if (settled) return;
              if (r?.j?.videoStreams?.length) { settled = true; resolve(r); return; }
              errors.push(new Error(host + ': empty streams'));
              pending--; checkDone();
            })
            .catch((e) => {
              if (settled) return;
              errors.push(e);
              pending--; checkDone();
            });
        }
      }).catch(() => null);
      if (!win) throw new Error('no winner');
      const normalized = normalize(win.j);
      if (!normalized.progressive.length && !normalized.videos.length) throw new Error('empty');
      this.goodInstance = win.host;
      this.failures = 0;
      normalized.host = win.host;
      return normalized;
    } catch (_) {
      this.failures++;
      if (this.failures >= 3) { this.brokenUntil = Date.now() + BREAKER_TIME; this.failures = 0; }
      return null;
    }
  }
}

function heightOf(quality) {
  const m = String(quality || '').match(/(\d{3,4})p/);
  return m ? Number(m[1]) : 0;
}

function isPlayableProxy(url) {
  // only actual videoplayback proxies (skip LBRY/HLS mirrors etc.)
  return typeof url === 'string' && /\/videoplayback[?/]/.test(url);
}

function normalize(j) {
  const vid = [];
  const aud = [];
  for (const v of j.videoStreams || []) {
    if (!isPlayableProxy(v.url) || v.format === 'HLS') continue;
    const mime = /WEBM/i.test(v.format) ? 'video/webm' : 'video/mp4';
    vid.push({
      itag: v.itag || 0,
      mime,
      codecs: v.codec || (mime === 'video/webm' ? 'vp9' : 'avc1.64001F'),
      qualityLabel: v.quality || '',
      height: heightOf(v.quality),
      width: 0, fps: v.fps || 30,
      bitrate: v.bitrate || 0,
      contentLength: 0,
      initRange: (v.initStart >= 0 && v.initEnd > 0) ? { start: String(v.initStart), end: String(v.initEnd) } : null,
      indexRange: (v.indexStart >= 0 && v.indexEnd > 0) ? { start: String(v.indexStart), end: String(v.indexEnd) } : null,
      isVideo: true, isAudio: false, hasUrl: true, cipher: null, url: v.url,
      videoOnly: !!v.videoOnly,
    });
  }
  for (const a of j.audioStreams || []) {
    if (!isPlayableProxy(a.url)) continue;
    const mime = /WEBM/i.test(a.format) ? 'audio/webm' : 'audio/mp4';
    aud.push({
      itag: a.itag || 0,
      mime,
      codecs: a.codec || (mime === 'audio/webm' ? 'opus' : 'mp4a.40.2'),
      qualityLabel: a.quality || '',
      bitrate: a.bitrate || 0,
      contentLength: 0,
      initRange: null, indexRange: null,
      isVideo: false, isAudio: true, hasUrl: true, cipher: null, url: a.url,
    });
  }
  const progressive = vid.filter(v => !v.videoOnly).sort((a, b) => b.height - a.height);
  const videos = vid.filter(v => v.videoOnly).sort((a, b) => b.height - a.height);
  aud.sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0));
  return {
    source: 'piped',
    title: j.title || '',
    author: j.uploader || '',
    channelId: '',
    lengthSeconds: Number(j.duration) || 0,
    viewCount: String(j.views || ''),
    progressive, videos, audios: aud,
    hls: null, // piped HLS mirrors are unreliable; progressive covers the fallback
  };
}

const piped = new PipedProvider();
module.exports = { piped };

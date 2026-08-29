'use strict';
/**
 * SigSolver — signature ("sig") and throttle ("n") decoder for YouTube player
 * responses. Uses yt-dlp's official standalone solver bundle (vendored in
 * server/vendor) which AST-extracts the real transform functions from the
 * current base.js and executes them — no fragile hand-written reverse
 * engineering. Results are memoized per player build and persisted to disk.
 *
 * Vandal Project — independent open project.
 */
const fs = require('node:fs');
const path = require('node:path');

const DATA_DIR = process.env.VERCEL ? '/tmp/vandal-data' : path.join(__dirname, '..', 'data');
const PLAYER_CACHE = path.join(DATA_DIR, 'player-cache.json');

// Last verified player build (2026-08, vandal smoke tests). Refresh happens
// automatically at runtime via watch-page discovery; this is just a seed.
const SEED_PLAYER_URLS = [
  '/s/player/b0d2d49a/player_es6.vflset/es_MX/base.js',
];

class SigSolver {
  constructor() {
    this._main = null;
    this._preprocessed = new Map(); // playerId -> preprocessed player js
    this._playerUrl = null;         // absolute base.js url currently in use
    this._ready = null;             // Promise<void>
  }

  _loadBundle() {
    if (this._main) return;
    const libSrc = fs.readFileSync(path.join(__dirname, 'vendor', 'yt.solver.lib.js'), 'utf8');
    const coreSrc = fs.readFileSync(path.join(__dirname, 'vendor', 'yt.solver.core.js'), 'utf8');
    const lib = new Function('"use strict";' + libSrc + '\n;return lib;')();
    this._main = new Function('meriyah', 'astring', '"use strict";' + coreSrc + '\n;return jsc;')(
      lib.meriyah, lib.astring
    );
  }

  _playerId(url) {
    const m = String(url).match(/\/s\/player\/([^/]+)\//);
    return m ? m[1] : null;
  }

  _loadDisk() {
    try {
      const raw = JSON.parse(fs.readFileSync(PLAYER_CACHE, 'utf8'));
      if (raw.preprocessed && typeof raw.preprocessed === 'object') {
        for (const [k, v] of Object.entries(raw.preprocessed)) {
          if (typeof v === 'string' && v.length > 1000) this._preprocessed.set(k, v);
        }
      }
      if (raw.playerUrl) this._playerUrl = raw.playerUrl;
    } catch (_) { /* first boot */ }
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) { /* noop */ }
  }

  _saveDisk() {
    try {
      fs.writeFileSync(PLAYER_CACHE, JSON.stringify({
        playerUrl: this._playerUrl,
        preprocessed: Object.fromEntries(this._preprocessed),
      }));
    } catch (_) { /* read-only fs */ }
  }

  get playerUrl() {
    return this._playerUrl;
  }

  /** Called with whatever PLAYER_JS_URL the watch page currently advertises. */
  notePlayerUrl(pathOrUrl) {
    if (!pathOrUrl) return;
    const abs = pathOrUrl.startsWith('http') ? pathOrUrl : 'https://www.youtube.com' + pathOrUrl;
    if (abs !== this._playerUrl) this._playerUrl = abs;
  }

  /**
   * Make sure we can decode. fetchText(url) must return base.js source and is
   * provided by the innertube transport layer (proxy aware).
   */
  async ensure(fetchText) {
    if (this._ready) return this._ready;
    this._ready = (async () => {
      this._loadBundle();
      this._loadDisk();
      const candidates = [];
      if (this._playerUrl) candidates.push(this._playerUrl);
      for (const s of SEED_PLAYER_URLS) candidates.push('https://www.youtube.com' + s);
      for (const url of [...new Set(candidates)]) {
        const id = this._playerId(url);
        const cached = id && this._preprocessed.get(id);
        try {
          if (cached) {
            // verify cached transform is still this player
            const out = this._main({ type: 'preprocessed', preprocessed_player: cached, requests: [{ type: 'sig', challenges: ['dGVzdA'] }] });
            if (out?.responses?.[0]?.type === 'result') { this._playerUrl = url; return; }
            this._preprocessed.delete(id);
          }
          const src = await fetchText(url);
          if (!src || src.length < 100000 || !src.includes('signatureCipher')) continue;
          const out = this._main({ type: 'player', player: src, output_preprocessed: true, requests: [{ type: 'sig', challenges: ['dGVzdA'] }] });
          if (out?.type !== 'result' || !out.preprocessed_player) continue;
          this._playerUrl = url;
          if (id) { this._preprocessed.set(id, out.preprocessed_player); this._saveDisk(); }
          return;
        } catch (_) { /* try next candidate */ }
      }
      throw new Error('solver-unavailable');
    })().catch(e => { this._ready = null; throw e; });
    return this._ready;
  }

  /**
   * Decode a batch of challenges of one type ('sig' | 'n').
   * Returns {challenge: decoded}.
   */
  solve(type, challenges) {
    if (!this._main || !this._playerUrl) throw new Error('solver-not-ready');
    const id = this._playerId(this._playerUrl);
    const pp = id && this._preprocessed.get(id);
    if (!pp) throw new Error('solver-not-ready');
    const uniq = [...new Set(challenges.filter(Boolean))];
    if (!uniq.length) return {};
    const out = this._main({ type: 'preprocessed', preprocessed_player: pp, requests: [{ type, challenges: uniq }] });
    if (out?.responses?.[0]?.type !== 'result') {
      // player rotated -> drop cache so next ensure() refetches
      this._preprocessed.delete(id);
      this._ready = null;
      throw new Error('solve-failed');
    }
    return out.responses[0].data || {};
  }
}

const sigSolver = new SigSolver();
module.exports = { sigSolver };

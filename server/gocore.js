'use strict';
/**
 * Go fetch-core client.
 *
 * When launch.mjs starts the Go edge it injects CORE_ORIGIN (loopback) and
 * CORE_TOKEN. We poll /health in the background and only send work to Go
 * once it answers — so a not-yet-listening core never adds latency (the
 * in-process hedge in ./hedge.js is used until then).
 *
 * Three consecutive Go failures flip ready=false and we stay on the Node
 * hedge for 4s. Ready flipping cannot make a request slower than the
 * historical sequential path.
 *
 * Pin/unpin are fire-and-forget; if Go is down they are no-ops (Node
 * still relays /api/stream itself).
 */
const { request: undiciRequest } = require('undici');
const nodeHedge = require('./hedge');

const CONNECT_MS = 250;

class GoCore {
  constructor() {
    this.origin = String(process.env.CORE_ORIGIN || '').replace(/\/+$/, '');
    this.token = String(process.env.CORE_TOKEN || '');
    this.ready = false;
    this.fails = 0;
    this.stats = { hedge: 0, fetch: 0, probe: 0, pin: 0, fallback: 0, goWins: 0 };
    this.lastHealth = null;
    if (this.origin) {
      this._poll();
      this._iv = setInterval(() => this._poll(), 4000);
      this._iv.unref?.();
    }
  }

  available() { return !!(this.origin && this.ready); }

  status() {
    return {
      origin: this.origin || null,
      ready: this.ready,
      fails: this.fails,
      stats: { ...this.stats },
      health: this.lastHealth,
    };
  }

  async _poll() {
    if (!this.origin) return;
    try {
      const j = await this._json('GET', '/health', null, 400);
      this.ready = !!(j && j.ok);
      this.lastHealth = j;
      if (this.ready) this.fails = 0;
    } catch (_) {
      this.fails += 1;
      if (this.fails >= 3) this.ready = false;
    }
  }

  async _json(method, path, body, timeoutMs) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const headers = { 'Accept': 'application/json' };
      if (this.token) headers['X-Persimmon-Core'] = this.token;
      let payload;
      if (body != null && method !== 'GET') {
        headers['Content-Type'] = 'application/json';
        payload = JSON.stringify(body);
      }
      const up = await undiciRequest(this.origin + path, {
        method,
        headers,
        body: payload,
        signal: ac.signal,
        headersTimeout: timeoutMs,
        bodyTimeout: timeoutMs,
      });
      const txt = await up.body.text();
      let j = null;
      try { j = txt ? JSON.parse(txt) : null; } catch (_) { j = null; }
      if (up.statusCode >= 400) {
        const err = new Error('core http ' + up.statusCode);
        err.status = up.statusCode;
        err.body = j;
        throw err;
      }
      return j;
    } finally {
      clearTimeout(t);
    }
  }

  _strip(req) {
    return {
      id: String(req.id || ''),
      method: req.method || 'GET',
      url: req.url,
      headers: req.headers || {},
      body: req.body || '',
      proxy: req.proxy || '',
      timeoutMs: req.timeoutMs || 9000,
    };
  }

  async hedge(requests, opts = {}) {
    const kind = opts.kind || 'first-2xx';
    if (this.available()) {
      try {
        // Go timeout = max of the individual timeouts + small slack, so we
        // never cut a still-legal attempt short. Connect is already proven
        // by the health poll.
        const maxMs = Math.max(2000, ...((requests || []).map(r => Number(r.timeoutMs) || 9000))) + 500;
        const j = await this._json('POST', '/v1/hedge', {
          kind,
          requests: (requests || []).map((r) => this._strip(r)),
        }, maxMs);
        this.stats.hedge += 1;
        if (j && j.ok) this.stats.goWins += 1;
        this.fails = 0;
        return j;
      } catch (_) {
        this.fails += 1;
        if (this.fails >= 3) this.ready = false;
        this.stats.fallback += 1;
      }
    } else {
      this.stats.fallback += 1;
    }
    return nodeHedge.hedge(requests, { kind });
  }

  async fetch(req) {
    this.stats.fetch += 1;
    if (this.available()) {
      try {
        const j = await this._json('POST', '/v1/fetch', this._strip(req), (Number(req.timeoutMs) || 9000) + 500);
        this.fails = 0;
        return j;
      } catch (_) {
        this.fails += 1;
        if (this.fails >= 3) this.ready = false;
      }
    }
    return nodeHedge.fetchSingle(req);
  }

  async probe(probes) {
    this.stats.probe += 1;
    if (this.available()) {
      try {
        const maxMs = Math.max(2000, ...((probes || []).map(p => Number(p.timeoutMs) || 4500))) + 500;
        const j = await this._json('POST', '/v1/probe', {
          probes: (probes || []).map((p) => this._strip(p)),
        }, maxMs);
        this.fails = 0;
        return j;
      } catch (_) {
        this.fails += 1;
        if (this.fails >= 3) this.ready = false;
      }
    }
    return nodeHedge.probe(probes);
  }

  async pin(payload) {
    if (!this.available() || !payload?.videoId) return false;
    this.stats.pin += 1;
    try {
      await this._json('POST', '/v1/pin', {
        videoId: payload.videoId,
        map: payload.map || {},
        proxyUrl: payload.proxyUrl || '',
        exp: payload.exp || 0,
        warmItag: payload.warmItag || '18',
      }, 1500);
      return true;
    } catch (_) {
      return false;
    }
  }

  async unpin(videoId) {
    if (!this.available() || !videoId) return;
    try {
      const ac = new AbortController();
      const t = setTimeout(() => ac.abort(), 800);
      const headers = {};
      if (this.token) headers['X-Persimmon-Core'] = this.token;
      await undiciRequest(this.origin + '/v1/pin?v=' + encodeURIComponent(videoId), {
        method: 'DELETE', headers, signal: ac.signal, headersTimeout: 800, bodyTimeout: 800,
      }).then((u) => u.body.dump?.().catch(() => {})).catch(() => {});
      clearTimeout(t);
    } catch (_) { /* noop */ }
  }
}

const gocore = new GoCore();
module.exports = { gocore, GoCore, CONNECT_MS };

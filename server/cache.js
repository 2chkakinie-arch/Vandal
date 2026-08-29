'use strict';
/**
 * Tiny dependency-free TTL + LRU cache.
 * Vandal Project — independent open project.
 */
class TTLCache {
  constructor({ max = 1000, ttl = 5 * 60 * 1000 } = {}) {
    this.max = max;
    this.ttl = ttl;
    this.map = new Map();
  }
  _expired(entry) {
    return !entry || (entry.exp !== 0 && entry.exp < Date.now());
  }
  get(key) {
    const e = this.map.get(key);
    if (this._expired(e)) {
      this.map.delete(key);
      return undefined;
    }
    // refresh recency
    this.map.delete(key);
    this.map.set(key, e);
    return e.value;
  }
  getStale(key) {
    const e = this.map.get(key);
    return e ? e.value : undefined;
  }
  set(key, value, ttl) {
    const exp = ttl === 0 ? 0 : Date.now() + (ttl === undefined ? this.ttl : ttl);
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, { value, exp });
    while (this.map.size > this.max) {
      const oldest = this.map.keys().next().value;
      this.map.delete(oldest);
    }
    return value;
  }
  has(key) {
    return this.get(key) !== undefined;
  }
  delete(key) {
    this.map.delete(key);
  }
  /** wc-prefix deletion (e.g. flush every 'vf:VIDEOID:*' variant at once) */
  deletePrefix(prefix) {
    for (const k of this.map.keys()) if (k.startsWith(prefix)) this.map.delete(k);
  }
  clear() {
    this.map.clear();
  }
  get size() {
    return this.map.size;
  }
  /**
   * Cache-aside helper: returns cached value or computes, stores and returns it.
   *
   * 高速化: 同じキーの同時計算を 1 本に束ねる（single-flight）。
   * ホバー先読み・視聴・warm が同時に同じ API を叩いたり、複数クライアントが
   * 同じ動画/検索を同時に開いた際に上流（無料プロキシ）への往復が
   * リクエスト数分ではなく必ず 1 本で済む。最初の呼び出し結果を全員が共有する。
   */
  async wrap(key, ttl, fn) {
    const hit = this.get(key);
    if (hit !== undefined) return hit;
    const inflight = this._wraps?.get(key);
    if (inflight) return inflight;
    if (!this._wraps) this._wraps = new Map();
    const job = (async () => {
      try {
        const value = await fn();
        this.set(key, value, ttl);
        return value;
      } finally {
        this._wraps.delete(key);
      }
    })();
    this._wraps.set(key, job);
    return job;
  }
}

module.exports = { TTLCache };

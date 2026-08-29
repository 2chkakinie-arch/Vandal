'use strict';/**
 * LogBus — サーバー内イベントログ基盤。
 *
 * サーバー内のあらゆる経路（プロキシ認定 / InnerTube 発行 / ストリーム中継 /
 * コメント取得 / 直結判定 / メッシュ …）をイベント化し、リングバッファ
 * （直近 N 件）に保持します。SSE 購読（subscribe）も提供。
 *
 * ログは channel(ch) で分類: proxy / player / stream / comments / meta / mesh /
 * engine / http。level: trace < debug < info < warn < error。
 *
 * Vandal Project — independent open project.
 */

const LEVELS = { trace: 10, debug: 20, info: 30, warn: 40, error: 50 };

class LogBus {
  constructor({ max = 4000 } = {}) {
    this.buf = [];
    this.max = max;
    this.subs = new Set(); // fn(event)
    this.seq = 0;
    this.minLevel = 'trace';
    this.counts = { total: 0, perSec: [] };
    this._winStart = Date.now();
    this._winCount = 0;
    this.rate = 0; // events/sec (rolling)
  }

  /** level filter — subs only receive events at or above minLevel */
  setMinLevel(lv) {
    if (LEVELS[lv]) this.minLevel = lv;
  }

  emit(level, ch, msg, data) {
    if (!LEVELS[level]) { level = 'info'; }
    this._winCount++;
    const now = Date.now();
    if (now - this._winStart >= 1000) {
      this.rate = Math.round((this._winCount * 1000) / (now - this._winStart) * 10) / 10;
      this._winStart = now; this._winCount = 0;
    }
    if (LEVELS[level] < LEVELS[this.minLevel]) return null;
    const ev = { seq: ++this.seq, ts: now, level, ch, msg: String(msg).slice(0, 400), data: undefined };
    // data must survive JSON — and never leak full URLs (privacy: strip nothing sensitive here,
    // callers pass timing/scalar objects)
    if (data !== undefined) {
      try {
        ev.data = JSON.parse(JSON.stringify(data));
        if (JSON.stringify(ev.data).length > 2000) ev.data = { note: 'truncated' };
      } catch (_) { ev.data = { note: 'unserializable' }; }
    }
    this.buf.push(ev);
    if (this.buf.length > this.max) this.buf.splice(0, this.buf.length - this.max);
    for (const sub of this.subs) {
      try { sub(ev); } catch (_) { /* subscriber died */ }
    }
    return ev;
  }

  trace(ch, msg, data) { return this.emit('trace', ch, msg, data); }
  debug(ch, msg, data) { return this.emit('debug', ch, msg, data); }
  info(ch, msg, data) { return this.emit('info', ch, msg, data); }
  warn(ch, msg, data) { return this.emit('warn', ch, msg, data); }
  error(ch, msg, data) { return this.emit('error', ch, msg, data); }

  /**
   * 実行区間のタイミングを自動計測して 1 イベントにまとめるヘルパ。
   * const done = log.span('player', 'issuance', {v}); ... done({ok:true})
   */
  span(ch, msg, meta = {}) {
    const t0 = Date.now();
    let finished = false;
    return (extra = {}) => {
      if (finished) return;
      finished = true;
      const ms = Date.now() - t0;
      const lv = extra.__error ? 'error' : (extra.__warn ? 'warn' : 'debug');
      const { __error, __warn, ...rest } = extra;
      this.emit(lv, ch, msg, { ms, ...meta, ...rest });
      return ms;
    };
  }

  subscribe(fn) {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }

  history({ since = 0, level, ch, limit = 600 } = {}) {
    let out = this.buf;
    if (since) out = out.filter(e => e.seq > since);
    if (level && LEVELS[level]) out = out.filter(e => LEVELS[e.level] >= LEVELS[level]);
    if (ch) out = out.filter(e => e.ch === ch);
    if (out.length > limit) out = out.slice(out.length - limit);
    return out;
  }

  status() {
    return { events: this.buf.length, subs: this.subs.size, rate: this.rate, minLevel: this.minLevel, seq: this.seq };
  }
}

const logbus = new LogBus();
module.exports = { logbus, LogBus, LEVELS };

'use strict';
/**
 * Health & telemetry — Vandal.
 *
 * インスタンスの「今の状態」を常時収集し、3 つの用途に供給する:
 *   1. /health / /healthz / /api/health エンドポイント（人間用ダッシュボード + 機械用 JSON）
 *   2. インスタンス協力メッシュの hello に乗るサマリ（ピアが相互に /health 確認する代わりに
 *      常時同じデータを交換する = 追加往復ゼロで「健全インスタンスの自動選別」ができる）
 *   3. ティア判定（S/A/B/C）のスコア計算 — CPU・イベントループ遅延・メモリ・
 *      直近の上流成功率・プロキシ認定数・稼働時間から 0〜100 のスコアを出す
 *
 * Vandal Project — independent open project.
 */
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');

const pkg = (() => {
  try { return JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8')); }
  catch (_) { return { version: '0.0.0' }; }
})();

class Health {
  constructor() {
    this.startedAt = Date.now();
    this.http = { active: 0, peak: 0, total: 0 };
    // 過去 60 秒のリクエスト数（リング）
    this._win = [];
    this._lag = 0;               // event-loop lag ms（指数移動平均）
    this._lagLoop = null;
  }

  start() {
    if (this._lagLoop) return;
    // event-loop lag: 100ms 周期タイマーの遅れを測る（ビジー具合の実測）
    let next = Date.now() + 100;
    this._lagLoop = setInterval(() => {
      const now = Date.now();
      const lag = Math.max(0, now - next);
      this._lag = this._lag ? (this._lag * 0.8 + lag * 0.2) : lag;
      next = now + 100;
      // 60 秒窓の RPS 記録
      this._win.push([now, this.http.total]);
      while (this._win.length > 2 && now - this._win[0][0] > 60000) this._win.shift();
    }, 100);
    if (this._lagLoop.unref) this._lagLoop.unref();
  }

  /** express middleware — 同時処理数 / 累計を数える */
  track(req, res, next) {
    this.http.active += 1;
    this.http.total += 1;
    if (this.http.active > this.http.peak) this.http.peak = this.http.active;
    res.on('close', () => { this.http.active = Math.max(0, this.http.active - 1); });
    next();
  }

  rps() {
    if (this._win.length < 2) return 0;
    const [t0, n0] = this._win[0];
    const [t1, n1] = this._win[this._win.length - 1];
    const dt = (t1 - t0) / 1000;
    return dt > 0 ? Math.max(0, Math.round(((n1 - n0) / dt) * 10) / 10) : 0;
  }

  /** 0〜100 の健全スコア（ティア判定の素） */
  score({ proxyPool = 0, proxyIssuers = 0, upstreamOkRate = 1 } = {}) {
    const lag = this._lag;
    const load = os.loadavg()[0] / Math.max(1, os.cpus().length); // 0〜(1=満載)
    const memUsed = 1 - os.freemem() / Math.max(1, os.totalmem());
    let s = 100;
    s -= Math.min(30, lag * 3);            // イベントループ詰まりは最重視（体感速度に直結）
    s -= Math.min(25, Math.max(0, load - 0.5) * 50);
    s -= Math.min(15, Math.max(0, memUsed - 0.85) * 100);
    s -= Math.min(20, (1 - upstreamOkRate) * 20);
    if (proxyPool <= 0) s -= 10;           // プロキシゼロ = 発行が direct 依存
    else s += Math.min(10, proxyPool / 10);
    if (proxyIssuers > 0) s += Math.min(10, proxyIssuers / 3);
    return Math.max(0, Math.min(100, Math.round(s)));
  }

  tierOf(score) {
    if (score >= 80) return 'S';
    if (score >= 60) return 'A';
    if (score >= 35) return 'B';
    return 'C';
  }

  /** メッシュ hello に乗る軽量サマリ（URL 等の秘密は含まない） */
  summary(extra = {}) {
    const proxyPool = extra.proxyPool ?? 0;
    const proxyIssuers = extra.proxyIssuers ?? 0;
    const okRate = extra.upstreamOkRate ?? 1;
    const score = this.score({ proxyPool, proxyIssuers, upstreamOkRate: okRate });
    return {
      cpu: Math.min(100, Math.round((os.loadavg()[0] / Math.max(1, os.cpus().length)) * 100)),
      cores: os.cpus().length,
      lag: Math.round(this._lag * 10) / 10,
      mem: Math.round((1 - os.freemem() / Math.max(1, os.totalmem())) * 100),
      active: this.http.active,
      rps: this.rps(),
      proxies: proxyPool,
      issuers: proxyIssuers,
      okRate: Math.round(okRate * 100) / 100,
      uptime: Math.round((Date.now() - this.startedAt) / 1000),
      score,
      tier: this.tierOf(score),
      version: pkg.version,
    };
  }

  /** /api/health 用の詳細版 */
  detail() {
    const mem = process.memoryUsage();
    const load = os.loadavg();
    return {
      ok: true,
      ts: Date.now(),
      uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
      version: pkg.version,
      node: process.version,
      platform: `${os.platform()}/${os.arch()}`,
      cpu: {
        model: (os.cpus()[0]?.model || '').split('@')[0].trim(),
        cores: os.cpus().length,
        load1: Math.round(load[0] * 100) / 100,
        load5: Math.round(load[1] * 100) / 100,
        load15: Math.round(load[2] * 100) / 100,
        busyPct: Math.min(100, Math.round((load[0] / Math.max(1, os.cpus().length)) * 100)),
      },
      memory: {
        rssMb: Math.round(mem.rss / 1048576),
        heapMb: Math.round(mem.heapUsed / 1048576),
        heapTotalMb: Math.round(mem.heapTotal / 1048576),
        systemFreeMb: Math.round(os.freemem() / 1048576),
        systemTotalMb: Math.round(os.totalmem() / 1048576),
        systemUsedPct: Math.round((1 - os.freemem() / Math.max(1, os.totalmem())) * 100),
      },
      eventLoop: { lagMs: Math.round(this._lag * 10) / 10 },
      http: { active: this.http.active, peak: this.http.peak, total: this.http.total, rps: this.rps() },
    };
  }
}

const health = new Health();
module.exports = { health, Health };

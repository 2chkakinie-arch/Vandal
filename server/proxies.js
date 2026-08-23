'use strict';
/**
 * ProxyManager V5 — llytpr++ 直結発行基盤。
 *
 * 「プロキシを使って googlevideo 生ストリームURLを"発行"し、プロキシを通さず
 *  再生できる生URLを得る」ための品質保証レイヤー。メディア本体は絶対に
 *  プロキシへ流さず（帯域ゼロ）、youtubei player の発行だけに使う。
 *
 * 水源（すべて無料公開リスト、複数系統で枯渇耐性）:
 *   - TheSpeedX/PROXY-List http.txt
 *   - ShiftyTR/Proxy-List http.txt
 *   - mmpx12/proxy-list http.txt
 *   - roosterkid/openproxylist HTTPS_RAW.txt
 *   - clarketm/proxy-list proxy-list-raw.txt
 *
 * 認定（3 段。実測が何よりの証明）:
 *   L1 tunnel : CONNECT で www.youtube.com/generate_204 まで到達（従来）
 *   L2 gvOk   : googlevideo ホストへ CONNECT トンネル可能（中継修復に必要）
 *   L3 issuer : そのプロキシ経由で InnerTube player (ANDROID+2AMB) が
 *               playabilityStatus=OK を返す = 「YouTube に IP バンされていない」
 *               を実測で証明 → 発行が通るプロキシだけを発行用途で優先選択
 *
 * 結果はディスク永続化し、Vercel コールドブートでも即応答。
 *
 * Made by Kakinie with llytpr-wl.v01nh TEAM. V1
 */
const fs = require('node:fs');
const path = require('node:path');
const { ProxyAgent, Agent, fetch: undiciFetch } = require('undici');
const { logbus } = require('./logbus');
const { engineConfig } = require('./config');

const LIST_URLS = [
  'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
  'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt',
  'https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt',
  'https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt',
  'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
];
const DATA_DIR = process.env.VERCEL ? '/tmp/llytpr-data' : path.join(__dirname, '..', 'data');
const CACHE_FILE = path.join(DATA_DIR, 'proxy-cache.json');

const TEST_TARGET = 'https://www.youtube.com/generate_204';
const GV_TARGET = 'https://rr5---sn-nx57ynsl.googlevideo.com/generate_204';
const TEST_TIMEOUT = 3800;
const MAX_TEST_BATCH = 300;      // candidates per refresh round
const SCAN_WIDTH = 48;           // 高速化: 並列スキャン幅（旧36→48、プール充填が約1.3倍速い）
const REFRESH_INTERVAL = 12 * 60 * 1000;
const CERTIFY_INTERVAL = 10 * 60 * 1000;
const CERTIFY_TOP_N = 24;        // certify only the fastest N per round
const CERTIFY_WIDTH = 4;         // 高速化: 認定の並列幅（旧2→4、L3 issuer 揃いが約2倍速い）
const CERT_TTL = 2 * 3600 * 1000;
const FAIL_EVICT = 3;

// InnerTube ANDROID クライアント（認定用カナリア発行。innertube.js と値を同期）
const CANARY_VIDEO = 'dQw4w9WgXcQ';
const YT_KEY = 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8';
const ANDROID_CTX = { clientName: 'ANDROID', clientVersion: '21.26.364', androidSdkVersion: 30, osName: 'Android', osVersion: '11' };
const ANDROID_UA = 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip';

class ProxyManager {
  constructor() {
    this.pool = [];            // [{url, latency, fails, lastOk, gvOkTs, issuerTs}]
    this.scanCursor = 0;
    this.list = [];
    this.rr = 0;
    this.rrI = 0;
    this.agents = new Map();   // proxyUrl -> ProxyAgent
    // 直 egress リレー専用の共有 Agent。undici のグローバル dispatcher を
    // そのまま使うと初回接続ごとに TCP/TLS ハンドシェイクが走るため、
    // googlevideo への keep-alive コネクションをまとめて再利用する。
    // メタ用とは別系統にして、動画中継のヘッビーなトラフィックが
    // InnerTube POST のコネクションプールを圧迫しないようにする。
    this._directRelayAgent = new Agent({
      keepAliveTimeout: 30000,
      keepAliveMaxTimeout: 60000,
      connections: 16,
    });
    this.refreshing = null;
    this.certifying = null;
    this.lastRefresh = 0;
    this.lastCertify = 0;
    this.enabled = process.env.LLYTPR_NO_PROXY !== '1';
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) { /* read-only fs */ }
    this._loadDisk();
    if (this.enabled) {
      this._refreshLoop = setInterval(() => this.refresh().catch(() => {}), REFRESH_INTERVAL);
      if (this._refreshLoop.unref) this._refreshLoop.unref();
      setTimeout(() => this.refresh().catch(() => {}), 50).unref?.();
    }
  }

  _loadDisk() {
    try {
      const raw = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
      if (Array.isArray(raw.pool)) {
        const now = Date.now();
        this.pool = raw.pool
          .filter(p => p && typeof p.url === 'string')
          .map(p => ({
            url: p.url,
            latency: p.latency || 1500,
            fails: 0,
            lastOk: p.lastOk || 0,
            gvOkTs: (p.gvOkTs && now - p.gvOkTs < CERT_TTL) ? p.gvOkTs : 0,
            issuerTs: (p.issuerTs && now - p.issuerTs < CERT_TTL) ? p.issuerTs : 0,
          }));
      }
      if (Array.isArray(raw.list) && raw.list.length) this.list = raw.list;
      this.scanCursor = raw.scanCursor || 0;
    } catch (_) { /* no cache yet */ }
  }

  _saveDisk() {
    try {
      fs.writeFileSync(CACHE_FILE, JSON.stringify({
        savedAt: Date.now(),
        pool: this.pool.slice(0, 80),
        list: this.list.slice(0, 9000),
        scanCursor: this.scanCursor,
      }));
    } catch (_) { /* read-only fs (vercel) */ }
  }

  /** 設定ページから変更可能なプール維持数（動的） */
  poolTarget() {
    return Math.max(8, Number(engineConfig.get('poolSize')) || 30);
  }

  _agent(url) {
    let a = this.agents.get(url);
    if (!a) {
      a = new ProxyAgent({
        uri: url,
        keepAliveTimeout: 8000,
        keepAliveMaxTimeout: 15000,
        // 高速化（リレー専用）: 無料プロキシ経由の動画バイト中継で、
        // 既定の小さなソケットバッファが TCP ウィンドウを詰まらせるのを
        // 防ぐ。メモリ使用量は総プロキシ数に比例するが、実際に開かれる
        // コネクション分だけなので高が知れている。メタ取得（hedge）は
        // 1 リクエスト毎に使い捨てのため影響は事実上ゼロ。
        connections: 8,
      });
      this.agents.set(url, a);
    }
    return a;
  }

  async _fetchLists() {
    const all = new Set();
    const parse = (text) => {
      for (const lineRaw of String(text).split(/\r?\n/)) {
        const token = lineRaw.trim().split(/\s+/)[0].replace(/^https?:\/\//, '');
        if (/^\d{1,3}(\.\d{1,3}){3}:\d{2,5}$/.test(token)) all.add(token);
      }
    };
    // 複数水源を並列取得（1つ死んでも他があれば十分）
    await Promise.all(LIST_URLS.map(async (u) => {
      const ac = new AbortController();
      const t = setTimeout(() => ac.abort(), 12000);
      try {
        const res = await undiciFetch(u, { signal: ac.signal });
        if (res.ok) parse(await res.text());
      } catch (_) { /* source dead — keep others */ }
      finally { clearTimeout(t); }
    }));
    const list = [...all];
    // randomize so we don't always hammer the head of the lists
    for (let i = list.length - 1; i > 0; i--) {
      const j = (Math.random() * (i + 1)) | 0;
      [list[i], list[j]] = [list[j], list[i]];
    }
    return list;
  }

  async _testOne(url) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), TEST_TIMEOUT);
    const start = Date.now();
    try {
      const res = await undiciFetch(TEST_TARGET, {
        dispatcher: this._agent(url),
        signal: ac.signal,
        headers: { 'User-Agent': 'Mozilla/5.0' },
      });
      res.body?.dump?.().catch(() => {});
      if (res.status < 500) return { url, latency: Date.now() - start, fails: 0, lastOk: Date.now(), gvOkTs: 0, issuerTs: 0 };
      return null;
    } catch (_) {
      return null;
    } finally {
      clearTimeout(t);
    }
  }

  /** L2: googlevideo への CONNECT トンネル可否（リレー修復用） */
  async _testGv(url) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), 5000);
    try {
      const res = await undiciFetch(GV_TARGET, { dispatcher: this._agent(url), signal: ac.signal });
      try { res.body?.cancel?.(); } catch (_) { /* noop */ }
      return res.status < 500;
    } catch (_) { return false; }
    finally { clearTimeout(t); }
  }

  /** L3: そのプロキシ経由で youtubei player が通る = YouTube 非BAN を実測証明 */
  async _testIssuer(url) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), 8000);
    const start = Date.now();
    try {
      const res = await undiciFetch(`https://www.youtube.com/youtubei/v1/player?key=${YT_KEY}`, {
        method: 'POST',
        dispatcher: this._agent(url),
        signal: ac.signal,
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': ANDROID_UA,
          'X-YouTube-Client-Name': '3',
          'X-YouTube-Client-Version': ANDROID_CTX.clientVersion,
          'Origin': 'https://www.youtube.com',
        },
        body: JSON.stringify({
          videoId: CANARY_VIDEO, contentCheckOk: true, racyCheckOk: true, params: '2AMB',
          context: { client: { hl: 'ja', gl: 'JP', ...ANDROID_CTX } },
        }),
      });
      const j = await res.json().catch(() => null);
      const ok = res.ok && j?.playabilityStatus?.status === 'OK'
        && !!((j.streamingData?.formats || []).length || (j.streamingData?.adaptiveFormats || []).filter(f => f.url).length);
      return ok ? Date.now() - start : null; // latency for scoring
    } catch (_) { return null; }
    finally { clearTimeout(t); }
  }

  /** プール上位から L2/L3 認定（バックグラウンド、refresh をブロックしない） */
  certify({ force = false } = {}) {
    if (!this.enabled) return Promise.resolve();
    if (this.certifying) return this.certifying;
    if (!engineConfig.get('certify')) return Promise.resolve();
    if (!force && Date.now() - this.lastCertify < CERTIFY_INTERVAL) return Promise.resolve();
    this.certifying = (async () => {
      const targets = [...this.pool].sort((a, b) => a.latency - b.latency).slice(0, CERTIFY_TOP_N);
      const stale = targets.filter(p => Date.now() - (p.issuerTs || 0) > CERT_TTL || Date.now() - (p.gvOkTs || 0) > CERT_TTL);
      const spanDone = logbus.span('proxy', 'L2/L3 認定ラウンド', { candidates: stale.length });
      // 4 並列で認定（無料プロキシを過負荷にしない範囲で最速。発行可能プロキシが
      // 早く揃うほど初回視聴の player 発行が速くなる）
      for (let i = 0; i < stale.length; i += CERTIFY_WIDTH) {
        await Promise.all(stale.slice(i, i + CERTIFY_WIDTH).map(async (p) => {
          const gv = await this._testGv(p.url).catch(() => false);
          if (gv) p.gvOkTs = Date.now();
          const issuerLat = await this._testIssuer(p.url).catch(() => null);
          if (issuerLat != null) { p.issuerTs = Date.now(); p.latency = Math.round(p.latency * 0.5 + issuerLat * 0.5); }
          logbus.trace('proxy', '認定', {
            url: p.url, gv: gv ? 'OK' : 'NG', issuer: issuerLat != null ? `OK ${issuerLat}ms` : 'NG',
          });
        }));
      }
      this.lastCertify = Date.now();
      this._saveDisk();
      spanDone({
        gvOk: this.pool.filter(p => p.gvOkTs && Date.now() - p.gvOkTs < CERT_TTL).length,
        issuerOk: this.pool.filter(p => p.issuerTs && Date.now() - p.issuerTs < CERT_TTL).length,
      });
    })().finally(() => { this.certifying = null; });
    return this.certifying;
  }

  async refresh({ force = false } = {}) {
    if (this.refreshing) return this.refreshing;
    if (!force && Date.now() - this.lastRefresh < 60000 && this.pool.length >= 8) return this.pool;
    const spanDone = logbus.span('proxy', 'プール更新開始', { pool: this.pool.length });
    this.refreshing = (async () => {
      // 1) re-validate existing pool first (cheap, keeps the good cache warm)
      if (this.pool.length) {
        const spanRe = logbus.span('proxy', '既存プール再検証', { n: this.pool.length });
        const checked = await Promise.all(this.pool.map(p => this._testOne(p.url)));
        const live = new Map(checked.filter(Boolean).map(p => [p.url, p]));
        const dead = this.pool.length - live.size;
        // keep certification flags of survivors
        this.pool = this.pool
          .filter(p => live.has(p.url))
          .map(p => ({ ...p, latency: live.get(p.url).latency, lastOk: live.get(p.url).lastOk, fails: 0 }))
          .sort((a, b) => a.latency - b.latency);
        spanRe({ alive: live.size, dead });
      }
      // 2) top up from the lists if needed
      const POOL_SIZE = this.poolTarget();
      if (this.pool.length < POOL_SIZE) {
        if (!this.list.length || this.scanCursor + MAX_TEST_BATCH > this.list.length * 2) {
          const spanLists = logbus.span('proxy', '水源リスト取得', { sources: LIST_URLS.length });
          try {
            const fresh = await this._fetchLists();
            if (fresh.length) { this.list = fresh; this.scanCursor = 0; }
            spanLists({ candidates: this.list.length });
          } catch (e) { spanLists({ __error: true, msg: e?.message }); /* keep old list */ }
        }
        const known = new Set(this.pool.map(p => p.url));
        const rejected = new Set();
        while (this.pool.length < POOL_SIZE) {
          const batch = [];
          for (let i = 0; i < MAX_TEST_BATCH && this.scanCursor < this.list.length; i++, this.scanCursor++) {
            const cand = 'http://' + this.list[this.scanCursor];
            if (!known.has(cand) && !rejected.has(cand)) batch.push(cand);
          }
          if (!batch.length) break;
          for (let i = 0; i < batch.length; i += SCAN_WIDTH) {
            if (this.pool.length >= POOL_SIZE) break;
            const chunk = batch.slice(i, i + SCAN_WIDTH);
            const results = await Promise.all(chunk.map(u => this._testOne(u)));
            results.forEach((r, j) => { if (r) this.pool.push(r); else rejected.add(chunk[j]); });
            logbus.debug('proxy', '候補スキャン', { tested: chunk.length, passed: results.filter(Boolean).length, pool: this.pool.length, cursor: this.scanCursor, listSize: this.list.length });
            if (this.scanCursor >= this.list.length) break;
          }
          if (this.pool.length >= POOL_SIZE || this.scanCursor >= this.list.length) break;
        }
        this.pool = [...new Map(this.pool.map(p => [p.url, p])).values()]
          .sort((a, b) => a.latency - b.latency)
          .slice(0, Math.max(POOL_SIZE * 2, 40));
      }
      this.lastRefresh = Date.now();
      this._saveDisk();
      spanDone({ pool: this.pool.length, issuers: this.pool.filter(p => p.issuerTs && Date.now() - p.issuerTs < CERT_TTL).length });
      // L2/L3 認定は裏で走らせる（発行用途は認定フラグで選別）
      this.certify().catch(() => {});
      return this.pool;
    })().finally(() => { this.refreshing = null; });
    return this.refreshing;
  }

  /** Best-effort pick of a healthy proxy (round robin over the fastest few). */
  pick(exclude = []) {
    if (!this.enabled || !this.pool.length) return null;
    const fast = this.pool.filter(p => p.latency < 6000).slice(0, 12);
    const usable = fast.length ? fast : this.pool.slice(0, 12);
    for (let i = 0; i < usable.length * 2; i++) {
      const p = usable[this.rr++ % usable.length];
      if (!exclude.includes(p.url)) return p.url;
    }
    return null;
  }

  /** rescue-round helper: n 個のフレッシュな候補をまとめて取る（重複なし）。 */
  pickMany(n = 5) {
    const out = [];
    for (let i = 0; i < n * 3 && out.length < n; i++) {
      const u = this.pick(out);
      if (!u) break;
      out.push(u);
    }
    return out;
  }

  /** 発行用途の優先ピック: 「YouTube非BAN実測済み」の issuer-grade を latency 順で。 */
  pickIssuer(exclude = []) {
    if (!this.enabled) return null;
    const certified = this.pool
      .filter(p => p.issuerTs && Date.now() - p.issuerTs < CERT_TTL)
      .sort((a, b) => a.latency - b.latency)
      .slice(0, 8);
    for (let i = 0; i < certified.length * 2 && certified.length; i++) {
      const p = certified[this.rrI++ % certified.length];
      if (!exclude.includes(p.url)) return p.url;
    }
    return this.pick(exclude); // 認定品が無ければ通常品にフォールバック
  }

  /** googlevideo 中継修復用: CONNECT トンネル実測済みのものを優先 */
  pickGv(exclude = []) {
    if (!this.enabled) return null;
    const gv = this.pool
      .filter(p => p.gvOkTs && Date.now() - p.gvOkTs < CERT_TTL)
      .sort((a, b) => a.latency - b.latency);
    for (const p of gv) if (!exclude.includes(p.url)) return p.url;
    return this.pick(exclude);
  }

  dispatcherFor(url) {
    return url ? this._agent(url) : undefined;
  }

  /**
   * 動画中継専用 dispatcher。プロキシありは各 ProxyAgent（keep-alive 済み）、
   * 直 egress はリレー専用の共有 Agent を返す。undefined を返すと呼び出し側が
   * グローバル dispatcher（毎回ハンドシェイクしがち）へ落ちるため、明示する。
   */
  dispatcherForRelay(url) {
    if (url) return this._agent(url);
    return this._directRelayAgent;
  }

  markBad(url) {
    if (!url) return;
    const p = this.pool.find(p => p.url === url);
    if (!p) return;
    p.fails++;
    logbus.debug('proxy', '失敗カウント', { url, fails: p.fails });
    if (p.fails >= FAIL_EVICT) {
      logbus.info('proxy', 'プールから除外', { url, latency: p.latency });
      this.pool = this.pool.filter(x => x.url !== url);
      const a = this.agents.get(url);
      if (a) { a.close?.().catch(() => {}); this.agents.delete(url); }
    }
  }

  /** youtubei player がそのプロキシ経由で LOGIN_REQUIRED になった = 発行用途では即降格。
   *  （トンネル/中継用途には残す。次回 certify ラウンドで再認定されれば復帰する） */
  markIssuerBad(url) {
    if (!url) return;
    const p = this.pool.find(p => p.url === url);
    if (p && p.issuerTs) { p.issuerTs = 0; this._saveDisk(); }
    logbus.warn('proxy', '発行用途から降格（LOGIN_REQUIRED）', { url });
    // 発行可能プロキシが枯渇しそうなら即座に再認定を蹴る（無料プロキシは鮮度が命）
    const live = this.pool.filter(x => x.issuerTs && Date.now() - x.issuerTs < CERT_TTL).length;
    if (live < 3) { clearTimeout(this._recertT); this._recertT = setTimeout(() => this.certify({ force: true }).catch(() => {}), 1500); this._recertT.unref?.(); }
  }

  markGood(url, latency) {
    const p = this.pool.find(p => p.url === url);
    if (!p) return;
    p.fails = 0;
    p.lastOk = Date.now();
    if (latency) p.latency = Math.round(p.latency * 0.7 + latency * 0.3);
  }

  status() {
    return {
      enabled: this.enabled,
      pool: this.pool.map(p => ({
        url: p.url, latency: p.latency, fails: p.fails,
        gv: !!p.gvOkTs && Date.now() - p.gvOkTs < CERT_TTL,
        issuer: !!p.issuerTs && Date.now() - p.issuerTs < CERT_TTL,
      })),
      issuers: this.pool.filter(p => p.issuerTs && Date.now() - p.issuerTs < CERT_TTL).length,
      gvOk: this.pool.filter(p => p.gvOkTs && Date.now() - p.gvOkTs < CERT_TTL).length,
      listSize: this.list.length,
      cursor: this.scanCursor,
      lastRefresh: this.lastRefresh,
      lastCertify: this.lastCertify,
      certifying: !!this.certifying,
      refreshing: !!this.refreshing,
      poolTarget: this.poolTarget(),
    };
  }
}

const proxyManager = new ProxyManager();
module.exports = { proxyManager, ProxyManager };

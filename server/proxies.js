'use strict';
/**
 * ProxyManager V5 — Vandal 直結発行基盤。
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
 * Vandal Project — independent open project.
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
const DATA_DIR = process.env.VERCEL ? '/tmp/vandal-data' : path.join(__dirname, '..', 'data');
const CACHE_FILE = path.join(DATA_DIR, 'proxy-cache.json');
const SHARDS = 8; // プロキシ収集の分業シャード数（メッシュで各インスタンスが別シャードを担当）

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
// ---- メッシュ協力による「プール無限肥大化」対策（読み込みが遅くなった主因の根治）:
// ピアからプロキシを共有してもらい続けるとプールは数百件へ膨らみ、旧実装は
// refresh() ごとに「プール全件を同時再実測」してイベントループを詰まらせていた。
// 上限を設け、再実測は上位のみ・分割で回す。
const REVALIDATE_TOP = 30;       // refresh 時に再実測するのは上位30件まで
const REVALIDATE_WIDTH = 15;     // 再実測の同時幅（全件一斉 → 15 本ずつ）
const ADOPT_MIN_INTERVAL = 25 * 1000; // ピアからの取り込みレート制限
const SAVE_MIN_INTERVAL = 20 * 1000;  // ディスク保存の間引き（書き込みストーム防止）
const STALE_EVICT = 35 * 60 * 1000;   // 再実測対象外で長く使っていないものは退出

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
    // ※ keepAliveTimeout は 15 秒: 接続先がアイドル接続を先に切っても
    //   「放置後の再接続で死んだソケットを掴む」バグ（放置後の動画が異常に
    //   遅くなる原因）を避ける。undici は keepAliveTimeout 超過のソケットを
    //   再利用しないので、放置からの復帰が常に新規接続から始まる。
    this._directRelayAgent = new Agent({
      keepAliveTimeout: 15 * 1000,
      keepAliveMaxTimeout: 30 * 1000,
      connections: 16,
      connect: { timeout: 6000 },
    });
    this.refreshing = null;
    this.certifying = null;
    this.lastRefresh = 0;
    this.lastCertify = 0;
    this.enabled = process.env.VANDAL_NO_PROXY !== '1';
    // 分業: このインスタンスが担当するプロキシ候補シャード（表示名から決定論的に導出。
    // メッシュの各インスタンスが別シャードを走査するので、集合としての発見速度が上がる）
    this.shard = this._shardOf(process.env.VANDAL_MESH_NAME || process.env.RENDER_EXTERNAL_URL || process.env.HOSTNAME || 'vandal');
    this.stats = { ok: 0, bad: 0, since: Date.now() }; // 直近の上流成否（health スコア用）
    this._rev = 1;            // プール変更世代（メッシュ共有の差分検出に使う）
    this._lastAdopt = 0;      // adoptProxies のレート制限
    this._lastSave = 0;       // _saveDisk の間引き
    this._adopting = null;    // 取り込み実行中 promise（同時多発を 1 本に束ねる）
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) { /* read-only fs */ }
    this._loadDisk();
    if (this.enabled) {
      this._refreshLoop = setInterval(() => this.refresh().catch(() => {}), REFRESH_INTERVAL);
      if (this._refreshLoop.unref) this._refreshLoop.unref();
      setTimeout(() => this.refresh().catch(() => {}), 50).unref?.();
    }
  }

  _shardOf(name) {
    let h = 5381;
    const s = String(name || 'vandal');
    for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
    return h % SHARDS;
  }

  issuerCount() {
    const now = Date.now();
    return this.pool.filter((p) => p.issuerTs && now - p.issuerTs < CERT_TTL).length;
  }

  gvCount() {
    const now = Date.now();
    return this.pool.filter((p) => p.gvOkTs && now - p.gvOkTs < CERT_TTL).length;
  }

  /** 直近の上流成功率（0〜1）。サンプルが少ないうちは 1（悲観しない） */
  okRate() {
    const { ok, bad, since } = this.stats;
    const total = ok + bad;
    if (total < 5) return 1;
    const ageH = (Date.now() - since) / 3600000;
    const decay = Math.pow(0.5, Math.max(0, ageH) * 2); // 古いサンプルは 2 時間で半減
    const o = ok * decay + 2, b = bad * decay + 0.5;    // ラプラス平滑化
    return Math.min(1, o / (o + b));
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

  _saveDisk(force = false) {
    const now = Date.now();
    if (!force && now - this._lastSave < SAVE_MIN_INTERVAL) return; // 書き込みストーム対策（取り込みのたびに同期書き込みしていたのを間引き）
    this._lastSave = now;
    try {
      fs.writeFileSync(CACHE_FILE, JSON.stringify({
        savedAt: now,
        pool: this.pool.slice(0, 80),
        list: this.list.slice(0, 9000),
        scanCursor: this.scanCursor,
      }));
    } catch (_) { /* read-only fs (vercel) */ }
  }

  /** プールの変更世代（メッシュが「共有すべき差分があるか」を O(1) で判定するため） */
  version() { return this._rev; }
  _touch() { this._rev = (this._rev + 1) >>> 0; }

  /** プールの絶対上限（メッシュ取り込みで際限なく膨らませない） */
  poolCap() { return Math.min(160, this.poolTarget() * 3 + 40); }

  /** 設定ページから変更可能なプール維持数（動的） */
  poolTarget() {
    return Math.max(8, Number(engineConfig.get('poolSize')) || 30);
  }

  _agent(url) {
    let a = this.agents.get(url);
    if (!a) {
      a = new ProxyAgent({
        uri: url,
        // keep-alive は 4 秒（undici 既定相当）: 無料プロキシはアイドル接続を
        // 数秒で切るものが多く、長い keep-alive は「放置後の最初のリクエストが
        // 死んだソケットを掴んで数秒〜数十秒固まる」バグを生む。短期間だけ
        // 再利用し、放置からの復帰は常に新鮮な接続で行う。
        keepAliveTimeout: 4000,
        keepAliveMaxTimeout: 8000,
        connect: { timeout: 5000 },
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
      //    高速化（重要）: 旧実装は「プール全件を Promise.all で一斉再実測」していた。
      //    メッシュでプールが数百件に膨らむと、この一斉テストがイベントループと
      //    ソケットを飽和させ、全リクエスト（ページ読み込み含む）が数十秒遅くなる
      //    「共同機能を入れたら激重になった」本体だった。上位のみ・分割で再実測する。
      if (this.pool.length) {
        const spanRe = logbus.span('proxy', '既存プール再検証', { n: this.pool.length });
        const ranked = [...this.pool].sort((a, b) =>
          ((b.issuerTs ? 2 : 0) + (b.gvOkTs ? 1 : 0)) - ((a.issuerTs ? 2 : 0) + (a.gvOkTs ? 1 : 0)) || a.latency - b.latency);
        const targets = ranked.slice(0, REVALIDATE_TOP);
        const live = new Map();
        for (let i = 0; i < targets.length; i += REVALIDATE_WIDTH) {
          const chunk = targets.slice(i, i + REVALIDATE_WIDTH);
          const checked = await Promise.all(chunk.map(p => this._testOne(p.url)));
          checked.forEach((r, j) => { if (r) live.set(chunk[j].url, r); });
        }
        const before = this.pool.length;
        this.pool = this.pool
          .filter(p => live.has(p.url) || targets.every(t => t.url !== p.url)) // 再実測したものは生存のみ
          .map(p => live.has(p.url)
            ? { ...p, latency: live.get(p.url).latency, lastOk: live.get(p.url).lastOk, fails: 0 }
            : p);
        // 再実測対象外で長期間使っていないもの（= メッシュ取り込みの古い在庫）は退出
        const staleCut = Date.now() - STALE_EVICT;
        this.pool = this.pool.filter(p => p.issuerTs || p.gvOkTs || !p.lastOk || p.lastOk > staleCut);
        this.pool.sort((a, b) => a.latency - b.latency);
        if (this.pool.length !== before) this._touch();
        spanRe({ alive: live.size, dead: targets.length - live.size, evicted: before - this.pool.length });
      }
      // 2) top up from the lists if needed
      const POOL_SIZE = this.poolTarget();
      if (this.pool.length < POOL_SIZE) {
        if (!this.list.length || this.scanCursor + MAX_TEST_BATCH > this.list.length * 2) {
          const spanLists = logbus.span('proxy', '水源リスト取得', { sources: LIST_URLS.length });
          try {
            const fresh = await this._fetchLists();
            if (fresh.length) {
              this.list = fresh;
              // 分業: 各インスタンスは候補リストの別シャード（担当区間）から走査を
              // 始める。同じリストを全員が先頭から舐めると発見が重複し非効率だが、
              // シャード分割なら集合として異なる候補を同時実測でき、メッシュ経由で
              // 全員のプールが足し合わされる（= 収集速度がインスタンス数倍になる）。
              this.scanCursor = Math.floor((this.shard / SHARDS) * this.list.length);
            }
            spanLists({ candidates: this.list.length, shard: `${this.shard}/${SHARDS}` });
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
        this._touch();
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

  /**
   * 高速化（救済レースの要）: player 発行が全滅した緊急ローテーションで使う軽量補充。
   * 旧実装は refresh({force:true}) を await していた — プール全件の再実測 +
   * 場合によっては水源リスト取得（最大12秒）までユーザーリクエストの中で待つ
   * 仕組みで、メッシュでプールが肥大化してからは「1 本の動画の救済に 10 秒以上」
   * が频発した。この関数は既存プールに触らず、未テストの候補だけを最大 n 本
   * SCAN_WIDTH 幅で数ブロック実測して即返る（目安 1〜4 秒・最悪でも TEST_TIMEOUT）。
   */
  async quickTopUp(n = 24) {
    if (!this.enabled) return this.pool;
    try {
      if (!this.list.length) {
        const fresh = await this._fetchLists();
        if (fresh.length) {
          this.list = fresh;
          this.scanCursor = Math.floor((this.shard / SHARDS) * this.list.length);
        }
      }
      const known = new Set(this.pool.map(p => p.url));
      const budget = Math.ceil(n / 0.35); // 通過率 ~35% を想定した試行本数
      const batch = [];
      while (batch.length < budget && this.scanCursor < this.list.length) {
        const cand = 'http://' + this.list[this.scanCursor++];
        if (!known.has(cand)) batch.push(cand);
      }
      if (this.scanCursor >= this.list.length) this.scanCursor = 0; // 一周したら先頭へ
      for (let i = 0; i < batch.length && this.pool.length < this.poolCap(); i += SCAN_WIDTH) {
        const chunk = batch.slice(i, i + SCAN_WIDTH);
        const results = await Promise.all(chunk.map(u => this._testOne(u).catch(() => null)));
        results.forEach((r) => { if (r && this.pool.length < this.poolCap()) this.pool.push(r); });
      }
      this._touch();
      this._saveDisk();
      return this.pool;
    } catch (_) { return this.pool; }
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
    this.stats.bad += 1;
    if (this.stats.ok + this.stats.bad > 5000) { this.stats.ok = 0; this.stats.bad = 0; this.stats.since = Date.now(); }
    const p = this.pool.find(p => p.url === url);
    if (!p) return;
    p.fails++;
    logbus.debug('proxy', '失敗カウント', { url, fails: p.fails });
    if (p.fails >= FAIL_EVICT) {
      logbus.info('proxy', 'プールから除外', { url, latency: p.latency });
      this.pool = this.pool.filter(x => x.url !== url);
      this._touch();
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
    this.stats.ok += 1;
    if (this.stats.ok + this.stats.bad > 5000) { this.stats.ok = 1; this.stats.bad = 0; this.stats.since = Date.now(); }
    const p = this.pool.find(p => p.url === url);
    if (!p) return;
    p.fails = 0;
    p.lastOk = Date.now();
    if (latency) p.latency = Math.round(p.latency * 0.7 + latency * 0.3);
  }

  /**
   * インスタンス協力メッシュ用: 自インスタンスが持つ「生きている」プロキシを
   * 他インスタンスへ共有する形に整えて返す（URL と実測メタのみ。資格情報は無い）。
   * issuer/gv 認定済みを優先し、遅延順に最大 N 件。
   * 高速化: hello/state から高頻度で呼ばれるため 30 秒キャッシュ + プール世代
   * キャッシュを挟み、毎回の全件ソートを廃止（肥大プールで CPU を食っていた）。
   */
  exportProxies(limit = 40) {
    const now = Date.now();
    if (this._exportCache && this._exportCache.limit === limit
      && this._exportCache.rev === this._rev && now - this._exportCache.ts < 30 * 1000) {
      return this._exportCache.arr;
    }
    const live = this.pool
      .filter(p => p.lastOk && now - p.lastOk < 10 * 60 * 1000)
      .map(p => ({
        url: p.url,
        latency: p.latency,
        issuer: !!(p.issuerTs && now - p.issuerTs < CERT_TTL),
        gv: !!(p.gvOkTs && now - p.gvOkTs < CERT_TTL),
      }))
      .sort((a, b) => (b.issuer - a.issuer) || (b.gv - a.gv) || (a.latency - b.latency));
    const arr = live.slice(0, limit);
    this._exportCache = { limit, rev: this._rev, ts: now, arr };
    return arr;
  }

  /** 共有候補の件数だけ欲しいときの O(1) 近似（state() 用 — exportProxies を呼ばない） */
  shareCount(limit = 40) {
    if (this._exportCache && this._exportCache.limit === limit
      && this._exportCache.rev === this._rev && Date.now() - this._exportCache.ts < 30 * 1000) {
      return this._exportCache.arr.length;
    }
    const now = Date.now();
    let n = 0;
    for (const p of this.pool) { if (p.lastOk && now - p.lastOk < 10 * 60 * 1000) { n++; if (n >= limit) return limit; } }
    return n;
  }

  /**
   * インスタンス協力メッシュ用: ピアから受信したプロキシをプールへ取り込む。
   *
   * 高速化・安定化（旧実装の問題点）:
   *  - 30 秒ごとの hello（＋最大3ホップのゴシップ転送）ごとに最大 180 件を
   *    取り込み、プールが数百件へ際限なく膨らんだ → refresh() の全件一斉再実測で
   *    イベントループが固まり、全ページの読み込みが激重になった。
   *  - 未認定 60 件 × 3.8 秒の実測を hello のたびに走らせ、帯域と CPU を食い潰した。
   *  対策: 取り込みはレート制限（25 秒に 1 回・並行呼び出しは 1 本に束ねる）、
   *  認定済みのみ即採用（上限あり）、未認定はプールが減っているときだけ軽く実測。
   */
  adoptProxies(list = []) {
    if (!this.enabled || !Array.isArray(list) || !list.length) return Promise.resolve(0);
    const now = Date.now();
    if (now - this._lastAdopt < ADOPT_MIN_INTERVAL) return Promise.resolve(0); // レート制限
    if (this._adopting) return this._adopting;                                // 同時多発を 1 本へ
    this._lastAdopt = now;
    this._adopting = this._adoptProxiesNow(list)
      .catch(() => 0)
      .finally(() => { this._adopting = null; });
    return this._adopting;
  }

  async _adoptProxiesNow(list = []) {
    const cap = this.poolCap();
    if (this.pool.length >= cap) return 0;
    const known = new Set(this.pool.map(p => p.url));
    const incoming = [];
    for (const p of list) {
      if (!p || typeof p.url !== 'string') continue;
      if (known.has(p.url)) continue;
      if (!/^https?:\/\/\d{1,3}(\.\d{1,3}){3}:\d{2,5}$/.test(p.url)) continue;
      known.add(p.url);
      incoming.push(p);
    }
    if (!incoming.length) return 0;

    const certed = incoming.filter(p => p.issuer); // issuer 認定（YouTube非BAN実測済み）のみ即採用
    let added = 0;
    const room = cap - this.pool.length;
    for (const p of certed.slice(0, room)) {
      this.pool.push({
        url: p.url,
        latency: Math.min(Math.max(Number(p.latency) || 1500, 50), 10000),
        fails: 0,
        lastOk: Date.now(),
        gvOkTs: p.gv ? Date.now() : 0,
        issuerTs: Date.now(),
      });
      added++;
    }
    // 未認定は「プールが目標を下回っている」ときだけ少量実測して補う
    if (this.pool.length < this.poolTarget()) {
      const rest = incoming.filter(p => !p.issuer).slice(0, 12);
      for (let i = 0; i < rest.length; i += 6) {
        const chunk = rest.slice(i, i + 6);
        const res = await Promise.all(chunk.map(p => this._testOne(p.url).catch(() => null)));
        for (const r of res) {
          if (!r || this.pool.length >= cap || this.pool.some(x => x.url === r.url)) continue;
          this.pool.push(r);
          added++;
        }
      }
    }
    if (added) {
      this.pool = [...new Map(this.pool.map(p => [p.url, p])).values()]
        .sort((a, b) => a.latency - b.latency)
        .slice(0, cap);
      this._touch();
      this._saveDisk();
      logbus.info('mesh', 'ピアからプロキシを採用', { added, total: this.pool.length, cap });
    }
    return added;
  }

  status() {
    return {
      enabled: this.enabled,
      pool: this.pool.map(p => ({
        url: p.url, latency: p.latency, fails: p.fails,
        gv: !!p.gvOkTs && Date.now() - p.gvOkTs < CERT_TTL,
        issuer: !!p.issuerTs && Date.now() - p.issuerTs < CERT_TTL,
      })),
      issuers: this.issuerCount(),
      gvOk: this.gvCount(),
      listSize: this.list.length,
      cursor: this.scanCursor,
      lastRefresh: this.lastRefresh,
      lastCertify: this.lastCertify,
      certifying: !!this.certifying,
      refreshing: !!this.refreshing,
      poolTarget: this.poolTarget(),
      shard: { index: this.shard, count: SHARDS },
      okRate: Math.round(this.okRate() * 100) / 100,
    };
  }
}

const proxyManager = new ProxyManager();
module.exports = { proxyManager, ProxyManager };

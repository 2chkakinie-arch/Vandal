'use strict';
/**
 * Vandal インスタンス協力メッシュ（Instance Cooperation Mesh）— v2「自動参加」。
 *
 * デプロイされた URL が確定した瞬間から自動でメッシュに参加し、
 *  - 自インスタンスの公開 URL（=「Vandal インスタンスリスト」への自動追加）
 *  - 生きている（実測済み）プロキシ
 *  - /health 相当の混み具合・CPU・イベントループ・プロキシ数のサマリ
 * をピアと常時共有する。VANDAL_MESH_URL / VANDAL_MESH_PEERS は**不要**（任意の上書き用に残置）。
 *
 * ゼロ設定で動く仕組み:
 *   1. 自 URL 自動検出 — Render / Vercel / Railway / Koyeb / Fly / Heroku などの
 *      プラットフォーム注入環境変数をスキャン。無ければ最初に公開リクエストが来た
 *      ときの Host / X-Forwarded-Proto から学習してディスクに永続化する。
 *      =「デプロイして URL ができた時点で」自動でリストに載る。
 *   2. ゴシップ型ピア発見 — hello に「知っているピア URL 一覧」を乗せ、受け取った
 *      側は未知の URL へ自ら接続しに行く（基数爆発は MAX_PEERS / バックオフで抑制）。
 *      ピア一覧はディスク永続化され、再起動後も自動で再接続する。
 *   3. 健全性の相互監視 — hello に health サマリ（CPU・lag・アクティブ処理・
 *      プロキシ数・スコア）を常時同梱。さらに URL を持つピアの /healthz を
 *      2 分ごとに実測して RTT / 到達性を更新する。
 *   4. 役割分担（スマート分業）— スコアで S/A/B/C ティアを自動選別し、
 *      - 上位ティア（S/A）へメタ取得（player 発行）を委譲（getVideoFull から並行発火）
 *      - プロキシ収集は各インスタンスが候補リストの別シャードを担当し、
 *        見つけた生きているプロキシを全員で共有（網羅速度がインスタンス数倍に）
 *      される。集中管理は存在しない（各ノードが同じルールで同じ結論を出す）。
 *
 * プライバシー:
 *   - デフォルトでは自 URL を UI 上のユーザーへ表示しない（内部の共同作業には全インスタンスが参加）。
 *   - data/config.json の "meshPrivate": true（または VANDAL_MESH_PRIVATE=1）で
 *     URL を一切外へ出さない匿名参加モードに切り替え可能（プロキシ共有は継続）。
 *
 * Vandal Project — independent open project.
 */
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');
const { request: undiciRequest } = require('undici');
const { logbus } = require('./logbus');
const { engineConfig } = require('./config');
const { health } = require('./health');

const MESH_PATH = '/.vandal-mesh';
const HELLO_INTERVAL = 30 * 1000;
const PEER_PROBE_INTERVAL = 120 * 1000;
const MAX_PEERS = 64;
const MAX_KNOWN = 200;
const HOP_LIMIT = 3;
const PROXY_SHARE_LIMIT = 40;
const PEER_DEAD_MS = 3 * 60 * 1000;          // hello が来ないピアは非 alive 扱い
const REGISTRY_TTL = 7 * 24 * 3600 * 1000;   // 発見済み URL の保持期間
const DATA_DIR = process.env.VERCEL ? '/tmp/vandal-data' : path.join(__dirname, '..', 'data');
const REG_FILE = path.join(DATA_DIR, 'mesh-registry.json');

/* ------------------------------------------------ self URL auto-detection */

function normalizeBase(u) {
  if (!u || typeof u !== 'string') return null;
  let s = String(u).trim().replace(/\/+$/, '');
  if (!s) return null;
  if (!/^https?:\/\//i.test(s)) {
    if (/^[\w.-]+(:\d+)?$/.test(s) && !/^[\d.]+(:\d+)?$/.test(s)) s = 'https://' + s; // hostname only
    else if (/^wss?:\/\//i.test(s)) s = s.replace(/^ws/i, 'http').replace(/\/\.vandal-mesh$/, '');
    else return null;
  }
  try { const x = new URL(s); if (!/^https?$/.test(x.protocol.replace(':', ''))) return null; return x.origin; } catch (_) { return null; }
}

function detectSelfUrl() {
  const E = process.env;
  const cands = [
    E.VANDAL_MESH_URL,
    E.RENDER_EXTERNAL_URL,
    E.VERCEL_PROJECT_PRODUCTION_URL, E.VERCEL_URL,
    E.RAILWAY_PUBLIC_DOMAIN, E.RAILWAY_STATIC_URL,
    E.KOYEB_PUBLIC_DOMAIN, E.KOYEB_APP_DOMAIN,
    E.FLY_APP_NAME ? `https://${E.FLY_APP_NAME}.fly.dev` : null,
    E.HEROKU_APP_NAME ? `https://${E.HEROKU_APP_NAME}.herokuapp.com` : null,
    E.WEBSITE_HOSTNAME,                     // Azure App Service
    E.REPLIT_DEV_DOMAIN,
    E.CODESPACE_NAME ? `https://${E.CODESPACE_NAME}-3000.app.github.dev` : null,
    E.GITPOD_WORKSPACE_URL,
    E.DETA_SPACE_APP_HOST,
    E.VANDAL_PUBLIC_URL,
  ];
  for (const c of cands) {
    const u = normalizeBase(c);
    if (u) return { url: u, source: c === E.VANDAL_MESH_URL ? 'env(VANDAL_MESH_URL)' : 'platform-env' };
  }
  return null;
}

/** Host ヘッダが「学習に値する公開ホスト」か（localhost / プライベート IP / 内部ポートは除外） */
function isPublicHost(host) {
  if (!host) return false;
  const h = String(host).toLowerCase().split(':')[0];
  if (!h || h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal')) return false;
  if (/^127\./.test(h) || /^10\./.test(h) || /^192\.168\./.test(h) || /^169\.254\./.test(h)) return false;
  if (/^172\.(1[6-9]|2\d|3[01])\./.test(h)) return false;
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(h)) return false; // 生 IP は安定しないので学習しない
  return true;
}

/** 公開 URL の仮名化表示 — 「デフォルトでは URL をユーザーに見せない」ための表記。
 *  内部の共同作業（WS hello・委譲）には完全な URL を使うので機能は一切変わらない。 */
function maskUrl(u) {
  if (!u) return null;
  try {
    const x = new URL(u);
    const h = x.hostname;
    if (h.length <= 18) return h;
    const parts = h.split('.');
    if (parts.length > 2) return `${parts[0].slice(0, 12)}…${parts.slice(-2).join('.')}`;
    return `${h.slice(0, 12)}…`;
  } catch (_) { return String(u).slice(0, 18) + '…'; }
}

function wsUrlFrom(base) {
  if (!base) return null;
  if (/^wss?:\/\//i.test(base)) return base;
  return base.replace(/^https?/i, (m) => (m.toLowerCase() === 'https' ? 'wss' : 'ws')) + MESH_PATH;
}

class Mesh {
  constructor(proxyManager) {
    this.pm = proxyManager;
    this.started = false;
    this.name = process.env.VANDAL_MESH_NAME || 'vandal';
    this.envSelfUrl = normalizeBase(process.env.VANDAL_MESH_URL || '');
    this.selfUrl = null;          // 実効的な自 URL（env → 検出 → 学習）
    this.selfSource = null;
    this.envPeerUrls = (process.env.VANDAL_MESH_PEERS || '')
      .split(',')
      .map((s) => normalizeBase(s))
      .filter(Boolean);
    this.wss = null;
    this.peers = new Map();       // key(conn) -> {ws, url, name, ts, proxies, health, tier, ms, fails}
    this.outgoing = new Map();    // baseUrl -> WebSocket
    this.registry = new Map();    // baseUrl -> {addedAt, lastSeen, name}（永続化される発見済みピア）
    this._pulse = null;
    this._maint = null;
    this._probe = null;
    this._connectTries = new Map(); // baseUrl -> attempts
    this._delegateJobs = new Map(); // videoId -> in-flight delegate promise（重複委譲を1本に束ねる）
    this._lastShareRev = -1;       // 最後に proxies を共有したプール世代（差分共有用）
    this._lastShareTs = 0;
    this.capToken = require('node:crypto').randomBytes(12).toString('hex'); // delegate 認証（hello で共有）
    this._loadRegistry();
  }

  get isActive() { return this.started && !!engineConfig.get('meshEnabled'); }
  get isPrivate() { return !!engineConfig.get('meshPrivate'); }
  get canDelegate() { return this.isActive && !!engineConfig.get('meshDelegate'); }

  /* ------------------------------------------------------------ registry */

  _loadRegistry() {
    try {
      const raw = JSON.parse(fs.readFileSync(REG_FILE, 'utf8'));
      const now = Date.now();
      if (raw && typeof raw === 'object') {
        if (!this.envSelfUrl && raw.selfUrl) { this.selfUrl = normalizeBase(raw.selfUrl); this.selfSource = 'learned'; }
        for (const [u, e] of Object.entries(raw.peers || {})) {
          if (e && now - (e.addedAt || 0) < REGISTRY_TTL) this.registry.set(u, e);
        }
      }
    } catch (_) { /* first boot */ }
  }

  _saveRegistry() {
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      const peers = {};
      for (const [u, e] of this.registry) peers[u] = e;
      fs.writeFileSync(REG_FILE, JSON.stringify({
        savedAt: Date.now(),
        selfUrl: this.selfUrl || null,
        peers,
      }, null, 1));
    } catch (_) { /* read-only fs */ }
  }

  _rememberPeer(url, name = '') {
    if (!url || url === this.selfUrl || this.registry.has(url)) { if (url && url !== this.selfUrl) this.registry.get(url) && (this.registry.get(url).lastSeen = Date.now()); return false; }
    this.registry.set(url, { addedAt: Date.now(), lastSeen: Date.now(), name });
    if (this.registry.size > MAX_KNOWN) {
      // 古いものから削る
      const ent = [...this.registry.entries()].sort((a, b) => a[1].lastSeen - b[1].lastSeen);
      for (const [u] of ent.slice(0, this.registry.size - MAX_KNOWN)) this.registry.delete(u);
    }
    this._saveRegistry();
    return true;
  }

  /** 公開リクエストから自 URL を学習する（express middleware）。「URL ができた時点でリストへ自動追加」の本体。 */
  learnFromRequest(req, res, next) {
    try {
      if (req.method !== 'GET') return next();
      const p = req.path || '';
      if (p === MESH_PATH || p.startsWith('/api/mesh')) return next();
      if (this.isPrivate) return next();
      const host = req.headers['x-forwarded-host'] || req.headers.host;
      if (!isPublicHost(host)) return next();
      const proto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() || (req.socket?.encrypted ? 'https' : 'http');
      if (proto !== 'https' && proto !== 'http') return next();
      const url = `${proto}://${host}`;
      if (this.envSelfUrl || this.selfUrl === url) return next();
      // 学習は「最初に見た公開 Host で確定」（sticky）。逆プロキシ配下では Host は
      // 信頼できるが、直接アクセスされる場合は偽装 Host で広告 URL を書き換えられ
      // ないようにする。確定値は data/mesh-registry.json に永続化され、再起動後も
      // 変わらない。変更したいときは VANDAL_MESH_URL か設定ページを使う。
      if (this.selfUrl) return next();
      this.selfUrl = url;
      this.selfSource = 'learned';
      this._saveRegistry();
      logbus.info('mesh', '公開 URL を自動検出しました（メッシュへ共有されます）', { url });
    } catch (_) { /* never break a request */ }
    return next();
  }

  /* ---------------------------------------------------------- lifecycle */

  /** 受信側（他インスタンスから接続される側）の WebSocket サーバーを HTTP サーバーへ取り付ける。 */
  attach(server) {
    if (this.wss) return;
    this.wss = new WebSocketServer({ noServer: true });
    server.on('upgrade', (req, socket, head) => {
      let pathname = '/';
      try { pathname = new URL(req.url || '/', 'http://x').pathname; } catch (_) { /* keep '/' */ }
      if (pathname !== MESH_PATH) { socket.destroy(); return; }
      this.wss.handleUpgrade(req, socket, head, (ws) => this.wss.emit('connection', ws, req));
    });
    this.wss.on('connection', (ws, req) => this._onPeer(ws, req));
    logbus.info('mesh', 'メッシュ受信サーバー起動', { path: MESH_PATH });
  }

  /**
   * デフォルトで自動参加。VANDAL_MESH_URL / VANDAL_MESH_PEERS が無くても:
   *   - 受信サーバーで他インスタンスの接続を受け、
   *   - 過去に学習した（ディスク保存された）ピアへ自動再接続し、
   *   - env ピア / ゴシップで発見したピアへ接続する。
   * meshEnabled=false（設定 or VANDAL_MESH=0）のときだけ完全単独運用になる。
   */
  start() {
    if (this.started) return;
    this.started = true;
    health.start();
    if (!engineConfig.get('meshEnabled')) {
      logbus.info('mesh', 'メッシュ無効（設定 meshEnabled=false）— 単独運用');
      return;
    }
    const det = detectSelfUrl();
    if (det && !this.envSelfUrl) { this.selfUrl = det.url; this.selfSource = det.source; }
    if (this.envSelfUrl) { this.selfUrl = this.envSelfUrl; this.selfSource = 'env(VANDAL_MESH_URL)'; }
    // env ピア + 保存済みレジストリを接続対象へ
    const seeds = new Map();
    for (const u of this.envPeerUrls) seeds.set(u, 'env');
    for (const [u] of this.registry) if (!seeds.has(u)) seeds.set(u, 'registry');
    this._pulse = setInterval(() => this._pulseNow().catch(() => {}), HELLO_INTERVAL);
    if (this._pulse.unref) this._pulse.unref();
    this._maint = setInterval(() => this._maintain(), 20 * 1000);
    if (this._maint.unref) this._maint.unref();
    this._probe = setInterval(() => this._probePeers().catch(() => {}), PEER_PROBE_INTERVAL);
    if (this._probe.unref) this._probe.unref();
    for (const [u] of seeds) this.connect(u);
    logbus.info('mesh', 'メッシュ自動参加', {
      self: this.selfUrl || '(初リクエスト時に学習)', selfSource: this.selfSource,
      seeds: seeds.size, mode: this.isPrivate ? 'private' : 'public',
    });
    // 初回は少し遅らせて実行（プロキシプール暖機後だと共有できるものがある）
    setTimeout(() => this._pulseNow().catch(() => {}), 4000).unref?.();
  }

  connect(baseUrl) {
    const url = wsUrlFrom(baseUrl);
    if (!url || this.outgoing.has(url) || this.outgoing.has(baseUrl)) return;
    const tries = this._connectTries.get(baseUrl) || 0;
    let ws;
    try { ws = new WebSocket(url); } catch (_) { return; }
    // 注: 同一ソケットを 2 つのキーで索引するが、ピア実体は peers の 1 エントリのみ。
    this.outgoing.set(url, ws);
    this.outgoing.set(baseUrl, ws);
    ws.on('open', () => {
      this._connectTries.delete(baseUrl);
      // 発信側も peers に正式登録（hello 相互交換で health / ティア / 委譲先として双方向が使える）
      if (this.peers.size < MAX_PEERS) {
        this.peers.set(baseUrl, { ws, url: '', name: '', ts: Date.now(), proxies: [], health: null, tier: 'C', ms: null, fails: 0, dir: 'out', cap: '' });
      }
      logbus.info('mesh', 'ピアへ接続', { url });
      this._send(ws, this._hello());
    });
    ws.on('message', (data) => this._onMessage(ws, data, baseUrl));
    ws.on('close', () => {
      this.outgoing.delete(url);
      this.outgoing.delete(baseUrl);
      this.peers.delete(baseUrl);
      // 接続失敗は指数バックオフ（最大 10 分）で再試行。成功でリセット。
      const t = Math.min(600 * 1000, 5000 * 2 ** Math.min(7, tries));
      this._connectTries.set(baseUrl, tries + 1);
      setTimeout(() => { if (this.started && engineConfig.get('meshEnabled')) this.connect(baseUrl); }, t).unref?.();
    });
    ws.on('error', () => { try { ws.close(); } catch (_) { /* noop */ } });
  }

  /* --------------------------------------------------------- inbound side */

  _onPeer(ws, req) {
    if (this.peers.size >= MAX_PEERS) { try { ws.close(); } catch (_) {} return; }
    const key = 'in:' + (req?.socket?.remoteAddress || '') + ':' + (Math.random() + '').slice(2, 8);
    this.peers.set(key, { ws, url: '', name: '', ts: Date.now(), proxies: [], health: null, tier: 'C', ms: null, fails: 0, dir: 'in', cap: '' });
    ws.on('message', (data) => this._onMessage(ws, data, key));
    ws.on('close', () => this.peers.delete(key));
    ws.on('error', () => { try { ws.close(); } catch (_) {} });
  }

  _onMessage(ws, data, key) {
    let msg;
    try { msg = JSON.parse(String(data)); } catch (_) { return; }
    if (!msg || typeof msg !== 'object') return;
    if (msg.type === 'hello') this._onHello(ws, key, msg);
  }

  _onHello(ws, key, msg) {
    const remoteUrl = normalizeBase(msg.url);
    // 自己エコー防御: ゴシップで巡ってきた「自分自身の hello」はピアとして数えず流さない
    if (remoteUrl && remoteUrl === this.selfUrl) return;
    // 直接 hello（hops=0）には即座に自分の hello を返す。旧実装は相手の 30 秒パルスを
    // 待つしかなく、接続直後の 30 秒間が「health 無しの匿名ピア」になっていた。
    if (msg.hops === 0) this._send(ws, this._hello());
    const proxies = Array.isArray(msg.proxies) ? msg.proxies.slice(0, PROXY_SHARE_LIMIT) : [];
    const p = this.peers.get(key);
    if (p) {
      p.url = remoteUrl || '';
      p.name = typeof msg.name === 'string' ? msg.name.slice(0, 60) : '';
      p.ts = Date.now();
      p.proxies = proxies;
      p.health = this._sanitizeHealth(msg.health);
      p.tier = p.health?.tier || 'C';
      p.cap = typeof msg.cap === 'string' ? msg.cap.slice(0, 64) : (p.cap || '');
    }

    // ピアの生きているプロキシを自プールへ取り込む（サーバー側集計の本体）
    if (proxies.length) this.pm.adoptProxies(proxies).catch(() => {});

    // ピアが知っている URL を登録（自動発見）し、未知なら接続しに行く
    if (Array.isArray(msg.knows)) {
      for (const raw of msg.knows.slice(0, 40)) {
        const u = normalizeBase(raw);
        if (!u || u === this.selfUrl) continue;
        if (this._rememberPeer(u, '')) {
          logbus.info('mesh', 'ピアを自動発見', { url: u, via: p?.url || p?.name || 'anonymous' });
          if (this.outgoing.size < MAX_PEERS) this.connect(u);
        }
      }
    }
    if (remoteUrl && this._rememberPeer(remoteUrl, p?.name || '')) {
      logbus.info('mesh', 'インスタンスリストへ自動追加', { url: remoteUrl, name: msg.name || '' });
    }

    // ゴシップ: 他ピアへ転送（ホップ数制限）。ソケット同一性で比較するので
    // 二重索引（ws:// と http:// の両キー）でも同じ線に返す自己エコーが起きない。
    // 高速化（重要）: 転送メッセージから proxies と cap は剥がす。
    //  - proxies を乗せたまま 3 ホップ転送すると、1 回の 30 秒パルスが
    //    ピア数²×40 件へ増幅され、各ノードが取り込み実測で CPU/帯域を食い潰して
    //    いた（「共同機能を入れたら読み込みが激重」の第二要因）。
    //    プロキシ共有は直結ピアとの hello だけで完結する。
    //  - cap（委譲トークン）は直結ピアにのみ開示する。
    if (typeof msg.hops === 'number' && msg.hops < HOP_LIMIT) {
      const fwd = { ...msg, knows: undefined, proxies: undefined, cap: undefined, hops: msg.hops + 1 };
      for (const [, q] of this.peers) if (q.ws !== ws) this._send(q.ws, fwd);
      const seen = new Set(this.peers.values().map((q) => q.ws));
      for (const [, o] of this.outgoing) if (o !== ws && !seen.has(o)) this._send(o, fwd);
    }
  }

  _sanitizeHealth(h) {
    if (!h || typeof h !== 'object') return null;
    const num = (x, lo, hi) => (Number.isFinite(+x) ? Math.min(hi, Math.max(lo, +x)) : null);
    const out = {
      cpu: num(h.cpu, 0, 100), cores: num(h.cores, 1, 4096), lag: num(h.lag, 0, 10000),
      mem: num(h.mem, 0, 100), active: num(h.active, 0, 1e6), rps: num(h.rps, 0, 1e6),
      proxies: num(h.proxies, 0, 1e5), issuers: num(h.issuers, 0, 1e5),
      uptime: num(h.uptime, 0, 1e9), score: num(h.score, 0, 100),
      tier: ['S', 'A', 'B', 'C'].includes(h.tier) ? h.tier : 'C',
      okRate: num(h.okRate, 0, 1), version: typeof h.version === 'string' ? h.version.slice(0, 16) : '',
    };
    return out;
  }

  _send(ws, obj) {
    try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); } catch (_) { /* noop */ }
  }

  async _pulseNow() {
    if (!engineConfig.get('meshEnabled')) return;
    const hello = this._hello();
    const seen = new Set();
    for (const [, p] of this.peers) { this._send(p.ws, hello); seen.add(p.ws); }
    // peers 未登録の接続中ソケット（open 直後〜hello 受信前）にも初回 hello を届かせる
    for (const [, o] of this.outgoing) if (!seen.has(o)) this._send(o, hello);
  }

  /** 死んだピアの掃除 + レジストリ枯れの再接続（20 秒ごと） */
  _maintain() {
    if (!engineConfig.get('meshEnabled')) return;
    const now = Date.now();
    for (const [k, p] of [...this.peers]) {
      if (now - p.ts > PEER_DEAD_MS) { try { p.ws.close(); } catch (_) {} this.peers.delete(k); }
    }
    // 生きている接続が 0 なら、レジストリの中から未接続のものへ再接続を試す
    if (this.outgoing.size === 0 && this.registry.size) {
      const candidates = [...this.registry.keys()].filter((u) => !wsUrlFrom(u) || !this.outgoing.has(wsUrlFrom(u)));
      for (const u of candidates.slice(0, 3)) this.connect(u);
    }
  }

  /** URL を持つピアの /healthz を実測して RTT / 到達性を更新（「/health 自動確認」） */
  async _probePeers() {
    if (!engineConfig.get('meshEnabled')) return;
    const urls = new Set();
    for (const [, p] of this.peers) if (p.url) urls.add(p.url);
    for (const [u, e] of this.registry) { if (!urls.has(u)) urls.add(u); if (urls.size > 24) break; }
    const list = [...urls].filter((u) => u && u !== this.selfUrl).slice(0, 8);
    await Promise.all(list.map(async (u) => {
      const t0 = Date.now();
      const ok = await this._healthz(u);
      const ms = Date.now() - t0;
      for (const [, p] of this.peers) if (p.url === u) { p.ms = ok ? ms : p.ms; if (!ok) p.fails++; }
      const e = this.registry.get(u);
      if (e) e.lastSeen = ok ? Date.now() : e.lastSeen;
      if (ok && this.pm) logbus.trace('mesh', 'ピア health 確認', { url: u, ms });
    }));
  }

  async _healthz(baseUrl) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), 5000);
    try {
      const up = await undiciRequest(baseUrl + '/healthz', {
        method: 'GET', signal: ac.signal, headersTimeout: 4500, bodyTimeout: 4500,
        headers: { 'User-Agent': 'vandal-mesh/2' },
        maxRedirections: 1,
      });
      up.body.dump?.().catch(() => {});
      return up.statusCode >= 200 && up.statusCode < 500;
    } catch (_) {
      return false;
    } finally { clearTimeout(t); }
  }

  _hello() {
    const h = health.summary({
      proxyPool: this.pm.pool.length,
      proxyIssuers: this.pm.issuerCount(),
      upstreamOkRate: this.pm.okRate(),
    });
    // 高速化（差分共有）: プールに変化がない限り proxies を乗せない。
    // 旧実装は 30 秒ごとに全ピアへ 40 件を再送し続け、受信側も毎回取り込み
    // 処理を起動していた。世代 (pm.version) が同じなら健康情報だけで十分。
    const now = Date.now();
    const poolChanged = this.pm.version() !== this._lastShareRev;
    const shareDue = now - (this._lastShareTs || 0) > 180 * 1000; // 3 分に 1 回は鮮度再送
    const includeProxies = poolChanged || shareDue;
    if (includeProxies) { this._lastShareRev = this.pm.version(); this._lastShareTs = now; }
    const msg = {
      type: 'hello',
      v: 2,
      url: this.isPrivate ? '' : (this.selfUrl || ''),
      name: this.isPrivate ? this.name + ' (private)' : this.name,
      private: this.isPrivate,
      hops: 0,
      ts: now,
      cap: this.capToken, // 委譲 API の認証用（メッシュ参加者だけが WS 経由で知り得る）
      proxies: includeProxies ? this.pm.exportProxies(PROXY_SHARE_LIMIT) : [],
      health: h,
      knows: this.isPrivate ? [] : [...this.registry.keys()].slice(0, 24),
    };
    return msg;
  }

  /* ------------------------------------------------------- 分業 (delegate) */

  /** 委譲先に値する（S/A ティアの）生きているピアをスコア順で返す。
   *  cap（委譲トークン）を持つピアのみ — 無いと委譲 API が 403 で必ず死ぬ。 */
  pickPeer({ minTier = 'A', exclude = [] } = {}) {
    if (!this.canDelegate) return null;
    const rank = { S: 3, A: 2, B: 1, C: 0 };
    const now = Date.now();
    const alive = [...this.peers.values()]
      .filter((p) => p.url && p.cap && p.health && now - p.ts < PEER_DEAD_MS && rank[p.tier] >= rank[minTier] && !exclude.includes(p.url))
      .sort((a, b) => (b.health.score || 0) - (a.health.score || 0));
    return alive[0] ? { url: alive[0].url, name: alive[0].name, health: alive[0].health, cap: alive[0].cap || '' } : null;
  }

  peerCount() {
    const now = Date.now();
    return [...this.peers.values()].filter((p) => now - p.ts < PEER_DEAD_MS).length;
  }

  healthyPeerCount() {
    const now = Date.now();
    return [...this.peers.values()].filter((p) => p.health && now - p.ts < PEER_DEAD_MS && p.health.score >= 35).length;
  }

  /**
   * 上位ティアのピアへ player 発行（メタ取得）を委譲する。
   * 呼び出し側は Promise を並行走らせ、自前発行が失敗したときだけ結果を採用する
   * （成功時は 1ms も待たない = 遅くならない）。
   * 高速化: 同じ動画の委譲が並行したら 1 本に束ねる（弱インスタンスが同時に
   * 同じ動画を開いたとき、強ピアへ重複発行して互いに遅くするのを防ぐ）。
   * 既定タイムアウトも 10s → 4.5s: 救済レースは「先に帰った方」を採用するので、
   * 長く待つほどユーザーの初速を削ぐだけだった。
   */
  delegatePlayer(videoId, { timeoutMs = 4500 } = {}) {
    if (!videoId) return Promise.resolve(null);
    const inflight = this._delegateJobs.get(videoId);
    if (inflight) return inflight;
    const job = this._delegatePlayerNow(videoId, timeoutMs)
      .catch(() => null)
      .finally(() => { this._delegateJobs.delete(videoId); });
    this._delegateJobs.set(videoId, job);
    return job;
  }

  _delegatePlayerNow(videoId, timeoutMs) {
    const peer = this.pickPeer({ minTier: 'A' });
    if (!peer) return Promise.resolve(null);
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    const t0 = Date.now();
    return undiciRequest(peer.url + '/api/mesh/delegate/player', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Vandal-Mesh': peer.cap, 'User-Agent': 'vandal-mesh/2' },
      body: JSON.stringify({ videoId }),
      signal: ac.signal,
      headersTimeout: timeoutMs,
      bodyTimeout: timeoutMs,
      maxRedirections: 1,
    }).then(async (up) => {
      const txt = await up.body.text();
      if (up.statusCode !== 200) throw new Error('delegate http ' + up.statusCode);
      const j = JSON.parse(txt);
      if (!j?.ok || !j.player?.__urlMap) throw new Error('delegate payload');
      logbus.info('mesh', 'メタ取得をピアへ委譲して成功', { peer: peer.url, v: videoId, ms: Date.now() - t0 });
      return j.player;
    }).catch((e) => {
      logbus.debug('mesh', '委譲は失敗（自前経路で継続）', { peer: peer.url, v: videoId, err: e?.message });
      return null;
    }).finally(() => { clearTimeout(t); });
  }

  /** delegate 用トークン検証（ピアから呼ばれる側） */
  checkDelegateToken(req) {
    return String(req.headers['x-vandal-mesh'] || '') === this.capToken;
  }

  /** 設定変更などですぐに状態を共有し直したいときの即時パルス */
  nudge() {
    if (!this.started) return;
    this._maintain();
    this._pulseNow().catch(() => {});
  }

  /* ---------------------------------------------------------------- state */

  /** /api/mesh/state とインスタンスリストページ用の公開情報。
   *  full=true はループバック（デプロイ者本人の /health 確認）など限られた呼び出しのみ。
   *  高速化: exportProxies（全件ソート）を毎回呼ばず shareCount() で済ませ、
   *  roles() も 1 回だけ計算する（旧実装は毎ポーリングで 2 回計算していた）。 */
  state({ full = false } = {}) {
    const show = (u) => (u ? (full ? u : maskUrl(u)) : null);
    const now = Date.now();
    const enabled = !!engineConfig.get('meshEnabled');
    const list = [];
    for (const [, p] of this.peers) {
      if (now - p.ts > PEER_DEAD_MS) continue;
      list.push({
        url: show(p.url),
        name: p.name || (p.url ? maskUrl(p.url) : 'anonymous peer'),
        anonymous: !p.url,
        proxies: p.proxies.length,
        health: p.health,
        tier: p.tier,
        ms: p.ms,
        lastSeen: p.ts,
        alive: true,
      });
    }
    // 接続試行中 / レジストリ既知だが未接続の URL も「既知」として下段に載せる
    for (const [u, e] of this.registry) {
      // 自分自身の URL（学習前に書かれた場合など）はピアとして載せない
      if (u === this.selfUrl || list.some((x) => x.url === show(u))) continue;
      const stale = now - (e.lastSeen || e.addedAt || 0) > PEER_DEAD_MS;
      list.push({
        url: show(u), name: e.name || maskUrl(u), anonymous: false, proxies: 0, health: null,
        tier: stale ? 'C' : 'B', ms: null, lastSeen: e.lastSeen || e.addedAt || 0, alive: !stale,
      });
    }
    list.sort((a, b) => (b.health?.score ?? 0) - (a.health?.score ?? 0));
    const tiers = { S: [], A: [], B: [], C: [] };
    for (const p of list) (tiers[p.tier] || tiers.C).push(p.url || p.name);
    const roles = this.roles();
    return {
      enabled,
      active: this.isActive && enabled,
      mode: this.isPrivate ? 'private' : 'public',
      self: {
        url: show(this.selfUrl),
        name: this.name,
        source: this.selfSource,
        advertised: enabled && !this.isPrivate && !!this.selfUrl,
        health: health.summary({ proxyPool: this.pm.pool.length, proxyIssuers: this.pm.issuerCount(), upstreamOkRate: this.pm.okRate() }),
      },
      peers: list,
      tiers,
      count: list.length,
      aliveCount: list.filter((p) => p.alive).length,
      healthyPeers: this.healthyPeerCount(),
      shareProxies: this.pm.shareCount(PROXY_SHARE_LIMIT),
      delegate: { enabled: !!engineConfig.get('meshDelegate'), roles: { ...roles, metaPeers: roles.metaPeers.map(maskUrl) } },
    };
  }

  /**
   * 役割分担の現状（各ノードがローカルに計算。集中管理なし）:
   *   meta   : S/A ティアのピア（委譲先として使える）
   *   scan   : 自分が担当するプロキシ候補シャード（インスタンス数で候補リストを分割担当）
   *   self   : 自分の現在の主役割
   */
  roles() {
    const selfScore = health.score({ proxyPool: this.pm.pool.length, proxyIssuers: this.pm.issuerCount(), upstreamOkRate: this.pm.okRate() });
    const selfTier = health.tierOf(selfScore);
    const metaPeers = [];
    for (const [, p] of this.peers) {
      if (p.url && p.health && ['S', 'A'].includes(p.tier)) metaPeers.push(p.url);
    }
    const shardCount = Math.max(1, this.peerCount() + 1);
    return {
      selfTier,
      selfScore,
      selfRole: selfTier === 'S' || selfTier === 'A' ? ['meta', 'scan', 'serve'] : ['scan', 'serve'],
      metaPeers,
      scanShard: { index: this.pm.shard, count: Math.max(8, shardCount) },
    };
  }
}

const { proxyManager } = require('./proxies');
const mesh = new Mesh(proxyManager);
module.exports = { mesh, Mesh, MESH_PATH };

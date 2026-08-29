'use strict';
/**
 * Vandal インスタンス協力メッシュ（Instance Cooperation Mesh）。
 *
 * 起動中の Vandal インスタンス同士を WebSocket で結び、サーバー側で
 *  - 各インスタンスの公開 URL
 *  - 生きている（実測済み）プロキシ
 * を自動集計・共有する。IP バンされたり egress の弱いインスタンスでも、
 * 他の健全なインスタンスが検証したプロキシを借りて再生を継続できる。
 *
 * 仕組み:
 *   - 自インスタンスの公開 URL を VANDAL_MESH_URL に設定すると、
 *     「hello」メッセージで { url, name, proxies } を対向へ定期通知する。
 *   - 対向（ピア）から受けた proxies は ProxyManager.adoptProxies() で
 *     プールへ取り込む（認定済みは即採用 / 非認定は軽実測のみ採用）。
 *   - 受信したピア情報はゴシップ的に他ピアへ転送（ホップ数制限 + 重複排除）。
 *
 * 完全にオプトイン。無効時・単独インスタンス時は従来どおりで、
 * オーバーヘッドはゼロ（WebSocket サーバーすら立てない）。
 *
 * 設定（環境変数）:
 *   VANDAL_MESH_URL    自インスタンスの公開 URL（例 https://vandal.example.com）
 *                      ※ 未設定でも他ピアへ接続だけは可能。
 *   VANDAL_MESH_PEERS  接続先ピアのカンマ区切りリスト（wss://…/.vandal-mesh）
 *   VANDAL_MESH_NAME   表示名（既定 "vandal"）
 */
const { WebSocketServer, WebSocket } = require('ws');
const { logbus } = require('./logbus');

const MESH_PATH = '/.vandal-mesh';
const HELLO_INTERVAL = 45 * 1000;
const MAX_PEERS = 64;
const HOP_LIMIT = 3;
const PROXY_SHARE_LIMIT = 40;

function wsUrlFrom(base) {
  if (!base) return null;
  if (/^wss?:\/\//i.test(base)) return base;
  return base.replace(/^https?/i, (m) => (m.toLowerCase() === 'https' ? 'wss' : 'ws')) + MESH_PATH;
}

class Mesh {
  constructor(proxyManager) {
    this.pm = proxyManager;
    this.enabled = false;
    this.started = false;
    this.name = process.env.VANDAL_MESH_NAME || 'vandal';
    this.selfUrl = (process.env.VANDAL_MESH_URL || '').replace(/\/+$/, '');
    this.peerUrls = (process.env.VANDAL_MESH_PEERS || '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    this.wss = null;              // 受信側 WebSocketServer
    this.peers = new Map();       // key -> { ws, url, name, ts, proxies }
    this.outgoing = new Map();    // url -> WebSocket (発信側)
    this._pulse = null;
  }

  get isActive() { return this.enabled && this.started; }

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

  start() {
    if (this.started) return;
    this.started = true;
    if (!this.selfUrl && !this.peerUrls.length) {
      // 明示的な設定が無い = 単独運用（従来どおり。何も起きない）
      logbus.debug('mesh', 'メッシュ未設定のため単独運用（無効）');
      return;
    }
    this.enabled = true;
    this._pulse = setInterval(() => this._pulseNow().catch(() => {}), HELLO_INTERVAL);
    if (this._pulse.unref) this._pulse.unref();
    // ピア URL は https:// でも wss://…/.vandal-mesh へ自動正規化
    for (const u of this.peerUrls) this._connect(wsUrlFrom(u) || u);
    if (this.selfUrl) {
      logbus.info('mesh', 'メッシュ有効', { self: this.selfUrl, peers: this.peerUrls.length });
    }
    // 初回は少し遅らせて実行（プロキシプール暖機後だと共有できるものがある）
    setTimeout(() => this._pulseNow().catch(() => {}), 5000).unref?.();
  }

  _connect(url) {
    if (!url || this.outgoing.has(url)) return;
    let ws;
    try { ws = new WebSocket(url); } catch (_) { return; }
    this.outgoing.set(url, ws);
    ws.on('open', () => logbus.info('mesh', 'ピアへ接続', { url }));
    ws.on('message', (data) => this._onMessage(ws, data, url));
    ws.on('close', () => {
      this.outgoing.delete(url);
      logbus.debug('mesh', 'ピア切断', { url });
      setTimeout(() => { if (this.started) this._connect(url); }, 10000).unref?.();
    });
    ws.on('error', () => { try { ws.close(); } catch (_) { /* noop */ } });
  }

  _onPeer(ws, req) {
    if (this.peers.size >= MAX_PEERS) { try { ws.close(); } catch (_) {} return; }
    const key = (req?.socket?.remoteAddress || '') + ':' + (Math.random() + '').slice(2, 8);
    this.peers.set(key, { ws, url: '', name: '', ts: Date.now(), proxies: [] });
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
    const remoteUrl = typeof msg.url === 'string' ? msg.url.replace(/\/+$/, '') : '';
    const proxies = Array.isArray(msg.proxies) ? msg.proxies.slice(0, PROXY_SHARE_LIMIT) : [];

    if (this.peers.has(key)) {
      const p = this.peers.get(key);
      p.url = remoteUrl; p.name = msg.name || ''; p.ts = Date.now(); p.proxies = proxies;
    }

    // ピアの生きているプロキシを自プールへ取り込む（サーバー側集計の本体）
    if (proxies.length) this.pm.adoptProxies(proxies).catch(() => {});

    // ゴシップ: 他ピアへ転送（ホップ制限）
    if (typeof msg.hops === 'number' && msg.hops < HOP_LIMIT) {
      const fwd = { ...msg, hops: msg.hops + 1 };
      for (const [k, p] of this.peers) {
        if (k === key) continue;
        this._send(p.ws, fwd);
      }
      for (const [u, o] of this.outgoing) {
        if (u === key) continue;
        this._send(o, fwd);
      }
    }
  }

  _send(ws, obj) {
    try { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); } catch (_) { /* noop */ }
  }

  async _pulseNow() {
    const hello = this._hello();
    for (const [k, p] of this.peers) this._send(p.ws, hello);
    for (const [u, o] of this.outgoing) this._send(o, hello);
  }

  _hello() {
    return {
      type: 'hello',
      url: this.selfUrl,
      name: this.name,
      hops: 0,
      ts: Date.now(),
      proxies: this.pm.exportProxies(PROXY_SHARE_LIMIT),
    };
  }

  /** /api/mesh/state 用の公開情報（URL 集計状況）。 */
  state() {
    const peers = [];
    for (const [k, p] of this.peers) if (p.url) peers.push({ url: p.url, name: p.name, proxies: p.proxies.length });
    for (const [u] of this.outgoing) if (!peers.some((x) => x.url === u)) peers.push({ url: u, name: '', proxies: 0 });
    return {
      enabled: this.enabled,
      active: this.isActive,
      self: this.selfUrl || null,
      name: this.name,
      peerUrls: this.peerUrls,
      peers,
      count: peers.length,
    };
  }
}

const { proxyManager } = require('./proxies');
const mesh = new Mesh(proxyManager);
module.exports = { mesh, Mesh };

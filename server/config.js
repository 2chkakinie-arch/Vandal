'use strict';/**
 * EngineConfig — 実行時に変更可能なエンジン設定（設定ページから操作）。
 *
 * 変更可能な項目:
 *   proxyMode : 'auto' | 'proxy' | 'direct'
 *               auto   = プロキシ優先 + direct 保険（従来既定）
 *               proxy  = プロキシのみ（発行・中継とも direct を極力使わない）
 *               direct = プロキシ不使用（サーバー自 egress のみ）
 *   poolSize  : プロキシプールの維持数（8〜120）
 *   certify   : L2/L3 認定（googlevideo トンネル / 発行可否の実測）を回すか
 *   warmBytes : ホットキャッシュの先頭プリフェッチ量（KB, 64〜4096）
 *   commentsPrefetch : watch 応答後にコメントを先行取得するか
 *   logLevel  : 'trace'|'debug'|'info'|'warn'|'error'
 *
 * data/config.json へ永続化（Vercel では /tmp — セッション内のみ）。
 * 変更は logbus 経由で SSE 配信され、設定ページに即時反映される。
 *
 * Vandal Project — independent open project.
 */
const fs = require('node:fs');
const path = require('node:path');
const { logbus } = require('./logbus');

const DATA_DIR = process.env.VERCEL ? '/tmp/vandal-data' : path.join(__dirname, '..', 'data');
const FILE = path.join(DATA_DIR, 'config.json');

const DEFAULTS = {
  proxyMode: 'auto',        // auto | proxy | direct
  poolSize: 30,             // プロキシプール維持数
  certify: true,            // L2(gv) / L3(issuer) 認定を回す
  warmBytes: 768,           // KB — ホットキャッシュ先頭プリフェッチ量
  commentsPrefetch: true,   // watch 応答後のコメント先行取得
  homeKeepWarm: true,       // 誰かが見ている間ホームキャッシュを暖め続ける
  logLevel: 'trace',        // SSE/ログの最小レベル
  // ---- インスタンス協力メッシュ（デフォルト ON。設定不要で自動参加） ----
  meshEnabled: true,        // メッシュ参加（false で完全単独運用へ戻す）
  meshPrivate: false,       // true = 自 URL を外へ共有しない（匿名参加。内部の共同作業は継続）
  meshDelegate: true,       // 上位ティアの健全インスタンスへメタ取得を委譲 / 被委譲（分業）
};

// 環境変数による既定上書き（settings.json の値より先に適用される初期値）
if (process.env.VANDAL_MESH === '0' || process.env.VANDAL_MESH === 'false') DEFAULTS.meshEnabled = false;
if (process.env.VANDAL_MESH_PRIVATE === '1' || process.env.VANDAL_MESH_PRIVATE === 'true') DEFAULTS.meshPrivate = true;
if (process.env.VANDAL_MESH_DELEGATE === '0') DEFAULTS.meshDelegate = false;

class EngineConfig {
  constructor() {
    this.data = { ...DEFAULTS };
    try {
      const raw = JSON.parse(fs.readFileSync(FILE, 'utf8'));
      if (raw && typeof raw === 'object') Object.assign(this.data, this.pickKnown(raw));
    } catch (_) { /* first boot */ }
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) { /* ro fs */ }
    this.apply({ silent: true });
  }

  pickKnown(obj) {
    const out = {};
    for (const k of Object.keys(DEFAULTS)) {
      if (obj[k] === undefined) continue;
      out[k] = this._coerce(k, obj[k]);
    }
    return out;
  }

  _coerce(k, v) {
    switch (k) {
      case 'poolSize': return Math.min(120, Math.max(8, Number(v) || DEFAULTS.poolSize));
      case 'warmBytes': return Math.min(4096, Math.max(64, Number(v) || DEFAULTS.warmBytes));
      case 'proxyMode': return ['auto', 'proxy', 'direct'].includes(v) ? v : 'auto';
      case 'logLevel': return ['trace', 'debug', 'info', 'warn', 'error'].includes(v) ? v : 'trace';
      case 'meshPrivate': case 'meshEnabled': case 'meshDelegate': return !!v;
      default: return typeof DEFAULTS[k] === 'boolean' ? !!v : v;
    }
  }

  get(key) { return key ? this.data[key] : { ...this.data }; }

  set(patch = {}) {
    const clean = this.pickKnown(patch);
    const changed = {};
    for (const [k, v] of Object.entries(clean)) {
      if (this.data[k] !== v) changed[k] = { from: this.data[k], to: v };
    }
    if (!Object.keys(changed).length) return this.get();
    Object.assign(this.data, clean);
    this._save();
    this.apply();
    logbus.info('engine', '設定を変更しました', { changed });
    return this.get();
  }

  /** 設定を実行系へ反映（起動時・変更時） */
  apply({ silent = false } = {}) {
    logbus.setMinLevel(this.data.logLevel);
    if (!silent) logbus.info('engine', 'エンジン設定を適用しました', this.data);
    else logbus.info('engine', 'エンジン設定をロードしました', this.data);
    return this.data;
  }

  _save() {
    try { fs.writeFileSync(FILE, JSON.stringify({ savedAt: Date.now(), ...this.data }, null, 2)); } catch (_) { /* ro fs */ }
  }

  reset() {
    this.data = { ...DEFAULTS };
    this._save();
    this.apply();
    logbus.info('engine', '設定を既定値に戻しました', this.data);
    return this.get();
  }
}

const engineConfig = new EngineConfig();
module.exports = { engineConfig, EngineConfig, DEFAULTS };

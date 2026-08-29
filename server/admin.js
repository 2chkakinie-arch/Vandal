'use strict';/**
 * AdminGate — インスタンス設定の所有権をデプロイ者へ戻す。
 *
 * なぜ必要か:
 *   旧実装は POST /api/settings が無認証で、**誰でも**（通りすがりの訪問者でも）
 *   meshPrivate / meshEnabled / proxyMode / poolSize を書き換えられ、
 *   data/config.json へ永続化されてしまった。「ユーザーがインスタンスを
 *   プライベートにできる」のはおかしい — インスタンスの運用方針は
 *   デプロイ者のものだから。訪問者には読み取り専用の状態だけを見せる。
 *
 * 3 つの管理路（優先順）:
 *   1. VANDAL_ADMIN_TOKEN 環境変数（明示運用）— リクエストの
 *      X-Vandal-Admin ヘッダが一致すれば管理扱い。
 *   2. ループバック（ssh / ローカル起動）— req.socket.remoteAddress で判定。
 *      ※ trust proxy 環境下の req.ip は X-Forwarded-For で偽装できるので
 *        ソケットの実アドレスだけを信頼する。
 *   3. 初回クラーム（ゼロ設定デプロイ）— デプロイ直後に設定ページで
 *      「管理権を取得」ボタンを押した最初のクライアントがトークンを得て
 *      localStorage に保存。2 台目以降は既存トークンの入力が必須。
 *      トークン自体は data/admin.json に保存され API からは二度と返らない。
 *
 * Vandal Project — independent open project.
 */
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const DATA_DIR = process.env.VERCEL ? '/tmp/vandal-data' : path.join(__dirname, '..', 'data');
const FILE = path.join(DATA_DIR, 'admin.json');

const envToken = () => String(process.env.VANDAL_ADMIN_TOKEN || '').trim() || null;

function readStored() {
  try {
    const j = JSON.parse(fs.readFileSync(FILE, 'utf8'));
    return j && typeof j === 'object' && typeof j.token === 'string' ? j : null;
  } catch (_) { return null; }
}

function writeStored(token) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(FILE, JSON.stringify({ createdAt: Date.now(), token }, null, 2));
    return true;
  } catch (_) { return false; }
}

/** ソケットの実リモートアドレスがループバックか（プロキシ経由の偽装不可） */
function isLoopback(req) {
  const ra = String(req.socket?.remoteAddress || '');
  return ra === '127.0.0.1' || ra === '::1' || ra === '::ffff:127.0.0.1';
}

function tokenOf() { return envToken() || readStored()?.token || null; }

/** このリクエストは管理者か（ループバック or 正しいトークン） */
function isAdmin(req) {
  if (isLoopback(req)) return true;
  const t = tokenOf();
  const sent = String(req.headers['x-vandal-admin'] || '');
  if (!t || !sent || sent.length !== t.length) return false;
  return crypto.timingSafeEqual(Buffer.from(sent), Buffer.from(t)); // 定時間比較（タイミング漏洩防止）
}

/** 設定ページ用の状態（トークンそのものは絶対に返さない） */
function status() {
  const claimed = !!envToken() || !!readStored();
  return { claimed, viaEnv: !!envToken(), loopbackOnly: !envToken() && !readStored() };
}

/**
 * 初回クラーム: まだ誰も管理していない場合のみ新しいトークンを発行して返す。
 * 既に取得済みなら token は返さない（2 人目に教えない）。
 * { ok:true, token } | { ok:false, reason:'claimed'|'env' }
 */
function claim() {
  if (envToken()) return { ok: false, reason: 'env' };
  if (readStored()) return { ok: false, reason: 'claimed' };
  const token = 'va_' + crypto.randomBytes(24).toString('base64url');
  if (!writeStored(token)) return { ok: false, reason: 'readonly-fs' };
  return { ok: true, token };
}

/** トークン照合（管理画面でのロック解除・保存ごとに使う） */
function verify(token) {
  const t = tokenOf();
  if (!t || typeof token !== 'string' || token.length !== t.length) return false;
  return crypto.timingSafeEqual(Buffer.from(token), Buffer.from(t));
}

module.exports = { isAdmin, isLoopback, status, claim, verify };

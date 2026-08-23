#!/usr/bin/env node
/**
 * launch.mjs — Persimmon 統合ランチャー（Node バックエンド + Go エッジ）
 *
 * 構成（Go 基盤の高速化レイヤー）:
 *
 *      internet ─▶ persimmon-edge (Go)        ← $PORT を listen（既定 3000）
 *                     │ 静的アセットをメモリから即配信（gzip 事前圧縮済み）
 *                     │ /api/stream ピン済み → googlevideo 直中継（Node ホップ無し）
 *                     └─▶ node index.js       ← 127.0.0.1:$INTERNAL_PORT（既定 3101）
 *                          │ InnerTube パース / cipher / プロキシプール
 *                          └─▶ Go fetch-core  ← 127.0.0.1:$CORE_PORT（既定 3102）
 *                               並列ヘッジ・Range プローブ・ピン登録
 *                               （コア未起動時は Node 側ヘッジに自動フォールバック）
 *
 * 絶対に落ちない / 遅くならないための安全策:
 *   1. Go バイナリが無い・ビルド失敗 → 自動で「従来どおりの Node 単体」にフォールバック
 *   2. エッジが連続して落ちた（3回）→ バックエンドを公開ポートで上げ直し Node 単体へ移行
 *   3. バックエンドが落ちた → プロセスごと終了（プラットフォームの再起動ポリシーに委ねる。
 *      これは従来の npm start 時のクラッシュ挙動と同一）
 *   4. PERSIMMON_EDGE=0 で初めから Node 単体モードにできる
 *
 * 環境変数:
 *   PORT               公開ポート（既定 3000）
 *   INTERNAL_PORT      Node バックエンドのローカルポート（既定 3101）
 *   PERSIMMON_EDGE_BIN エッジバイナリのパス上書き
 *   PERSIMMON_EDGE=0   エッジを無効化（従来構成で起動）
 */
import { spawn, spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

const PUBLIC_PORT = Number(process.env.PORT) || 3000;
const INTERNAL_PORT = Number(process.env.INTERNAL_PORT) || 3101;
let CORE_PORT = Number(process.env.CORE_PORT) || 3102;
if (CORE_PORT === PUBLIC_PORT || CORE_PORT === INTERNAL_PORT) {
  CORE_PORT = INTERNAL_PORT + 1;
  if (CORE_PORT === PUBLIC_PORT) CORE_PORT = INTERNAL_PORT + 2;
}
const CORE_TOKEN = process.env.CORE_TOKEN || crypto.randomBytes(16).toString('hex');
const EDGE_DISABLED = process.env.PERSIMMON_EDGE === '0';
const NODE_BIN = process.execPath;

const log = (...a) => console.log('[launch]', ...a);
const warn = (...a) => console.warn('[launch]', ...a);

/** 従来どおり Node 単体で公開ポートを listen する（npm start と同一構成） */
function startBackend(port, host, label, extraEnv = {}) {
  const child = spawn(NODE_BIN, [path.join(ROOT, 'index.js')], {
    stdio: 'inherit',
    env: { ...process.env, PORT: String(port), HOST: host, PERSIMMON_MODE: label, ...extraEnv },
  });
  child.on('error', (e) => warn(`バックエンド起動失敗 (${label}):`, e.message));
  return child;
}

function findEdgeBinary() {
  const candidates = [];
  if (process.env.PERSIMMON_EDGE_BIN) candidates.push(process.env.PERSIMMON_EDGE_BIN);
  const name = process.platform === 'win32' ? 'persimmon-edge.exe' : 'persimmon-edge';
  candidates.push(path.join(ROOT, 'goedge', 'bin', name));
  for (const p of candidates) {
    try {
      fs.accessSync(p, fs.constants.X_OK);
      return p;
    } catch { /* not here */ }
  }
  return null;
}

/** Go ツールチェーンがあればビルドを試す（標準ライブラリのみなのでオフラインでも可） */
function tryBuildEdge() {
  const go = spawnSync('go', ['version'], { encoding: 'utf8' });
  if (go.error || go.status !== 0) return null;
  const name = process.platform === 'win32' ? 'persimmon-edge.exe' : 'persimmon-edge';
  const out = path.join(ROOT, 'goedge', 'bin', name);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  log('Go 検出 — エッジをビルドします（初回のみ数秒）…');
  const r = spawnSync('go', ['build', '-trimpath', '-ldflags', '-s -w', '-o', out, '.'], {
    cwd: path.join(ROOT, 'goedge'),
    encoding: 'utf8',
    timeout: 180000,
    env: { ...process.env, CGO_ENABLED: '0' },
  });
  if (r.error || r.status !== 0) {
    warn('エッジのビルド失敗（Node 単体で続行します）:', (r.stderr || r.error?.message || '').toString().slice(0, 400));
    return null;
  }
  return out;
}

/* ------------------------------------------------------------ legacy mode */

if (EDGE_DISABLED) {
  log('PERSIMMON_EDGE=0 — Node 単体モードで起動');
  const child = startBackend(PUBLIC_PORT, '0.0.0.0', 'legacy');
  child.on('exit', (code, sig) => process.exit(sig ? 0 : (code ?? 1)));
  forwardSignals([child]);
} else {
  main();
}

function main() {
  let bin = findEdgeBinary();
  if (!bin) bin = tryBuildEdge();
  if (!bin) {
    warn('Go エッジバイナリが見つからない/ビルド不可 → 従来の Node 単体構成で起動します（機能は完全互換）');
    const child = startBackend(PUBLIC_PORT, '0.0.0.0', 'legacy');
    child.on('exit', (code, sig) => process.exit(sig ? 0 : (code ?? 1)));
    forwardSignals([child]);
    return;
  }

  log(`公開ポート :${PUBLIC_PORT} → Go エッジ / バックエンド 127.0.0.1:${INTERNAL_PORT} / fetch-core 127.0.0.1:${CORE_PORT}`);

  let backend = null;
  let edge = null;
  let edgeRestarts = 0;
  let shuttingDown = false;
  let mode = 'edge';

  function killChild(child, sig = 'SIGTERM') {
    try { child?.kill(sig); } catch { /* already dead */ }
  }

  function startInternalBackend() {
    backend = startBackend(INTERNAL_PORT, '127.0.0.1', 'supervised', {
      CORE_ORIGIN: `http://127.0.0.1:${CORE_PORT}`,
      CORE_TOKEN,
    });
    backend.on('exit', (code, sig) => {
      backend = null;
      if (shuttingDown || mode !== 'edge') return;
      // 既存の npm start と同じ「落ちたら終了」— プラットフォームがコンテナごと再起動する
      warn(`バックエンドが終了しました (code=${code} sig=${sig}) — プロセスを終了します（プラットフォームの再起動に委譲）`);
      stopAll(1);
    });
  }

  function startEdge() {
    edge = spawn(bin, [], {
      stdio: 'inherit',
      env: {
        ...process.env,
        PORT: String(PUBLIC_PORT),
        EDGE_ORIGIN: process.env.EDGE_ORIGIN || `http://127.0.0.1:${INTERNAL_PORT}`,
      },
    });
    edge.on('error', (e) => warn('エッジ起動失敗:', e.message));
    edge.on('exit', (code, sig) => {
      edge = null;
      if (shuttingDown || mode !== 'edge') return;
      edgeRestarts += 1;
      warn(`エッジが終了 (code=${code} sig=${sig}) — ${edgeRestarts}/3 回目の再起動判定`);
      if (edgeRestarts <= 3) {
        const delay = Math.min(1000 * 2 ** (edgeRestarts - 1), 4000);
        setTimeout(() => { if (!shuttingDown && mode === 'edge') startEdge(); }, delay);
      } else {
        // 絶対にサービスを止めない: バックエンドを公開ポートへ付け替えて Node 単体へ移行
        warn('エッジが安定しないため、従来の Node 単体構成へ自動切替します（機能は完全互換・速度は従来と同等）');
        mode = 'legacy';
        if (backend) {
          const b = backend;
          b.once('exit', () => startLegacyBackend());
          killChild(b, 'SIGTERM');
          setTimeout(() => { killChild(b, 'SIGKILL'); startLegacyBackend(); }, 4000).unref();
        } else {
          startLegacyBackend();
        }
      }
    });
  }

  function startLegacyBackend() {
    if (shuttingDown || mode !== 'legacy') return;
    backend = startBackend(PUBLIC_PORT, '0.0.0.0', 'legacy-fallback');
    backend.on('exit', (code, sig) => { if (!shuttingDown) process.exit(sig ? 0 : (code ?? 1)); });
  }

  function stopAll(code) {
    if (shuttingDown) return;
    shuttingDown = true;
    killChild(edge, 'SIGTERM');
    killChild(backend, 'SIGTERM');
    setTimeout(() => {
      killChild(edge, 'SIGKILL');
      killChild(backend, 'SIGKILL');
      process.exit(code);
    }, 6000).unref();
    // 両方死んだら即終了
    const iv = setInterval(() => {
      if (!edge && !backend) { clearInterval(iv); process.exit(code); }
    }, 100);
    iv.unref?.();
  }

  const onSignal = (sig) => {
    log(`${sig} 受信 — 正常終了します`);
    stopAll(0);
  };
  process.on('SIGTERM', () => onSignal('SIGTERM'));
  process.on('SIGINT', () => onSignal('SIGINT'));

  startInternalBackend();
  startEdge();
}

/** シグナルを子へ横流し（legacy / 早期フォールバック経路用） */
function forwardSignals(children) {
  const killAll = (sig) => {
    for (const c of children) { try { c.kill(sig); } catch { /* noop */ } }
  };
  for (const sig of ['SIGTERM', 'SIGINT']) {
    process.on(sig, () => {
      killAll(sig);
      setTimeout(() => process.exit(0), 5000).unref();
    });
  }
}

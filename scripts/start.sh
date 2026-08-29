#!/bin/sh
# Vandal 共通起動スクリプト（Replit / CodeSandbox / 任意 VM 用）
# Go ツールチェーンがあればエッジを先にビルドし、なければそのまま
# launch.mjs へ（launch.mjs 側もフォールバック＆ビルド再試行を持つ = 二重の安全弁）
set -u
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)" || exit 1

BIN="goedge/bin/vandal-edge"
if [ ! -x "$BIN" ] && command -v go >/dev/null 2>&1; then
  echo "[start] Go 検出 — エッジをビルドします"
  mkdir -p goedge/bin
  (cd goedge && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "bin/vandal-edge" .) \
    || echo "[start] エッジのビルドに失敗 — Node 単体で起動します（機能は完全互換）"
fi

exec node scripts/launch.mjs

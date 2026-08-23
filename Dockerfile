# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Persimmon 汎用コンテナ（Railway / Render(docker) / Fly.io / Koyeb / VPS 等）
#   1段目: Go エッジ（静的アセットのメモリ配信＋リバースプロキシ）をビルド
#   2段目: Node 依存の本番インストール
#   3段目: 実行イメージ（node:22-alpine + エッジバイナリ）
# 起動: node scripts/launch.mjs（Go エッジ + Node バックエンドの統合ランチャー）
# ---------------------------------------------------------------------------

FROM golang:1.23-alpine AS edgebuild
WORKDIR /src
COPY goedge/ .
# 標準ライブラリのみ使用 → 外部モジュール取得なしでオフラインでもビルド可能
RUN CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags "-s -w" -o /out/persimmon-edge .

FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

FROM node:22-alpine AS run
ENV NODE_ENV=production
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY --from=edgebuild /out/persimmon-edge ./goedge/bin/persimmon-edge

# アプリ本体
COPY package.json ./
COPY index.js ./
COPY server ./server
COPY public ./public
COPY scripts ./scripts
# コールドスタート高速化用のランタイムスナップショット（リポジトリに同梱済みのもの）
COPY data ./data

# data/config.json などのランタイム書き込み用
RUN mkdir -p /app/data && chown -R node:node /app
USER node

EXPOSE 3000
ENV PORT=3000 \
    INTERNAL_PORT=3101 \
    CORE_PORT=3102

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/__edge/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "scripts/launch.mjs"]

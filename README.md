# Vandal (llytpr-wl.v01nh) — Persimmon

_ultra-fast YouTube frontend. InnerTube + issuer-certified proxy direct-play engine + ♦Ask AI._

Made by Kakinie with llytpr-wl.v01nh TEAM. V1

---

## V9 — Go エッジ基盤 + あらゆるプラットフォームへ展開

**Go レイヤーを配信の基盤に追加して、初期表示をさらに高速化。** Node バックエンド（実績ある InnerTube エンジン）は一切そのまま — Go エッジがその前に立って静的配信を肩代わりします。

```
internet ─▶ persimmon-edge (Go)      ← $PORT（静的シェルをメモリから gzip 事前圧縮で即配信）
               └─▶ node index.js     ← 127.0.0.1:$INTERNAL_PORT（InnerTube / プロキシ / SSE）
```

### ⚡ Go 基盤による高速化ポイント

| 領域 | 内容 |
|---|---|
| **静的シェル配信** | `/` `/index.html` `/app.js` `/styles.css` `/logo.png` `/vendor/hls.min.js` を起動時に Node から 1 回だけ取り込み、**生バイト + gzip 事前圧縮バイトをメモリ保持**。リクエストごとの fs 走査・gzip 圧縮 CPU をゼロ化 |
| **実測（ローカル 200 連打）** | `/app.js` (gzip) avg 2.75ms → **0.29ms（約 9.5 倍速）**、p95 3.14ms → **0.40ms**。`/` は 1.30ms → **0.20ms**。フリープランの弱い CPU でも gzip 圧縮と静的配信が API 処理を圧迫しない |
| **ETag / 304** | `If-None-Match` / `If-Modified-Since` に対応（バンドルにも ETag が付き再訪問が更に軽い） |
| **透過プロキシ** | `/api/*`・動画リレー・SSE は `FlushInterval=-1` の生ストリーミング転送。Range・POST・エラーも含め **Node 応答を 1 バイトも変更しない** |
| **自己修復** | アセットは 5 分ごとに再取り込み（失敗時は古いものを保持）、origin は 5 秒ごとに生存監視 |

### 🛡️ 絶対に遅くならない・落ちないための安全設計

1. **挙動同一性**: エッジ高速パスは「GET/HEAD・Range 無し・アセット取得済み」のときだけ。それ以外は全て従来どおり Node が応答（`scripts/smoke-edge.js` で 22 項目の完全パリティを検証）
2. **自動フォールバック**: Go バイナリが無い/ビルド失敗/連続クラッシュ → ランチャーが **従来どおりの Node 単体構成**へ自動切替（機能完全互換）
3. **`npm start` は不変**: 従来コマンドのままなら Node 単体の今までどおりの挙動

### 🚀 デプロイ対応プラットフォーム（V9）

| プラットフォーム | 構成 | 設定ファイル |
|---|---|---|
| **Render** | Docker（Go エッジ + Node） | `render.yaml` + `Dockerfile` |
| **Railway** | Docker（Go エッジ + Node） | `railway.toml` + `Dockerfile` |
| **Replit** | nix で Go+Node を用意、エッジ同梱起動 | `.replit` + `replit.nix` |
| **CodeSandbox** | コンテナ VM → Go エッジ構成（Go が無ければ自動で純 Node） | `.codesandbox/tasks.json` + `.devcontainer/devcontainer.json` |
| **Vercel** | 従来どおり Node サーバーレス（変更なし・壊れません） | `vercel.json` |
| **Cloudflare** | エッジキャッシュ Worker（オリジンは Render/Railway 等の本サーバー） | `wrangler.toml` + `cloudflare/worker.js` |
| **その他 Docker が動く所** | Fly.io / Koyeb / VPS 等 | `Dockerfile` |

手順の詳細は下の「デプロイ」セクションを参照。

---

## V7 — 高速化 + 設定・診断 + 新 UI

### ⚡ 高速化（今回の主な変更）

| 領域 | 内容 |
|---|---|
| **初回待ち** | サーバー起動と同時に **プロキシプール / ホーム / visitorData を並行暖機**。最初のリクエストが来る頃には初期化が終わっている |
| **初回待ち** | ホームの内容を `data/runtime-cache.json` にスナップショット保存し、コールドブート直後は 30 分前までの内容で即描画（stale-while-revalidate） |
| **初回待ち** | クライアントもホームを localStorage スナップショット + sessionStorage キャッシュで即描画、裏で最新化 |
| **player 発行** | 直列総当たり → **並列ヘッジ（プロキシレース）** に変更。wave1 は「定番 + direct + issuer プロキシ」を同時発射し、最初に OK が戻った瞬間に確定。最悪 N×timeout が max(1×timeout) に |
| **コメント** | ①watch 応答に commentsToken を同梱 → コメント取得の**トークン発見往復を廃止（約 1/2 のレイテンシ）** ②watch 表示直後にサーバーがコメントを先行取得 ③single-flight で同時リクエストを 1 往復に束ねる |
| **並列束ね** | `TTLCache.wrap` が single-flight 化。同一 API の同時呼び出し（ホバー先読み+視読+warm など）は必ず 1 本の上流往復で全員に分配 |
| **圧縮** | gzip level 1（JSON は圧縮率 ~5% 落ちるだけで CPU 1/3 以下 → 初バイト高速化）。SSE・動画中継は圧縮除外 |
| **プロキシ** | スキャン並列幅 36→48、L2/L3 認定並列幅 2→4・対象 20→24 で **issuer 認定が約 2 倍速く揃う** |

### 🎛️ 設定・診断ページ（`/#/settings`）

- **エンジン設定** — プロキシ運用モード（自動 / プロキシのみ / 直結のみ）、プール維持数、先読み量、コメント先行取得・ホーム常時暖機・認定の ON/OFF、ログレベル
- **プロキシプール** — 生きているプロキシの一覧・遅延・等級（L1/L2/L3）をライブ表示、強制更新・手動認定
- **プロキシレース** — L1 トンネル → L2 googlevideo → L3 発行実測の段階選抜を全員同時に実走して勝者を決定
- **ストリーム取得テスト** — URL 発行 → 直結/ピン経由の初バイト実測 → ホットキャッシュ載せまでの分解タイミング
- **メタ情報取得テスト** — watchNext / コメント（トークン再利用 vs 従来 2 往復の差分表示）/ 検索の実測
- **ライブログ** — サーバー内の超細かいログ（proxy / player / stream / comments / meta / http / engine …）を **SSE でリアルタイム**閲覧。レベル・チャンネルフィルタ、一時停止、クリア

### 🧭 UI (V7)

- ナビゲーションは**既定で閉じたドロワー**（☰ で開閉、状態は記憶）。モバイルはボトムナビ
- テーマは既定で OS 設定に追従（ダーク/ライト）
- `/` または Ctrl+K で検索フォーカス、Esc でドロワーを閉じる
- カード浮遊アニメーション、ブランドグラデーションのアクセント、スケルトン微光

### 🍅 ロゴの差し替え方

**ファイルを上書きするだけ**で、ヘッダー・ファビコン・設定ページのロゴがすべて変わります:

| ファイル | 用途 |
|---|---|
| `public/logo.png` | ヘッダーのロックアップ + ファビコン + 設定ページのマーク |

透明 PNG 推奨（任意サイズに対応。透過余白は表示時に自動で詰められます）。パスを変える場合は `public/index.html` 内の `<img src="/logo.png">` と `<link rel="icon">` を書き換えてください。

### 🗂️ ソース分散（V8）

サーバー・クライアントのソースは読み取りにくくするため複数ファイルへ意図的に分散しています:

| 場所 | 内容 |
|---|---|
| `server/parse.js` / `server/transport.js` / `server/caches.js` / `server/errors.js` | InnerTube エンジンの分割モジュール（`server/innertube.js` が束ねる） |
| `server/routes/*.js` | ルーティングの分割モジュール（`server/routes.js` がファサード） |
| `public/_src/js/*.part` + `js.manifest.json` | フロント SPA の部品群（`server/client-bundle.js` が連結して `/app.js` として配信） |
| `public/_src/css/*.part` + `css.manifest.json` | スタイルの部品群（同じく `/styles.css` として配信） |

配信は従来どおり **1 リクエスト**（サーバー起動時に連結済みのバンドルをキャッシュ）。読み込み速度は変わりません。マニフェストが無い場合は部品を辞書順に連結するフォールバックが働きます。

## 実行

```bash
npm install
npm start            # 従来どおり Node 単体で起動（PORT env、既定 3000）
npm run start:edge   # Go エッジ + Node の高速構成で起動（推奨）
npm run test:api     # ローカルスモークテスト（外部ネット不要）
npm run test:edge    # Go エッジ統合テスト（パリティ 22 項目）
```

`npm run start:edge` は `scripts/launch.mjs` を起動します — Go エッジバイナリが
見つからなければ（初回は）Go ツールチェーンで自動ビルドを試み、それも無理なら
**自動で従来の Node 単体構成にフォールバック**します。必ず起動します。

## デプロイ

### Render
このリポジトリを Blueprint としてデプロイするだけ（`render.yaml` が `Dockerfile` を使用）。
ヘルスチェックは `/__edge/healthz`。従来の Node サービス（`env: node` +
`npm start`）のまま運用することも可能です — その場合も一切の変更なしで動きます。

### Railway
GitHub 連携でリポジトリを選ぶだけ。`railway.toml` が Dockerfile ビルドとヘルスチェック
（`/__edge/healthz`）を自動設定します。

### Replit
「Import from GitHub」→ Run。`.replit` / `replit.nix` が Node 22 + Go を用意し、
`scripts/start.sh` がエッジをビルドしてから統合ランチャーを起動します。

### CodeSandbox
「Import repository」するだけ。`.codesandbox/tasks.json` が `npm install` と起動タスクを
登録します。VM に Go がある場合はエッジ構成、無い場合は自動で純 Node 構成になります。

### Vercel
従来どおりそのまま deploy 可能（`vercel.json` は不変更）。サーバーレス環境では
Node 単体で動作します。SSE がバッファされる場合があります — その場合はログビューアが
自動再接続を繰り返すだけで、他機能には影響しません（ポーリング API `/api/diag/logs` も利用可）。

### Cloudflare（エッジキャッシュ層）
オリジン（Render / Railway などで動く本サーバー）の前に Cloudflare Worker を
置くと、SPA シェルが世界中の PoP から即時配信されます:

```bash
$EDITOR wrangler.toml   # [vars] ORIGIN を自分のオリジン URL に
npx wrangler login
npx wrangler deploy
```

詳細は `cloudflare/README.md` を参照。

### Dockerfile（汎用）
`docker build -t persimmon . && docker run -p 3000:3000 persimmon` で
Go エッジ + Node の完全構成が 1 コンテナで動きます — Fly.io / Koyeb / VPS など
Docker が動く場所ならどこでも同じ構成になります。

## 環境変数

| 変数 | 意味 |
|---|---|
| `LLYTPR_NO_PROXY=1` | プロキシ完全不使用（デバッグ用） |
| `LLY_PIPED` | 追加 Piped インスタンス（カンマ区切り） |
| `HOST` | Node のバインドアドレス（既定 `0.0.0.0`。ランチャー配下では `127.0.0.1` が自動指定される） |
| `PORT` | 公開ポート（各プラットフォームが自動注入） |
| `INTERNAL_PORT` | launch.mjs 配下の Node ローカルポート（既定 3101） |
| `PERSIMMON_EDGE=0` | Go エッジを無効化し従来構成で起動 |
| `PERSIMMON_EDGE_BIN` | エッジバイナリのパス上書き |
| `EDGE_ORIGIN` | エッジ→バックエンドの向き先（既定 `http://127.0.0.1:3101`） |
| `EDGE_ASSET_REFRESH_MS` | 静的アセット再取り込み間隔（既定 300000。0 で無効） |
| `EDGE_VERBOSE=1` | エッジの詳細ログ |

エンジン設定は `data/config.json` に永続化され、設定ページから実行時に変更できます。

# Vandal

<p align="center">
  <img src="public/logo.svg" alt="Vandal" width="360" />
</p>

_超高速・自己完結型の動画フロントエンド。InnerTube 直結エンジン + 認定プロキシ・ダイレクト再生 + インスタンス協力メッシュ。_

**Vandal Project — 完全に独立したオープンプロジェクト。**

---

## 🚀 ワンクリック・デプロイ

| プラットフォーム | デプロイ |
|---|---|
| **Render** | [![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/2chkakinie-arch/Vandal) |
| **Vercel** | [![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https://github.com/2chkakinie-arch/Vandal) |
| **Railway** | [![Deploy on Railway](https://img.shields.io/badge/Deploy%20on%20Railway-0B0D0E?style=for-the-badge&logo=railway&logoColor=white)](https://railway.app/new) |
| **Replit** | [![Run on Replit](https://img.shields.io/badge/Run%20on%20Replit-F26207?style=for-the-badge&logo=replit&logoColor=white)](https://replit.com/github/2chkakinie-arch/Vandal) |
| **Koyeb** | [![Deploy to Koyeb](https://img.shields.io/badge/Deploy%20to%20Koyeb-121212?style=for-the-badge&logo=koyeb&logoColor=white)](https://app.koyeb.com/deploy?type=git&repository=github.com/2chkakinie-arch/Vandal) |
| **Fly.io / VPS** | `docker build -t vandal . && docker run -p 3000:3000 vandal` |

> Render / Railway / Koyeb は Dockerfile をそのまま使い、Vercel は Node サーバーレスで動作します。Replit は `.replit` / `replit.nix` が Go + Node を用意します。

---

## ✨ 特徴

| 領域 | 内容 |
|---|---|
| **⚡ 直結再生** | InnerTube の player 発行を並列ヘッジ（プロキシレース）で行い、最初に `playability=OK` が返った経路を即採用。負けた経路はキャンセル |
| **🛰️ インスタンス協力メッシュ** | **設定不要で自動参加。** デプロイして URL が確定した瞬間にインスタンスリストへ自動追加され、公開 URL・生きているプロキシ・健全性を WebSocket で相互共有 |
| **🩺 /health & インスタンスページ** | 混み具合・CPU・イベントループ・利用できるプロキシの数を `/health`（ダッシュボード）と `/api/health`（JSON）で公開。「インスタンス」ページでティア表（S/A/B/C）をライブ表示 |
| **🧠 自動分業** | 健全インスタンスをティア自動選別し、上位へメタ取得を委譲・プロキシ収集はシャード分担・十分に健全なら並列取得でさらに高速化 |
| **🖼️ スプレーアート・ロゴ** | 大胆でかっこいい SVG ロゴ（マゼンタ→オレンジ→イエローのスプレーグラデーション + スプラッタ） |
| **💬 コメント永久読み込み** | スクロールする限り自動でコメントを追記。失効トークンは静かに終端化（400 ループを構造的に排除） |
| **🔀 その他** | ホーム / 検索 / チャンネル / 再生リスト / ショート専用プレイヤー / ローカル履歴・高評価・保存・登録チャンネル / Ask AI |

## 🌐 インスタンス協力メッシュ（デフォルト ON・設定不要）

デプロイされた Vandal は**何も設定しなくても**自動でメッシュに参加します:

1. **自 URL 自動検出** — Render / Vercel / Railway / Koyeb / Fly / Heroku 等のプラットフォーム環境変数を自動スキャン。無ければ最初の公開リクエストの Host から学習し、`data/mesh-registry.json` に永続化。= **「デプロイして URL ができた時点で」自動で Vandal インスタンスリストに追加**され、プロキシを共有する役割を担います。
2. **ゴシップ型ピア発見** — hello に「知っているピア一覧」を乗せて回すので、1 台でも知り合いがいれば網全体に広がります（ピア一覧は永続化され、再起動後も自動再接続）。
3. **健全性の相互監視** — hello に health サマリ（CPU・イベントループ遅延・混み具合・プロキシ数・スコア）を常時同梱し、さらに URL を持つピアの `/healthz` を 2 分ごとに実測。
4. **スマート分業** — スコアで S/A/B/C ティアを自動選別し、
   - 動画情報の発行基盤が弱いインスタンスは **S/A ティアへメタ取得を並行委譲**（自前が成功すれば 1ms も待たない）
   - **プロキシ収集はシャード分担**（各インスタンスが候補リストの別担当区間を実測 → 見つけた生きているプロキシは全員で共有 = 収集速度がインスタンス数倍）
   - 健全インスタンスが十分にいるインスタンスほど当たり筋が増え、発行・コメント取得が並列化
5. **プライバシー** — 訪問者には URL を省略表記でしか見せません（内部の共同作業には完全な URL を使用）。設定ページ or `data/config.json` の `"meshPrivate": true`（env: `VANDAL_MESH_PRIVATE=1`）で**プライベートインスタンス**（URL を一切共有しない匿名参加）に切替可能。内部の共同作業にはデプロイされた生きている全インスタンスが適用されます。

```bash
# 以下はすべて**任意**。設定しなくてもメッシュは自動で動きます。
VANDAL_MESH_URL=https://vandal.example.com   # 自 URL を明示固定したいとき
VANDAL_MESH_PEERS=https://peer1.example.com  # 最初の接続先（ゴシップで自動拡大）
VANDAL_MESH=0                                # 完全単独運用に戻す
```

### インスタンス設定はデプロイ者のもの（管理者ゲート）

エンジン設定（`POST /api/settings`）の書き込みは**管理者のみ**です。通りすがりの訪問者が
プライベート化やメッシュ脱退、`proxyMode: direct` などでインスタンスの挙動を書き換えられないようにするためです。

- `VANDAL_ADMIN_TOKEN=<シークレット>` を設定 → そのトークンを `X-Vandal-Admin` ヘッダに乗せたクライアントだけが管理者
- 未設定の場合 → デプロイ後に設定ページで**最初に「管理権を取得」したクライアント**が管理者（トークンは 1 回だけ発行され `data/admin.json` に保存。以降はローカル保持のトークンでロック解除）
- サーバーをローカル（`127.0.0.1`）で開いている場合は自動的に管理者扱い（ソケット実アドレスで判定 — `X-Forwarded-For` 偽装不可）

- 受信側の WebSocket サーバーは `/.vandal-mesh` に生えます。
- 共有されるのは **URL と実測メタ（遅延・issuer/gv 認定）のみ**。資格情報は一切送信されません。
- ピアから受けたプロキシは、認定済みは即採用・非認定は軽実測に通ったものだけ採用（毒入れ対策）。
- 集計状況は `GET /api/mesh/state`、参加者はアプリ内の **インスタンス** ページ（ティア表）で確認できます。

## 🩺 /health — 混み具合・CPU・プロキシの見える化

| エンドポイント | 内容 |
|---|---|
| `GET /health` | 人間用ダッシュボード（15 秒自動更新）。CPU / メモリ / イベントループ遅延 / 同時処理数 / 使えるプロキシの数 / メッシュのティア表 |
| `GET /healthz` | 極軽の生存確認（メッシュの到達性実測に使用） |
| `GET /api/health` | 詳細 JSON（score / tier / relay / caches / core 込み） |

メッシュはこの情報を常時交換して「健全インスタンスの数」を全ノードが把握し、分業と並列化に使います。

## 🚦 クイックスタート

```bash
npm install
npm start            # Node 単体で起動（PORT env、既定 3000）
npm run start:edge   # Go エッジ + Node の高速構成で起動（推奨）
```

`npm run start:edge` は Go エッジバイナリが見つからなければ自動ビルドを試み、それも無理なら**自動で Node 単体構成にフォールバック**します。必ず起動します。

### テスト

```bash
npm run test:api     # ローカルスモークテスト（外部ネット不要）
npm run test:core    # 並列ヘッジ / プローブ / gocore フォールバック
npm run test:edge    # Go エッジ統合テスト（パリティ 22 項目）
```

## ⚙️ 環境変数

| 変数 | 意味 |
|---|---|
| `VANDAL_MESH_URL` | （任意）自インスタンスの公開 URL を明示固定。未設定なら自動検出 |
| `VANDAL_MESH_PEERS` | （任意）最初の接続先ピア（カンマ区切り）。ゴシップで自動拡大 |
| `VANDAL_MESH=0` | メッシュ無効化（完全単独運用） |
| `VANDAL_MESH_PRIVATE=1` | プライベートインスタンス（URL 非共有の匿名参加） |
| `VANDAL_MESH_DELEGATE=0` | 分業（メタ取得の委譲/被委譲）を無効化 |
| `VANDAL_MESH_NAME` | 表示名（既定 `vandal`） |
| `VANDAL_NO_PROXY=1` | プロキシ完全不使用（デバッグ用） |
| `VANDAL_PIPED` | 追加 Piped インスタンス（カンマ区切り） |
| `HOST` | Node のバインドアドレス（既定 `0.0.0.0`） |
| `PORT` | 公開ポート（各プラットフォームが自動注入） |
| `INTERNAL_PORT` | launch.mjs 配下の Node ローカルポート（既定 3101） |
| `VANDAL_EDGE=0` | Go エッジを無効化し従来構成で起動 |
| `VANDAL_EDGE_BIN` | エッジバイナリのパス上書き |
| `EDGE_ORIGIN` | エッジ→バックエンドの向き先（既定 `http://127.0.0.1:3101`） |

## 🗂️ ソース構成

| 場所 | 内容 |
|---|---|
| `server/innertube.js` + `parse.js` / `transport.js` / `caches.js` / `errors.js` | InnerTube エンジン |
| `server/proxies.js` | 認定プロキシプール（L1 トンネル / L2 googlevideo / L3 issuer） |
| `server/mesh.js` | インスタンス協力メッシュ（自動参加・ゴシップ発見・分業） |
| `server/health.js` | テレメトリ & ティアスコア（/health・メッシュ hello の素） |
| `server/routes/*.js` | ルーティング（`health.js` = /health /healthz /api/health） |
| `public/_src/js/*.part` / `css/*.part` | フロント SPA 部品（`server/client-bundle.js` が連結して配信） |
| `goedge/` | Go エッジアクセラレータ |

---

## ⚖️ ライセンスと利用条件（絶対遵守）

**このプロジェクトをあなたの環境で自由に使用、デプロイ、または他者へ共有することを許可します。改変の禁止（絶対遵守）**

**本プロジェクトのソースコードや設計思想を保持するため、コードの改変は一切禁止とします。元の形を尊重し、配布・公開を行うよう強くお願いします。このプロジェクトを絶対に他プロジェクトのAPIや内部技術として利用しないでください。**

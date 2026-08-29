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
| **🛰️ インスタンス協力メッシュ** | 起動中の Vandal インスタンス同士を WebSocket で結び、**公開 URL と生きているプロキシをサーバー側で自動集計・共有** |
| **🖼️ スプレーアート・ロゴ** | 大胆でかっこいい SVG ロゴ（マゼンタ→オレンジ→イエローのスプレーグラデーション + スプラッタ） |
| **💬 コメント永久読み込み** | スクロールする限り自動でコメントを追記。失敗時も自動リトライ |
| **🔀 その他** | ホーム / 検索 / チャンネル / 再生リスト / ショート専用プレイヤー / ローカル履歴・高評価・保存・登録チャンネル / Ask AI |

## 🌐 インスタンス協力メッシュ

複数の Vandal インスタンスがお互いを認識し、**生きているプロキシ**を共有し合います。IP バンや egress の弱い環境でも、他の健全なインスタンスが検証したプロキシを借りて再生を継続できます。単独のインスタンスとしても、従来どおり（メッシュ無効時はオーバーヘッドゼロ）高速に動作します。

```bash
# 自インスタンスの公開 URL（設定すると他へ共有される）
VANDAL_MESH_URL=https://vandal.example.com

# 接続先ピア（カンマ区切り）
VANDAL_MESH_PEERS=wss://peer1.example.com/.vandal-mesh,wss://peer2.example.com/.vandal-mesh
```

- 受信側の WebSocket サーバーは `/.vandal-mesh` に生えます。
- 共有されるのは **URL と実測メタ（遅延・issuer/gv 認定）のみ**。資格情報は一切送信されません。
- ピアから受けたプロキシは、認定済みは即採用・非認定は軽実測に通ったものだけ採用（毒入れ対策）。
- 集計状況は `GET /api/mesh/state` で確認できます。

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
| `VANDAL_MESH_URL` | 自インスタンスの公開 URL（メッシュ共有用） |
| `VANDAL_MESH_PEERS` | 接続先ピア（カンマ区切り、`wss://…/.vandal-mesh`） |
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
| `server/mesh.js` | インスタンス協力メッシュ（WebSocket） |
| `server/routes/*.js` | ルーティング |
| `public/_src/js/*.part` / `css/*.part` | フロント SPA 部品（`server/client-bundle.js` が連結して配信） |
| `goedge/` | Go エッジアクセラレータ |

---

## ⚖️ ライセンスと利用条件（絶対遵守）

**このプロジェクトをあなたの環境で自由に使用、デプロイ、または他者へ共有することを許可します。改変の禁止（絶対遵守）**

**本プロジェクトのソースコードや設計思想を保持するため、コードの改変は一切禁止とします。元の形を尊重し、配布・公開を行うよう強くお願いします。このプロジェクトを絶対に他プロジェクトのAPIや内部技術として利用しないでください。**

# persimmon-edge（Go エッジアクセラレータ）

Persimmon の配信基盤となる Go 製フロントサーバー。**標準ライブラリのみ**
（外部モジュール依存ゼロ）なので、Go ≥ 1.20 さえあればオフラインでもビルドできます。

## 役割

1. **静的シェルのメモリ配信**: `/` `/index.html` `/app.js` `/styles.css` `/logo.png`
   `/vendor/hls.min.js` を起動時に Node バックエンドから取り込み、生バイトと
   gzip 事前圧縮バイト（BestSpeed = Node の level 1 相当）を保持して即時応答。
   ETag / Last-Modified / 条件付きリクエスト（304）対応。
2. **完全透過リバースプロキシ**: 上記以外のすべて（`/api/*`、動画リレー、SSE、
   POST、Range 要求）は Node へストリーミング転送（バッファリング無し）。
   応答を一切改変しません。
3. **生存監視・自己修復**: `/api/health` を 5 秒間隔で監視、アセットは 5 分ごと
   に再取り込み（失敗時は現行を保持）。

## 安全性の原則

- 高速パスに乗るのは「GET/HEAD かつ Range 無し かつ アセット取得済み」のみ。
- アセット取得に失敗したパスは自動的にプロキシ透過（= 従来の Node 直配信と同一挙動）。
- バイナリが存在しない・起動失敗する環境では、ランチャー（`scripts/launch.mjs`）が
  純 Node 構成へフォールバックするため、**どの環境でも必ず従来どおり動作**します。

## ビルド・テスト

```bash
cd goedge
go build -trimpath -ldflags "-s -w" -o bin/persimmon-edge .   # ビルド
go test ./...                                                 # ユニットテスト
```

## 環境変数

| 変数 | 既定 | 意味 |
|---|---|---|
| `PORT` | 3000 | 公開ポート |
| `EDGE_BIND` | 0.0.0.0 | バインドアドレス |
| `EDGE_ORIGIN` | http://127.0.0.1:3101 | Node バックエンドのアドレス |
| `EDGE_ASSET_REFRESH_MS` | 300000 | アセット再取り込み間隔（0=無効） |
| `EDGE_BOOT_TIMEOUT_MS` | 45000 | 初回取り込みのリトライ総時間 |
| `EDGE_HEALTH_GRACE_MS` | 60000 | 起動直後の healthz 200 猶予 |
| `EDGE_VERBOSE` | - | `1` で詳細ログ |
| `CORE_PORT` | 0（無効） | 動画取得コアのループバックポート。`launch.mjs` は 3102 を渡す |
| `CORE_BIND` | 127.0.0.1 | コアのバインド。公開面には出さない |
| `CORE_TOKEN` | - | 任意の共有秘密（`X-Persimmon-Core`）。ランチャーが自動生成 |
| `EDGE_ALLOW_LOCAL` | - | `1` のときだけコアが localhost へ出られる（ユニットテスト用） |

## エンドポイント

- `GET /__edge/healthz` — `{"ok":true,"mode":"go-edge","assets":5,"assetTotal":5,"originUp":true,"core":{...}}`
  （Docker HEALTHCHECK / Railway / Render のヘルスチェック用）
- ループバック専用（公開されない）:
  - `GET  /health` `POST /v1/hedge` `POST /v1/fetch` `POST /v1/probe` `POST|DELETE|GET /v1/pin`
- その他の `/__edge/*` はオリジンへ透過（未知パスの挙動も従来と一致）

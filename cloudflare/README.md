# Cloudflare（エッジキャッシュ層）

Cloudflare には Node サーバー（InnerTube エンジン/プロキシ中継/SSE）はそのまま
載せられないため、**「オリジン(Render/Railway 等の Node+Go 構成) + Cloudflare
Worker のエッジキャッシュ」**という 2 層構成にします。SPA シェルの静的ファイル
（`/app.js` `/styles.css` `/logo.png` など）は Cloudflare の世界中の PoP から
即時配信され、初回表示が大きく速くなります。

- キャッシュ対象: `GET` かつ静的拡張子のパスで、オリジン応答の
  `Cache-Control: max-age` が正のもののみ（オリジン指定の鮮度をそのまま尊重）
- キャッシュしないもの: `/api/*` 全般・POST・SSE・動画リレー（すべて素通し転送。
  ストリーミングはバッファされずそのまま中継されます）

## デプロイ手順

```bash
# 1. オリジン URL を自分のものに変更（Render / Railway の公開 URL）
$EDITOR wrangler.toml        # [vars] ORIGIN = "https://your-persimmon.example"

# 2. デプロイ（wrangler は npm から入ります）
npm i -D wrangler            # 任意（npx でも可）
npx wrangler login
npx wrangler deploy
```

デプロイ後は `https://persimmon-edge.<あなた>.workers.dev` が公開 URL になります。

## カスタムドメイン

Cloudflare ダッシュボード → Workers → persimmon-edge → Settings → Triggers で
`example.com` 等のカスタムドメインを割り当てられます（Cloudflare 管理の DNS が必要）。

## キャッシュ挙動の確認

`x-persimmon-cf` 応答ヘッダが付きます:

| 値 | 意味 |
|---|---|
| `HIT` | Cloudflare エッジキャッシュから即時応答 |
| `MISS` | オリジンから取得しエッジに保存 |
| `PASS` | 静的パスだがキャッシュ条件外（オリジン指定どおり） |
| `API` | `/api/*` または動的 — 常にオリジンへ素通し |

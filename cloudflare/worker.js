/**
 * Persimmon — Cloudflare エッジ Worker
 *
 * Cloudflare の世界中の PoP から SPA シェル（index.html / app.js / styles.css /
 * 画像など）を**キャッシュ付きで**配信し、初期表示を大幅に速くする。
 * /api/*（動画リレー・SSE・InnerTube）は一切キャッシュせず、オリジン
 * （Render / Railway 等で動く Persimmon サーバー）へストリーミング転送する。
 *
 * 設定: wrangler.toml の [vars] ORIGIN を自分のオリジン URL に変更してデプロイ。
 *       npx wrangler deploy
 */

const STATIC_EXT = /\.(js|css|png|jpe?g|webp|gif|svg|ico|woff2?|ttf|otf|map|webmanifest|txt)$/i;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = (env.ORIGIN || '').replace(/\/+$/, '');
    if (!origin) {
      return new Response(
        'Persimmon edge: ORIGIN が未設定です。wrangler.toml の [vars] ORIGIN を設定してください。',
        { status: 500 },
      );
    }

    const method = request.method.toUpperCase();
    const isApi = url.pathname.startsWith('/api/') || url.pathname === '/__edge/healthz';
    const maybeStatic = STATIC_EXT.test(url.pathname) ||
      url.pathname === '/' || url.pathname === '/index.html';

    // キャッシュするのは「GET の静的パス」のみ。それ以外（POST・/api/*・Range 等は
    // オリジン応答ヘッダが許す場合を除き全て）は素通し。
    const cacheable = method === 'GET' && !isApi && maybeStatic;
    // cache（caches.default）は実 Cloudflare 上では常に存在するが、
    // ローカル standalone 実行等では未設定のことがある → 無ければ素通しで安全に動作する
    const cache = cacheable ? getDefaultCache() : null;
    const cacheKey = cacheable ? new Request(new URL(url.pathname + url.search, url.origin).toString()) : null;

    if (cache) {
      try {
        const hit = await cache.match(cacheKey);
        if (hit) {
          const res = new Response(hit.body, hit);
          res.headers.set('x-persimmon-cf', 'HIT');
          return res;
        }
      } catch { /* キャッシュ不能でも転送は続行 */ }
    }

    let res;
    try {
      res = await fetch(buildOriginRequest(request, url, origin));
    } catch (e) {
      return json({ error: 'origin temporarily unavailable', code: 'CF_EDGE_UPSTREAM' }, 502);
    }

    // オリジンの Cache-Control を尊重してエッジキャッシュに載せる
    // （no-cache の index.html は再検証される＝鮮度はオリジンの指定どおり）
    if (cacheable && res.status === 200) {
      const cc = res.headers.get('cache-control') || '';
      const m = /max-age=(\d+)/i.exec(cc);
      const noStore = /no-store|private/i.test(cc);
      if (m && Number(m[1]) > 0 && !noStore) {
        // ボディは 2 回使わない（put 用と返却用に別々の Response を作る）
        const body = await new Response(res.body, res).arrayBuffer();
        const headers = new Headers(res.headers);
        // エッジでは少しだけ長く保持（ブラウザ向け max-age は維持）
        headers.set('cache-control', cc.replace(/max-age=\d+/i, `max-age=${m[1]}, s-maxage=${Math.max(Number(m[1]), 3600)}`));
        headers.set('x-persimmon-cf', 'MISS');
        if (cache) {
          try {
            // 配信を待たせないようバックグラウンドで保存
            const putPromise = cache.put(cacheKey, new Response(body.slice(0), { status: res.status, headers }));
            ctx.waitUntil(putPromise.catch(() => {}));
          } catch { /* put 失敗も配信には影響させない */ }
        }
        return new Response(body, { status: res.status, headers });
      }
    }

    const passthrough = new Response(res.body, res);
    passthrough.headers.set('x-persimmon-cf', isApi ? 'API' : 'PASS');
    return passthrough;
  },
};

/** caches.default を安全に取得（未設定の実行環境では null） */
function getDefaultCache() {
  try {
    return typeof caches !== 'undefined' ? caches.default : null;
  } catch {
    return null;
  }
}

/** オリジンへの転送リクエストを構築（ボディはストリーミングのまま） */
function buildOriginRequest(request, url, origin) {
  const target = origin + url.pathname + url.search;
  const headers = new Headers(request.headers);
  headers.set('x-forwarded-host', url.host);
  headers.set('x-forwarded-proto', url.protocol.replace(':', ''));
  return new Request(target, {
    method: request.method,
    headers,
    body: request.method === 'GET' || request.method === 'HEAD' ? undefined : request.body,
    // リダイレクトも Worker が追わず、そのままクライアントへ返す（= Worker 無しと同一挙動）
    redirect: 'manual',
  });
}

function json(obj, status) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}

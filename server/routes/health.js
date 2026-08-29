'use strict';
/**
 * Health endpoints — Vandal.
 *
 *   GET /health     人間用ダッシュボード（HTML）。?format=json で JSON。
 *   GET /healthz    機械用の極軽チェック（メッシュの到達性実測に使う。整数 1 応答）。
 *   GET /api/health 詳細 JSON（混み具合 / CPU / メモリ / イベントループ /
 *                   利用できるプロキシの数 / メッシュの状態 / ティア）。
 *
 * インスタンス協力メッシュはこの情報を常時交換して「健全インスタンスの数」を
 * 全ノードが把握し、分業と並列化に使う。詳細は server/health.js 参照。
 *
 * Vandal Project — independent open project.
 */
const express = require('express');
const { health } = require('../health');
const { proxyManager } = require('../proxies');
const { mesh } = require('../mesh');
const { engineConfig } = require('../config');
const { hotChunks } = require('../media');
const { caches } = require('../caches');
const { gocore } = require('../gocore');
const { relayState } = require('./helpers');

const router = express.Router();

function fullHealthJson() {
  const base = health.detail();
  const pmSummary = {
    enabled: proxyManager.enabled,
    pool: proxyManager.pool.length,
    target: proxyManager.poolTarget(),
    issuers: proxyManager.issuerCount(),
    gvTunnel: proxyManager.gvCount(),
    okRate: Math.round(proxyManager.okRate() * 100) / 100,
    certifying: !!proxyManager.certifying,
    refreshing: !!proxyManager.refreshing,
    shard: proxyManager.status().shard,
  };
  const h = health.summary({ proxyPool: pmSummary.pool, proxyIssuers: pmSummary.issuers, upstreamOkRate: proxyManager.okRate() });
  return {
    ...base,
    score: h.score,
    tier: h.tier,
    proxies: pmSummary,
    mesh: {
      active: mesh.isActive,
      mode: mesh.isPrivate ? 'private' : 'public',
      self: mesh.selfUrl,
      peers: mesh.peerCount(),
      healthyPeers: mesh.healthyPeerCount(),
    },
    relay: { ...relayState, hot: hotChunks.status() },
    caches: { api: caches.api.size, streams: caches.streams.size, visitor: caches.visitor.size },
    core: gocore.status(),
    engine: { proxyMode: engineConfig.get('proxyMode') },
  };
}

router.get('/healthz', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({ ok: true, ts: Date.now() });
});

router.get('/api/health', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json(fullHealthJson());
});

router.get('/health', (req, res) => {
  const wantsJson = String(req.query.format || '') === 'json' || !String(req.headers.accept || '').includes('text/html');
  if (wantsJson) { res.setHeader('Cache-Control', 'no-store'); res.json(fullHealthJson()); return; }
  res.setHeader('Cache-Control', 'no-store');
  const d = fullHealthJson();
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const pct = (x) => `${Math.round(x)}%`;
  const tierColor = { S: '#7ce38b', A: '#6ee7ff', B: '#ffd166', C: '#ff7b72' };
  const peers = mesh.state();
  const peerRows = peers.peers.slice(0, 64).map((p) => `
    <tr>
      <td><span class="tier" style="background:${tierColor[p.tier] || '#888'}">${esc(p.tier)}</span></td>
      <td class="mono">${esc(p.anonymous ? '(匿名ピア)' : p.url)}${p.url === peers.self.url ? ' <b class="self">self</b>' : ''}</td>
      <td>${p.health ? pct(p.health.score) : '—'}</td>
      <td>${p.health ? pct(p.health.cpu) : '—'}</td>
      <td>${p.health ? p.health.proxies : 0}</td>
      <td>${p.ms != null ? p.ms + 'ms' : '—'}</td>
      <td>${p.alive ? '<span class="ok">alive</span>' : '<span class="dead">stale</span>'}</td>
    </tr>`).join('');
  res.send(`<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8"><title>Vandal — health</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="15">
<style>
  :root { color-scheme: dark; }
  body { background:#0f0f0f; color:#e8e8e8; font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Hiragino Sans,"Noto Sans JP",sans-serif; margin:0; padding:32px; }
  h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:15px; margin:28px 0 8px; color:#aaa; font-weight:600; }
  .wrap { max-width:1080px; margin:0 auto; }
  .cards { display:flex; flex-wrap:wrap; gap:12px; margin:18px 0; }
  .card { background:#1b1b1b; border:1px solid #2c2c2c; border-radius:12px; padding:14px 18px; min-width:150px; }
  .card .v { font-size:24px; font-weight:700; } .card .k { color:#888; font-size:12px; }
  .tier { display:inline-block; color:#0f0f0f; font-weight:800; border-radius:6px; padding:1px 8px; }
  table { border-collapse:collapse; width:100%; } th,td { text-align:left; padding:6px 10px; border-bottom:1px solid #262626; }
  th { color:#888; font-weight:500; font-size:12px; } .mono { font-family:ui-monospace,monospace; font-size:12.5px; }
  .ok { color:#7ce38b; } .dead { color:#ff7b72; } .self { color:#ffd166; }
  .bar { background:#262626; border-radius:4px; height:8px; width:120px; display:inline-block; vertical-align:middle; }
  .bar i { display:block; height:8px; border-radius:4px; background:linear-gradient(90deg,#ff2ec4,#ff5c33,#ffe600); }
  a { color:#6ee7ff; } .foot { color:#666; font-size:12px; margin-top:28px; }
</style></head><body><div class="wrap">
<h1>🩺 Vandal instance health</h1>
<div>tier <span class="tier" style="background:${tierColor[d.tier]}">${d.tier}</span> ・ score <b>${d.score}</b>/100 ・ uptime ${Math.floor(d.uptimeSec / 60)} 分 ・ v${esc(d.version)}</div>
<div class="cards">
  <div class="card"><div class="v">${pct(d.cpu.busyPct)}</div><div class="k">CPU load</div></div>
  <div class="card"><div class="v">${d.eventLoop.lagMs}ms</div><div class="k">event-loop lag</div></div>
  <div class="card"><div class="v">${pct(d.memory.systemUsedPct)}</div><div class="k">memory</div></div>
  <div class="card"><div class="v">${d.http.active} <small style="color:#888">/ ${d.http.rps}rps</small></div><div class="k">混み具合（同時処理）</div></div>
  <div class="card"><div class="v">${d.proxies.pool} <small style="color:#888">(issuer ${d.proxies.issuers})</small></div><div class="k">使えるプロキシ</div></div>
  <div class="card"><div class="v">${d.mesh.peers} <small style="color:#888">(健全 ${d.mesh.healthyPeers})</small></div><div class="k">メッシュピア</div></div>
  <div class="card"><div class="v">${d.relay.active}</div><div class="k">動画中継アクティブ</div></div>
</div>
<h2>メッシュに参加しているインスタンス（ティア表）</h2>
<table><thead><tr><th>tier</th><th>instance</th><th>score</th><th>cpu</th><th>proxies</th><th>rtt</th><th>state</th></tr></thead>
<tbody>${peerRows || '<tr><td colspan="7" style="color:#888">ピアなし — 単独運用中（他の Vandal がこのURLの /.vandal-mesh に接続すると自動参加します）</td></tr>'}</tbody></table>
<p class="foot">15 秒ごとに自動更新 ・ 詳細 JSON は <a href="/health?format=json">/health?format=json</a> と <a href="/api/health">/api/health</a> ・ Vandal Project — independent open project</p>
</div></body></html>`);
});

module.exports = { router, fullHealthJson };

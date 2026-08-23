'use strict';/**
 * Diagnostics surface for llytpr-wl.v01nh — 設定ページのバックエンド。
 *
 *  - GET  /api/diag/logs?sse=1&level=&ch=&since=   超高精細ログ（SSE ライブ配信 or JSON）
 *  - GET  /api/diag/state                          プロキシプール/ホットキャッシュ/統計の全体像
 *  - GET  /api/diag/config / PUT(POST) 同          エンジン設定の読み書き（プロキシ運用モード等）
 *  - POST /api/diag/race {n}                       プロキシレース（L1→L2→L3 を実測、勝者を決定）
 *  - POST /api/diag/stream-test {v}                ストリーム取得テスト（発行→初バイト実測）
 *  - POST /api/diag/meta-test {v,q}                メタ情報取得テスト（watchNext/コメント/検索）
 *
 * すべてのテストは logbus チャンネル (race / streamtest / metatest) に超細かい
 * 経過イベントを流すため、SSE を開いていれば設定ページでリアルタイムに見える。
 *
 * Made by Kakinie with llytpr-wl.v01nh TEAM. V1
 */
const express = require('express');
const { request: undiciRequest, fetch: undiciFetch } = require('undici');
const { logbus } = require('./logbus');
const { engineConfig } = require('./config');
const { proxyManager } = require('./proxies');
const { hotChunks } = require('./media');
const it = require('./innertube');
const { gocore } = require('./gocore');

const router = express.Router();
router.use(express.json({ limit: '64kb' }));

/* ------------------------------------------------------------------ helpers */

const UA_ANDROID = 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip';
const CANARY_VIDEO = 'dQw4w9WgXcQ';
const YT_KEY = 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8';

/** 同一テストの多重実行を防ぐ（レースはプロキシ帯域を食うので 1 本ずつ） */
const busy = new Map();
function guard(key, res) {
  if (busy.has(key)) { res.status(409).json({ error: 'テスト実行中です。完了までお待ちください', code: 'BUSY' }); return false; }
  busy.set(key, Date.now());
  return true;
}
function unguard(key) { busy.delete(key); }

/* --------------------------------------------------------------- log stream */

router.get('/api/diag/logs', (req, res) => {
  const level = ['trace', 'debug', 'info', 'warn', 'error'].includes(req.query.level) ? req.query.level : null;
  const ch = req.query.ch ? String(req.query.ch) : null;
  const since = Number(req.query.since || 0) || 0;

  if (req.query.sse !== '1') {
    // ポーリングフォールバック（SSE が遮断される環境向け）
    res.json({ events: logbus.history({ since, level, ch }), status: logbus.status() });
    return;
  }
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
    'Access-Control-Allow-Origin': '*',
  });
  if (typeof res.flushHeaders === 'function') res.flushHeaders();
  res.write('retry: 3000\n\n');
  const want = (ev) => (!level || LEVEL_GTE(ev.level, level)) && (!ch || ev.ch === ch);
  let last = since;
  for (const ev of logbus.history({ since, level: undefined, ch: undefined, limit: 400 })) {
    if (want(ev)) { res.write(`id: ${ev.seq}\ndata: ${JSON.stringify(ev)}\n\n`); }
    last = Math.max(last, ev.seq);
  }
  res.write(`event: ready\ndata: ${JSON.stringify({ lastSeq: logbus.seq, status: logbus.status() })}\n\n`);
  const unsub = logbus.subscribe((ev) => {
    if (!want(ev)) return;
    try { res.write(`id: ${ev.seq}\ndata: ${JSON.stringify(ev)}\n\n`); } catch (_) { /* gone */ }
  });
  const hb = setInterval(() => { try { res.write(': hb\n\n'); } catch (_) { /* gone */ } }, 15000);
  req.on('close', () => { unsub(); clearInterval(hb); });
});

const LEVEL_ORDER = { trace: 10, debug: 20, info: 30, warn: 40, error: 50 };
function LEVEL_GTE(a, b) { return (LEVEL_ORDER[a] || 30) >= (LEVEL_ORDER[b] || 10); }

/* -------------------------------------------------------------------- state */

router.get('/api/diag/state', (req, res) => {
  const proxies = proxyManager.status();
  const st = hotChunks.status();
  res.json({
    proxies,
    hot: st,
    logbus: logbus.status(),
    config: engineConfig.get(),
    caches: {
      api: it.caches.api.size,
      streams: it.caches.streams.size,
      visitor: it.caches.visitor.size,
    },
    uptime: Math.round(process.uptime()),
    mem: {
      rss: Math.round(process.memoryUsage().rss / 1048576),
      heapUsed: Math.round(process.memoryUsage().heapUsed / 1048576),
    },
    node: process.version,
    core: gocore.status(),
  });
});

/* ------------------------------------------------------------------- config */

router.get('/api/diag/config', (req, res) => res.json(engineConfig.get()));

/** 設定ページ「L2/L3 認定を今すぐ実行」ボタン — 経過は ch:proxy に流れる */
router.post('/api/diag/certify', (req, res) => {
  logbus.info('proxy', '手動認定トリガー（設定ページ）');
  proxyManager.certify({ force: true }).catch(() => {});
  res.json({ ok: true });
});
router.put('/api/diag/config', handleConfig);
router.post('/api/diag/config', handleConfig);
router.post('/api/diag/config/reset', (req, res) => res.json(engineConfig.reset()));

function handleConfig(req, res) {
  const patch = (req.body && typeof req.body === 'object') ? req.body : {};
  if (patch.poolSize !== undefined && Number(patch.poolSize) > proxiesLen() + 60) {
    // 大きなプールは無料プロキシのスキャン時間が伸びるだけなので上限で飽和
    patch.poolSize = Math.min(120, Number(patch.poolSize) || 30);
  }
  res.json(engineConfig.set(patch));
  // プール目標数が変わったら即座に追従させる
  if (patch.poolSize !== undefined) proxyManager.refresh({ force: true }).catch(() => {});
}
function proxiesLen() { return proxyManager.pool ? proxyManager.pool.length : 0; }

/* -------------------------------------------------------------- proxy race */

/** L1: トンネル到達性 (www.youtube.com/generate_204) */
async function raceL1(url) {
  const t0 = Date.now();
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), 5000);
  try {
    const res = await undiciFetch('https://www.youtube.com/generate_204', {
      dispatcher: proxyManager.dispatcherFor(url), signal: ac.signal,
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });
    res.body?.dump?.().catch(() => {});
    return { ok: res.status < 500, ms: Date.now() - t0 };
  } catch (_) { return { ok: false, ms: Date.now() - t0 }; }
  finally { clearTimeout(t); }
}

/** L2: googlevideo への CONNECT トンネル */
async function raceL2(url) {
  const t0 = Date.now();
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), 6000);
  try {
    const res = await undiciFetch('https://rr5---sn-nx57ynsl.googlevideo.com/generate_204', {
      dispatcher: proxyManager.dispatcherFor(url), signal: ac.signal,
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });
    try { res.body?.cancel?.(); } catch (_) { /* noop */ }
    return { ok: res.status < 500, ms: Date.now() - t0 };
  } catch (_) { return { ok: false, ms: Date.now() - t0 }; }
  finally { clearTimeout(t); }
}

/** L3: youtubei player 発行が通るか（実動画で playability OK を実測） */
async function raceL3(url, videoId) {
  const t0 = Date.now();
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), 9000);
  try {
    const res = await undiciFetch(`https://www.youtube.com/youtubei/v1/player?key=${YT_KEY}`, {
      method: 'POST',
      dispatcher: proxyManager.dispatcherFor(url),
      signal: ac.signal,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': UA_ANDROID,
        'X-YouTube-Client-Name': '3',
        'X-YouTube-Client-Version': '21.26.364',
        'Origin': 'https://www.youtube.com',
      },
      body: JSON.stringify({
        videoId: videoId || CANARY_VIDEO, contentCheckOk: true, racyCheckOk: true, params: '2AMB',
        context: { client: { hl: 'ja', gl: 'JP', clientName: 'ANDROID', clientVersion: '21.26.364', androidSdkVersion: 30, osName: 'Android', osVersion: '11' } },
      }),
    });
    const j = await res.json().catch(() => null);
    const ok = res.ok && j?.playabilityStatus?.status === 'OK'
      && !!(((j.streamingData?.formats || []).length) || (j.streamingData?.adaptiveFormats || []).filter(f => f.url).length);
    return { ok, ms: Date.now() - t0, playability: j?.playabilityStatus?.status || 'HTTP ' + res.status };
  } catch (_) { return { ok: false, ms: Date.now() - t0, playability: 'network' }; }
  finally { clearTimeout(t); }
}

router.post('/api/diag/race', async (req, res) => {
  if (!guard('race', res)) return;
  const n = Math.min(12, Math.max(3, Number(req.body?.n) || 6));
  const videoId = /^[\w-]{11}$/.test(String(req.body?.v || '')) ? String(req.body.v) : CANARY_VIDEO;
  try {
    // プールが空なら先に補充
    if (!proxyManager.pool.length) await proxyManager.refresh({ force: true }).catch(() => {});
    const cands = [...proxyManager.pool]
      .sort((a, b) => a.latency - b.latency).slice(0, n).map(p => p.url);
    if (!cands.length) { res.status(503).json({ error: 'プロキシプールが空です' }); return; }
    logbus.info('race', `プロキシレース開始（${cands.length} 台 / L1→L2→L3 段階選抜）`, { v: videoId });

    // ---- Phase 1: L1 全員同時レース
    logbus.info('race', 'Phase 1: L1 トンネル到達レース', { n: cands.length });
    const p1 = await Promise.all(cands.map(async (url) => {
      const r = await raceL1(url);
      logbus.trace('race', `L1 ${r.ok ? '✓' : '✗'} ${r.ms}ms`, { url, phase: 1, ...r });
      return { url, l1: r };
    }));
    const l1pass = p1.filter(x => x.l1.ok).sort((a, b) => a.l1.ms - b.l1.ms);
    logbus.info('race', `Phase 1 終了: ${l1pass.length}/${cands.length} 台通過`, {
      pass: l1pass.map(x => ({ url: x.url, ms: x.l1.ms })),
    });

    // ---- Phase 2: L2 googlevideo トンネル（上位全数）
    const p2 = l1pass.length ? await Promise.all(l1pass.map(async (x) => {
      const r = await raceL2(x.url);
      logbus.trace('race', `L2 ${r.ok ? '✓' : '✗'} ${r.ms}ms`, { url: x.url, phase: 2, ...r });
      return { ...x, l2: r };
    })) : [];
    const l2pass = p2.filter(x => x.l2.ok).sort((a, b) => a.l1.ms - b.l1.ms);
    logbus.info('race', `Phase 2 終了: ${l2pass.length}/${p2.length} 台が googlevideo トンネル可`, {
      pass: l2pass.map(x => x.url),
    });

    // ---- Phase 3: L3 発行実測（全 L2 合格者、同時レース）
    const p3 = l2pass.length ? await Promise.all(l2pass.map(async (x) => {
      const r = await raceL3(x.url, videoId);
      logbus.trace('race', `L3 ${r.ok ? '✓' : '✗'} ${r.ms}ms (${r.playability})`, { url: x.url, phase: 3, ...r });
      return { ...x, l3: r };
    })) : [];
    const l3pass = p3.filter(x => x.l3.ok).sort((a, b) => (a.l3.ms + a.l1.ms) - (b.l3.ms + b.l1.ms));

    const winner = l3pass[0] || l2pass[0] || l1pass[0] || null;
    if (winner) {
      logbus.info('race', `レース終了 — 勝者 ${winner.url}`, {
        winner: winner.url,
        l1: winner.l1?.ms, l2: winner.l2?.ok ? winner.l2.ms : null,
        l3: winner.l3?.ok ? winner.l3.ms : null,
        grade: winner.l3?.ok ? 'L3 issuer（発行可能・最優先）' : winner.l2?.ok ? 'L2 gv（中継可）' : 'L1 tunnel',
      });
    } else {
      logbus.warn('race', 'レース終了 — 生き残ったプロキシなし（プール更新を推奨）');
    }
    res.json({
      ok: true,
      v: videoId,
      winner: winner ? {
        url: winner.url,
        l1: winner.l1?.ms ?? null,
        l2: winner.l2?.ok ? winner.l2.ms : null,
        l3: winner.l3?.ok ? winner.l3.ms : null,
      } : null,
      results: p3.length ? p3 : (p2.length ? p2 : p1),
      l1pass: l1pass.length, l2pass: l2pass.length, l3pass: l3pass.length,
    });
  } catch (e) {
    logbus.error('race', 'レース中にエラー', { err: e?.message });
    res.status(500).json({ error: e?.message || 'race failed' });
  } finally { unguard('race'); }
});

/* ------------------------------------------------------------ stream test */

router.post('/api/diag/stream-test', async (req, res) => {
  if (!guard('streamtest', res)) return;
  const v = String(req.body?.v || '');
  if (!/^[\w-]{11}$/.test(v)) { res.status(400).json({ error: '動画ID (11文字) を指定してください' }); unguard('streamtest'); return; }
  const out = { v, steps: [] };
  const step = (name, data) => { out.steps.push({ name, ...data }); };
  try {
    logbus.info('streamtest', `ストリーム取得テスト開始 (v=${v})`);

    // 1) キャッシュ済み map があるか（= 即応答できる状態か）
    const cachedBefore = it.caches.streams.getStale('map:' + v) || null;
    step('cacheCheck', { hit: !!cachedBefore, source: cachedBefore?.source || null });

    // 2) 新規発行（キャッシュ破棄して実測）
    const tIssue0 = Date.now();
    it.invalidateVideo(v);
    const { url, proxyUrl } = await it.getStreamUrl(v, 18);
    const issueMs = Date.now() - tIssue0;
    step('issuance', { ms: issueMs, ok: !!url, viaPin: proxyUrl || 'direct' });
    logbus.info('streamtest', `発行 ${issueMs}ms (${url ? 'URL取得 ✓' : '失敗 ✗'})`, { ms: issueMs, via: proxyUrl || 'direct' });

    // 3) 初バイト実測 — direct とピン経由を並列で計測
    if (url) {
      const ttfb = async (label, dispatcher) => {
        const t0 = Date.now();
        try {
          const r = await undiciRequest(url, {
            method: 'GET',
            headers: { Range: 'bytes=0-63', 'User-Agent': UA_ANDROID },
            dispatcher, headersTimeout: 12000, bodyTimeout: 12000,
          });
          const ms = Date.now() - t0;
          const ok = r.statusCode === 206 || r.statusCode === 200;
          r.body.dump().catch(() => {});
          logbus.info('streamtest', `初バイト (${label}) ${ok ? '✓' : '✗ ' + r.statusCode} ${ms}ms`, { ms, status: r.statusCode, via: label });
          return { ok, ms, status: r.statusCode };
        } catch (e) {
          const ms = Date.now() - t0;
          logbus.warn('streamtest', `初バイト (${label}) 失敗 ${ms}ms — ${e?.message || e}`, { ms, via: label });
          return { ok: false, ms, status: 0 };
        }
      };
      const [direct, pinned] = await Promise.all([
        ttfb('direct', undefined),
        proxyUrl ? ttfb('proxy-pin', proxyManager.dispatcherFor(proxyUrl)) : Promise.resolve(null),
      ]);
      step('ttfbDirect', direct);
      if (pinned) step('ttfbPinned', pinned);
    }

    // 4) ホットキャッシュに先読み
    if (url) {
      hotChunks.warm(v, 18, url, proxyUrl);
      await new Promise(r => setTimeout(r, 2500)); // 先読み完了を少し待つ
      const hot = hotChunks.map.get(v + ':18');
      step('hotCache', { warmed: !!hot, bytes: hot ? hot.buf.length : 0 });
      logbus.info('streamtest', `ホットキャッシュ ${hot ? `${hot.buf.length} bytes 載せ ✓` : '未載せ'}`);
    }

    logbus.info('streamtest', 'ストリーム取得テスト終了', { steps: out.steps.length });
    res.json({ ok: true, ...out, hot: hotChunks.status() });
  } catch (e) {
    logbus.error('streamtest', 'テスト中にエラー', { err: e?.message });
    res.status(502).json({ error: e?.message || 'stream test failed', ...out });
  } finally { unguard('streamtest'); }
});

/* --------------------------------------------------------------- meta test */

router.post('/api/diag/meta-test', async (req, res) => {
  if (!guard('metatest', res)) return;
  const v = String(req.body?.v || '');
  const q = String(req.body?.q || '').slice(0, 100);
  const out = { tests: {} };
  try {
    logbus.info('metatest', `メタ情報取得テスト開始${v ? ` (v=${v})` : ''}${q ? ` (q=${q})` : ''}`);

    let token = null;
    if (/^[\w-]{11}$/.test(v)) {
      // watchNext（メタ本体）
      it.caches.api.delete('w:' + v + 'jaJP');
      const t0 = Date.now();
      const w = await it.watchNext(v, { fresh: true });
      const ms = Date.now() - t0;
      token = w.commentsToken;
      out.tests.watchNext = { ms, ok: true, title: (w.title || '').slice(0, 60), related: w.related?.length || 0, hasCommentsToken: !!token };
      logbus.info('metatest', `watchNext ${ms}ms ✓（関連 ${w.related?.length || 0} 件）`, { ms });

      // コメント: トークン付き（高速経路）
      it.caches.api.delete('c0:' + v);
      const t1 = Date.now();
      const c1 = await it.comments(v, token || undefined).catch(e => ({ error: e.message }));
      const c1ms = Date.now() - t1;
      out.tests.commentsWithToken = { ms: c1ms, ok: !c1.error, count: c1.comments?.length || 0, count2: c1.count || '', error: c1.error };
      logbus.info('metatest', `コメント(トークン再利用) ${c1ms}ms — ${c1.comments?.length || 0} 件`, { ms: c1ms, via: 'token' });

      // コメント: トークン無し（旧来の 2 往復経路 — 差分を見せる）
      it.caches.api.delete('c0:' + v);
      const t2 = Date.now();
      const c2 = await it.comments(v).catch(e => ({ error: e.message }));
      const c2ms = Date.now() - t2;
      out.tests.commentsNoToken = { ms: c2ms, ok: !c2.error, count: c2.comments?.length || 0 };
      logbus.info('metatest', `コメント(トークン無し=2往復) ${c2ms}ms — ${c2.comments?.length || 0} 件`, { ms: c2ms, via: 'discovery' });
      out.tests.commentsSpeedup = c2ms > 0 && c1ms >= 0
        ? `${Math.max(1, Math.round(c2ms / Math.max(1, c1ms) * 10) / 10)}x 高速化（${c2ms}ms → ${c1ms}ms）`
        : null;
    }

    // 検索
    const query = q || (v ? null : '音楽 人気');
    if (query) {
      const t3 = Date.now();
      const s = await it.search(query, { fresh: true }).catch(e => ({ error: e.message }));
      const s3 = Date.now() - t3;
      out.tests.search = { ms: s3, ok: !s.error, items: s.items?.length || 0, error: s.error };
      logbus.info('metatest', `search「${query}」 ${s3}ms — ${s.items?.length || 0} 件`, { ms: s3 });
    }

    logbus.info('metatest', 'メタ情報取得テスト終了');
    res.json({ ok: true, ...out });
  } catch (e) {
    logbus.error('metatest', 'テスト中にエラー', { err: e?.message });
    res.status(502).json({ error: e?.message || 'meta test failed', ...out });
  } finally { unguard('metatest'); }
});

module.exports = { router };

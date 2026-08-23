'use strict';
/**
 * In-process hedge / probe — the Node fallback for the Go fetch core.
 *
 * Semantics match goedge/core.go so switching between Go and Node never
 * changes who wins a race or which errors are definitive:
 *   - first acceptable 2xx wins, losers are aborted (not markBad'd)
 *   - kind=player requires playabilityStatus=OK + formats/hls
 *   - UNPLAYABLE / AGE_CHECK / CONTENT_NOT_AVAILABLE abort the wave (451)
 *   - 4xx except 429 is a payload error (stop rotating)
 *
 * Used when CORE_ORIGIN is unset (Vercel / npm start) or the Go core is
 * not yet healthy. Never slower than the historical sequential loop:
 * TTFB is min(attempts) instead of sum(attempts).
 */
const { request: undiciRequest } = require('undici');
const { YTError } = require('./errors');

const MAX_HEDGE = 8;
const MAX_BODY = 4 * 1024 * 1024;

function dispatcherFor(proxy) {
  if (!proxy) return undefined;
  try { return require('./proxies').proxyManager.dispatcherFor(proxy); } catch (_) { return undefined; }
}

function isAbort(e) {
  if (!e) return false;
  const name = e.name || '';
  const code = e.code || '';
  return name === 'AbortError' || name === 'TimeoutError'
    || code === 'UND_ERR_ABORTED' || code === 'UND_ERR_DESTROYED'
    || /aborted|abort/i.test(String(e.message || ''));
}

function inspectPlayer(body) {
  let j = null;
  try { j = typeof body === 'string' ? JSON.parse(body) : body; } catch (_) { return { accept: false, err: 'not json' }; }
  const ps = j?.playabilityStatus || {};
  const status = ps.status || '';
  const reason = ps.reason || '';
  if (status !== 'OK') return { accept: false, playability: status || 'no playability', reason, err: status || 'no playability' };
  const sd = j.streamingData || {};
  if (sd.hlsManifestUrl) return { accept: true, playability: 'OK', reason };
  if ((sd.formats && sd.formats.length) || (sd.adaptiveFormats && sd.adaptiveFormats.length)) {
    return { accept: true, playability: 'OK', reason };
  }
  return { accept: false, playability: 'OK', reason, err: 'no formats' };
}

function isDefinitiveUnplayable(playability, reason) {
  if (!['UNPLAYABLE', 'AGE_CHECK_REQUIRED', 'CONTENT_NOT_AVAILABLE_IN_THIS_APP'].includes(playability)) return false;
  return !/ログイン|sign in/i.test(reason || '');
}

async function fetchOne(req, signal) {
  const timeout = Math.max(200, Math.min(60_000, Number(req.timeoutMs) || 9000));
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeout);
  const onParent = () => ac.abort();
  if (signal) {
    if (signal.aborted) { clearTimeout(t); const e = new Error('aborted'); e.name = 'AbortError'; throw e; }
    signal.addEventListener('abort', onParent, { once: true });
  }
  const method = String(req.method || 'GET').toUpperCase();
  try {
    const up = await undiciRequest(req.url, {
      method,
      headers: req.headers || {},
      body: (method !== 'GET' && method !== 'HEAD' && req.body) ? req.body : undefined,
      dispatcher: dispatcherFor(req.proxy),
      signal: ac.signal,
      maxRedirections: 3,
      headersTimeout: timeout,
      bodyTimeout: timeout,
    });
    const ab = await up.body.arrayBuffer();
    const buf = Buffer.from(ab);
    if (buf.length > MAX_BODY) return { status: up.statusCode, headers: {}, body: '', err: 'body too large' };
    const headers = {};
    const ct = up.headers['content-type'];
    if (ct) headers['content-type'] = Array.isArray(ct) ? ct[0] : ct;
    return { status: up.statusCode, headers, body: buf.toString('utf8') };
  } finally {
    clearTimeout(t);
    try { signal?.removeEventListener?.('abort', onParent); } catch (_) { /* noop */ }
  }
}

function judge(kind, fetched) {
  const att = {
    id: fetched.id,
    ok: fetched.status >= 200 && fetched.status < 300,
    accept: false,
    status: fetched.status || 0,
    ms: fetched.ms || 0,
    err: fetched.err || '',
    playability: '',
    reason: '',
  };
  if (fetched.err && !att.ok) return { att, unplay: false, fourxx: false };
  const fourxx = att.status >= 400 && att.status < 500 && att.status !== 429;
  if (kind === 'player' && att.ok) {
    const info = inspectPlayer(fetched.body);
    att.playability = info.playability || '';
    att.reason = info.reason || '';
    att.accept = !!info.accept;
    if (!att.accept) att.err = info.err || att.err;
    return { att, unplay: isDefinitiveUnplayable(att.playability, att.reason), fourxx: false, body: fetched.body, headers: fetched.headers };
  }
  if (kind === 'json' && att.ok) {
    try { JSON.parse(fetched.body); att.accept = true; } catch (_) { att.err = 'not json'; }
    return { att, unplay: false, fourxx, body: fetched.body, headers: fetched.headers };
  }
  att.accept = att.ok;
  return { att, unplay: false, fourxx: fourxx && !att.accept, body: fetched.body, headers: fetched.headers };
}

/**
 * @param {Array<object>} requests
 * @param {{kind?: string}} [opts]
 * @returns {Promise<{ok:boolean,winner?:string,status:number,headers?:object,body?:string,ms:number,attempts:object[]}>}
 */
async function hedge(requests, { kind = 'first-2xx' } = {}) {
  const reqs = (requests || []).slice(0, MAX_HEDGE);
  const attempts = [];
  if (!reqs.length) return { ok: false, status: 0, ms: 0, attempts };
  const ac = new AbortController();
  const t0 = Date.now();
  return await new Promise((resolve) => {
    let pending = reqs.length;
    let settled = false;
    const finish = (out) => {
      if (settled) return;
      settled = true;
      ac.abort();
      out.ms = Date.now() - t0;
      out.attempts = attempts.concat(out.attemptsExtra || []);
      delete out.attemptsExtra;
      resolve(out);
    };
    for (const req of reqs) {
      const id = req.id || req.url;
      const started = Date.now();
      fetchOne(req, ac.signal).then((got) => {
        if (settled) return;
        got.id = id;
        got.ms = Date.now() - started;
        const j = judge(kind, got);
        attempts.push(j.att);
        if (j.att.accept) {
          finish({ ok: true, winner: id, status: j.att.status, headers: j.headers || {}, body: j.body || '', attemptsExtra: [] });
          return;
        }
        if (j.unplay) {
          finish({ ok: false, winner: id, status: 451, body: j.body || '', attemptsExtra: [] });
          return;
        }
        if (j.fourxx) {
          finish({ ok: false, winner: id, status: j.att.status, headers: j.headers || {}, body: j.body || '', attemptsExtra: [] });
          return;
        }
        if (--pending <= 0) finish({ ok: false, status: 0, attemptsExtra: [] });
      }).catch((e) => {
        if (settled) return;
        if (!isAbort(e)) {
          attempts.push({ id, ok: false, accept: false, status: 0, ms: Date.now() - started, err: e?.message || 'network' });
        } else {
          attempts.push({ id, ok: false, accept: false, status: 0, ms: Date.now() - started, err: 'canceled' });
        }
        if (--pending <= 0) finish({ ok: false, status: 0, attemptsExtra: [] });
      });
    }
  });
}

async function fetchSingle(req) {
  return hedge([req], { kind: 'first-2xx' });
}

async function probe(probes) {
  const list = (probes || []).slice(0, 16);
  const results = await Promise.all(list.map(async (p) => {
    const timeout = Math.max(200, Math.min(15_000, Number(p.timeoutMs) || 4500));
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeout);
    const t0 = Date.now();
    try {
      const up = await undiciRequest(p.url, {
        method: 'GET',
        headers: {
          Range: 'bytes=0-63',
          Accept: '*/*',
          'User-Agent': 'com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip',
          ...(p.headers || {}),
        },
        dispatcher: dispatcherFor(p.proxy),
        signal: ac.signal,
        maxRedirections: 2,
        headersTimeout: timeout,
        bodyTimeout: timeout,
      });
      try { up.body.dump?.().catch(() => {}); } catch (_) { /* noop */ }
      const ok = up.statusCode === 200 || up.statusCode === 206;
      return { id: p.id || p.url, ok, status: up.statusCode, ms: Date.now() - t0 };
    } catch (e) {
      return { id: p.id || p.url, ok: false, status: 0, ms: Date.now() - t0, err: isAbort(e) ? 'canceled' : (e?.message || 'network') };
    } finally {
      clearTimeout(t);
    }
  }));
  return { results };
}

function throwIfHedgeDefinitive(result) {
  if (!result || result.ok) return;
  if (result.status === 451) {
    const a = (result.attempts || []).find(x => x.playability && isDefinitiveUnplayable(x.playability, x.reason));
    throw new YTError(a?.reason || a?.playability || '再生できません', 451, a?.playability || 'UNPLAYABLE');
  }
  if (result.status >= 400 && result.status < 500 && result.status !== 429) {
    let j = null;
    try { j = JSON.parse(result.body || ''); } catch (_) { /* noop */ }
    const msg = j?.error?.message || `YouTube HTTP ${result.status}`;
    throw new YTError(msg, result.status === 400 ? 400 : 502, j?.error?.status || ('HTTP_' + result.status));
  }
}

module.exports = {
  hedge, fetchSingle, probe, inspectPlayer, isDefinitiveUnplayable,
  isAbort, throwIfHedgeDefinitive, MAX_HEDGE,
};

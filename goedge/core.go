// Fetch core — the Go foundation for video-info & stream acquisition.
//
// Listens on 127.0.0.1 only (CORE_PORT). Node talks to it over loopback;
// the public internet never reaches these endpoints. If CORE_PORT is 0
// the core is not started and Node keeps using its own hedge (no regression).
//
// Endpoints:
//   GET  /health          liveness + counters
//   POST /v1/fetch        single outbound request
//   POST /v1/hedge        parallel first-acceptable-wins, losers cancelled
//   POST /v1/probe        parallel Range probes (206/200 = playable)
//   POST /v1/pin          register a video's googlevideo URL map
//   DELETE /v1/pin?v=     drop a pin
//   GET  /v1/pin?v=       lookup (debug)
//
// Outbound hosts are allow-listed (YouTube / googlevideo / ytimg / piped).
// Anything else is rejected so this can never become an open proxy.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/* ------------------------------------------------------------------ types */

type fetchReq struct {
	ID        string            `json:"id"`
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	Body      string            `json:"body"`
	Proxy     string            `json:"proxy"`
	TimeoutMs int               `json:"timeoutMs"`
}

type hedgeIn struct {
	Requests []fetchReq `json:"requests"`
	Kind     string     `json:"kind"` // "", "first-2xx", "json", "player"
}

type attemptOut struct {
	ID          string `json:"id"`
	OK          bool   `json:"ok"`
	Accept      bool   `json:"accept"`
	Status      int    `json:"status"`
	Ms          int64  `json:"ms"`
	Err         string `json:"err,omitempty"`
	Playability string `json:"playability,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type hedgeOut struct {
	OK       bool              `json:"ok"`
	Winner   string            `json:"winner,omitempty"`
	Status   int               `json:"status"`
	Headers  map[string]string `json:"headers,omitempty"`
	Body     string            `json:"body,omitempty"`
	Ms       int64             `json:"ms"`
	Attempts []attemptOut      `json:"attempts"`
}

type probeIn struct {
	Probes []fetchReq `json:"probes"`
}

type probeOne struct {
	ID     string `json:"id"`
	OK     bool   `json:"ok"`
	Status int    `json:"status"`
	Ms     int64  `json:"ms"`
	Err    string `json:"err,omitempty"`
}

type probeOut struct {
	Results []probeOne `json:"results"`
}

type pinIn struct {
	VideoID  string            `json:"videoId"`
	Map      map[string]string `json:"map"`
	ProxyURL string            `json:"proxyUrl"`
	Exp      int64             `json:"exp"` // unix ms; 0 = +5h
	WarmItag string            `json:"warmItag"`
}

type coreStats struct {
	hedges atomic.Int64
	fetches atomic.Int64
	probes atomic.Int64
	pins   atomic.Int64
	relays atomic.Int64
	wins   atomic.Int64
}

func (s *coreStats) snapshot(pins int, listen string, up bool) map[string]interface{} {
	if s == nil {
		return map[string]interface{}{"ok": false}
	}
	return map[string]interface{}{
		"ok":      up,
		"listen":  listen,
		"pins":    pins,
		"hedges":  s.hedges.Load(),
		"fetches": s.fetches.Load(),
		"probes":  s.probes.Load(),
		"pinSets": s.pins.Load(),
		"relays":  s.relays.Load(),
		"wins":    s.wins.Load(),
	}
}

/* ---------------------------------------------------------------- runtime */

// rtEdge is wired up in main() so the public handler and /__edge/healthz
// can see the core without threading every pointer through serveHealth.
var rtEdge = struct {
	pins   *pinStore
	pool   *clientPool
	stats  *coreStats
	listen string
	up     atomic.Bool
}{}

/* ----------------------------------------------------------- allow-list */

func hostAllowed(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	// Loopback is test-only (EDGE_ALLOW_LOCAL=1). Production must never
	// fetch localhost — that would be SSRF through the core.
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return allowLocal()
	}
	if host == "youtube.com" || strings.HasSuffix(host, ".youtube.com") {
		return true
	}
	if host == "googlevideo.com" || strings.HasSuffix(host, ".googlevideo.com") {
		return true
	}
	if host == "ytimg.com" || strings.HasSuffix(host, ".ytimg.com") {
		return true
	}
	if host == "ggpht.com" || strings.HasSuffix(host, ".ggpht.com") {
		return true
	}
	if host == "googleusercontent.com" || strings.HasSuffix(host, ".googleusercontent.com") {
		return true
	}
	if host == "googleapis.com" || strings.HasSuffix(host, ".googleapis.com") {
		return true
	}
	if host == "gstatic.com" || strings.HasSuffix(host, ".gstatic.com") {
		return true
	}
	if host == "google.com" || strings.HasSuffix(host, ".google.com") {
		return true
	}
	// Piped public instances (same rule as Node isGoogleVideo).
	if strings.HasPrefix(host, "piped.") || strings.Contains(host, ".piped.") ||
		strings.HasPrefix(host, "pipedapi.") || strings.HasPrefix(host, "pipedproxy.") {
		return true
	}
	return false
}

func urlAllowed(raw string) bool {
	u, err := parseAbsURL(raw)
	if err != nil {
		return false
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return false
	}
	return hostAllowed(u.Hostname())
}

func proxyAllowed(raw string) bool {
	if raw == "" {
		return true
	}
	u, err := parseAbsURL(raw)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	h := strings.ToLower(u.Hostname())
	// Proxies are untrusted IPs by design (the free-proxy pool). We only
	// refuse loopback/link-local so the core cannot be pointed at itself
	// or at cloud metadata.
	if h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "0.0.0.0" {
		return allowLocal()
	}
	if strings.HasPrefix(h, "169.254.") || strings.HasPrefix(h, "10.") ||
		strings.HasPrefix(h, "192.168.") || strings.HasPrefix(h, "172.") {
		// RFC1918 / link-local: the existing Node ProxyAgent already
		// accepts these (that's where free proxies live). Allow.
		return true
	}
	return true
}

func parseAbsURL(raw string) (*url.URL, error) {
	return url.Parse(raw)
}

/* ------------------------------------------------------- player accept */

type playerMeta struct {
	status string
	reason string
}

func inspectPlayer(body []byte) (accept bool, meta playerMeta, errMsg string) {
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		return false, meta, "not json"
	}
	if ps, _ := m["playabilityStatus"].(map[string]interface{}); ps != nil {
		meta.status, _ = ps["status"].(string)
		meta.reason, _ = ps["reason"].(string)
	}
	if meta.status != "OK" {
		if meta.status == "" {
			return false, meta, "no playability"
		}
		return false, meta, meta.status
	}
	sd, _ := m["streamingData"].(map[string]interface{})
	if sd == nil {
		return false, meta, "no streamingData"
	}
	if s, _ := sd["hlsManifestUrl"].(string); strings.TrimSpace(s) != "" {
		return true, meta, ""
	}
	if hasNonEmptyArray(sd["formats"]) || hasNonEmptyArray(sd["adaptiveFormats"]) {
		return true, meta, ""
	}
	return false, meta, "no formats"
}

func hasNonEmptyArray(v interface{}) bool {
	arr, ok := v.([]interface{})
	return ok && len(arr) > 0
}

// signInish reports whether a playability reason is a login wall (rotation
// can still fix it) rather than a definitive block.
func signInish(s string) bool {
	l := strings.ToLower(s)
	return strings.Contains(l, "sign in") || strings.Contains(s, "ログイン")
}

func isDefinitiveUnplayable(meta playerMeta) bool {
	switch meta.status {
	case "UNPLAYABLE", "AGE_CHECK_REQUIRED", "CONTENT_NOT_AVAILABLE_IN_THIS_APP":
		return !signInish(meta.reason)
	}
	return false
}

/* ----------------------------------------------------------- hedge run */

type hedgeKind int

const (
	kindFirst2xx hedgeKind = iota
	kindJSON
	kindPlayer
)

func parseKind(s string) hedgeKind {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "player":
		return kindPlayer
	case "json":
		return kindJSON
	default:
		return kindFirst2xx
	}
}

type oneResult struct {
	req      fetchReq
	att      attemptOut
	body     string
	headers  map[string]string
	unplay   bool
	fourxx   bool
}

func runHedge(parent context.Context, pool *clientPool, in hedgeIn) hedgeOut {
	kind := parseKind(in.Kind)
	reqs := in.Requests
	if len(reqs) > maxHedge {
		reqs = reqs[:maxHedge]
	}
	out := hedgeOut{Attempts: make([]attemptOut, 0, len(reqs))}
	if len(reqs) == 0 {
		out.ErrEmpty()
		return out
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	ch := make(chan oneResult, len(reqs))
	var wg sync.WaitGroup
	for i := range reqs {
		wg.Add(1)
		go func(fr fetchReq) {
			defer wg.Done()
			ch <- doOne(ctx, pool, fr, kind)
		}(reqs[i])
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	t0 := time.Now()
	pending := len(reqs)
	for r := range ch {
		out.Attempts = append(out.Attempts, r.att)
		pending--
		if r.att.Accept {
			cancel()
			out.OK = true
			out.Winner = r.req.ID
			out.Status = r.att.Status
			out.Headers = r.headers
			out.Body = r.body
			out.Ms = time.Since(t0).Milliseconds()
			// drain the rest so we don't leak goroutines waiting on send
			// (channel is buffered + closer, so this is just bookkeeping)
			for extra := range ch {
				out.Attempts = append(out.Attempts, extra.att)
			}
			return out
		}
		// Preserve Node tryOnce / firstWin: a definitive UNPLAYABLE aborts
		// the whole wave (video is blocked, more clients will not help).
		if r.unplay {
			cancel()
			out.OK = false
			out.Winner = r.req.ID
			out.Status = 451
			out.Body = r.body
			out.Ms = time.Since(t0).Milliseconds()
			for extra := range ch {
				out.Attempts = append(out.Attempts, extra.att)
			}
			return out
		}
		// 4xx (except 429) is a payload error — same combo on another
		// transport will not help. Return it so Node can throw YTError.
		if r.fourxx {
			cancel()
			out.OK = false
			out.Winner = r.req.ID
			out.Status = r.att.Status
			out.Headers = r.headers
			out.Body = r.body
			out.Ms = time.Since(t0).Milliseconds()
			for extra := range ch {
				out.Attempts = append(out.Attempts, extra.att)
			}
			return out
		}
		if pending <= 0 {
			break
		}
	}
	out.Ms = time.Since(t0).Milliseconds()
	return out
}

func (o *hedgeOut) ErrEmpty() {
	o.OK = false
	o.Attempts = []attemptOut{}
}

func doOne(ctx context.Context, pool *clientPool, fr fetchReq, kind hedgeKind) oneResult {
	id := fr.ID
	if id == "" {
		id = fr.URL
	}
	att := attemptOut{ID: id}
	t0 := time.Now()
	fail := func(msg string) oneResult {
		att.Ms = time.Since(t0).Milliseconds()
		att.Err = msg
		return oneResult{req: fr, att: att}
	}
	if !urlAllowed(fr.URL) {
		return fail("host not allowed")
	}
	if !proxyAllowed(fr.Proxy) {
		return fail("proxy not allowed")
	}
	method := strings.ToUpper(strings.TrimSpace(fr.Method))
	if method == "" {
		method = http.MethodGet
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost:
	default:
		return fail("method not allowed")
	}

	timeout := clampTimeout(fr.TimeoutMs)
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var bodyReader io.Reader
	if fr.Body != "" && method != http.MethodGet && method != http.MethodHead {
		bodyReader = strings.NewReader(fr.Body)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, fr.URL, bodyReader)
	if err != nil {
		return fail(err.Error())
	}
	for k, v := range fr.Headers {
		if k == "" || v == "" {
			continue
		}
		// Hop-by-hop / identity headers must not be forwarded.
		lk := strings.ToLower(k)
		if lk == "host" || lk == "content-length" || lk == "connection" || lk == "transfer-encoding" {
			continue
		}
		req.Header.Set(k, v)
	}

	res, err := pool.do(req, strings.TrimSpace(fr.Proxy))
	att.Ms = time.Since(t0).Milliseconds()
	if err != nil {
		if reqCtx.Err() != nil && ctx.Err() != nil {
			att.Err = "canceled"
			return oneResult{req: fr, att: att}
		}
		att.Err = err.Error()
		return oneResult{req: fr, att: att}
	}
	defer drainAndClose(res)
	att.Status = res.StatusCode

	limited := io.LimitReader(res.Body, maxBodyBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		att.Err = err.Error()
		return oneResult{req: fr, att: att}
	}
	if len(raw) > maxBodyBytes {
		att.Err = "body too large"
		return oneResult{req: fr, att: att}
	}
	hdrs := map[string]string{}
	if ct := res.Header.Get("Content-Type"); ct != "" {
		hdrs["content-type"] = ct
	}
	body := string(raw)
	att.OK = res.StatusCode >= 200 && res.StatusCode < 300

	fourxx := res.StatusCode >= 400 && res.StatusCode < 500 && res.StatusCode != 429
	unplay := false

	switch kind {
	case kindPlayer:
		if att.OK {
			ok, meta, msg := inspectPlayer(raw)
			att.Playability = meta.status
			att.Reason = meta.reason
			att.Accept = ok
			if !ok {
				att.Err = msg
			}
			if isDefinitiveUnplayable(meta) {
				unplay = true
			}
		}
	case kindJSON:
		if att.OK {
			var tmp interface{}
			if json.Unmarshal(raw, &tmp) == nil {
				att.Accept = true
			} else {
				att.Err = "not json"
			}
		}
	default: // first-2xx
		att.Accept = att.OK
	}

	return oneResult{
		req:     fr,
		att:     att,
		body:    body,
		headers: hdrs,
		unplay:  unplay,
		fourxx:  fourxx && !att.Accept,
	}
}

/* -------------------------------------------------------------- probe */

func runProbe(parent context.Context, pool *clientPool, in probeIn) probeOut {
	probes := in.Probes
	if len(probes) > 16 {
		probes = probes[:16]
	}
	out := probeOut{Results: make([]probeOne, len(probes))}
	if len(probes) == 0 {
		return out
	}
	var wg sync.WaitGroup
	for i := range probes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out.Results[i] = doProbe(parent, pool, probes[i])
		}(i)
	}
	wg.Wait()
	return out
}

func doProbe(parent context.Context, pool *clientPool, fr fetchReq) probeOne {
	id := fr.ID
	if id == "" {
		id = fr.URL
	}
	one := probeOne{ID: id}
	t0 := time.Now()
	if !urlAllowed(fr.URL) {
		one.Err = "host not allowed"
		one.Ms = time.Since(t0).Milliseconds()
		return one
	}
	if !proxyAllowed(fr.Proxy) {
		one.Err = "proxy not allowed"
		one.Ms = time.Since(t0).Milliseconds()
		return one
	}
	timeout := clampTimeout(fr.TimeoutMs)
	if timeout > 15*time.Second {
		timeout = 15 * time.Second
	}
	ctx, cancel := ctxWithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fr.URL, nil)
	if err != nil {
		one.Err = err.Error()
		one.Ms = time.Since(t0).Milliseconds()
		return one
	}
	req.Header.Set("Range", "bytes=0-63")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip")
	for k, v := range fr.Headers {
		if k == "" || v == "" {
			continue
		}
		req.Header.Set(k, v)
	}
	res, err := pool.do(req, strings.TrimSpace(fr.Proxy))
	one.Ms = time.Since(t0).Milliseconds()
	if err != nil {
		one.Err = err.Error()
		return one
	}
	drainAndClose(res)
	one.Status = res.StatusCode
	one.OK = res.StatusCode == 200 || res.StatusCode == 206
	return one
}

/* ----------------------------------------------------------- http api */

func newCoreHandler(cfg *config, pins *pinStore, pool *clientPool, stats *coreStats) http.Handler {
	mux := http.NewServeMux()
	auth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if cfg != nil && cfg.coreToken != "" {
				if r.Header.Get("X-Persimmon-Core") != cfg.coreToken {
					http.Error(w, `{"error":"forbidden","code":"CORE_AUTH"}`, http.StatusForbidden)
					return
				}
			}
			next(w, r)
		}
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"ok":    true,
			"mode":  "go-core",
			"stats": stats.snapshot(pins.count(), listenOf(cfg), true),
		})
	})
	mux.HandleFunc("/v1/fetch", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		var fr fetchReq
		if err := readJSON(r, &fr); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad json"})
			return
		}
		stats.fetches.Add(1)
		out := runHedge(r.Context(), pool, hedgeIn{Requests: []fetchReq{fr}, Kind: "first-2xx"})
		if out.OK {
			stats.wins.Add(1)
		}
		writeJSON(w, 200, out)
	}))
	mux.HandleFunc("/v1/hedge", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		var in hedgeIn
		if err := readJSON(r, &in); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad json"})
			return
		}
		stats.hedges.Add(1)
		out := runHedge(r.Context(), pool, in)
		if out.OK {
			stats.wins.Add(1)
		}
		writeJSON(w, 200, out)
	}))
	mux.HandleFunc("/v1/probe", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		var in probeIn
		if err := readJSON(r, &in); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad json"})
			return
		}
		stats.probes.Add(1)
		writeJSON(w, 200, runProbe(r.Context(), pool, in))
	}))
	mux.HandleFunc("/v1/pin", auth(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var in pinIn
			if err := readJSON(r, &in); err != nil {
				writeJSON(w, 400, map[string]string{"error": "bad json"})
				return
			}
			if !validVideoID(in.VideoID) {
				writeJSON(w, 400, map[string]string{"error": "bad id"})
				return
			}
			if err := pins.set(in, pool); err != nil {
				writeJSON(w, 400, map[string]string{"error": err.Error()})
				return
			}
			stats.pins.Add(1)
			writeJSON(w, 200, map[string]interface{}{"ok": true, "videoId": in.VideoID, "itagCount": len(in.Map)})
		case http.MethodDelete:
			v := r.URL.Query().Get("v")
			pins.del(v)
			writeJSON(w, 200, map[string]interface{}{"ok": true})
		case http.MethodGet:
			v := r.URL.Query().Get("v")
			e := pins.get(v)
			if e == nil {
				writeJSON(w, 404, map[string]string{"error": "not pinned"})
				return
			}
			writeJSON(w, 200, map[string]interface{}{
				"ok": true, "videoId": v, "itagCount": len(e.mapURLs),
				"proxyUrl": e.proxyURL, "exp": e.exp.UnixMilli(),
			})
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	return mux
}

func listenOf(cfg *config) string {
	if cfg == nil {
		return ""
	}
	return net.JoinHostPort(cfg.coreBind, strconv.Itoa(cfg.corePort))
}

func validVideoID(id string) bool {
	if len(id) != 11 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func readJSON(r *http.Request, dst interface{}) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 2<<20))
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// startCore binds the loopback fetch-core server. Failure is non-fatal:
// the public edge still serves assets + proxies to Node, and Node falls
// back to its in-process hedge. Never listen on 0.0.0.0 for the core.
func startCore(cfg *config, pins *pinStore, pool *clientPool, stats *coreStats) *http.Server {
	if cfg == nil || cfg.corePort <= 0 {
		return nil
	}
	bind := cfg.coreBind
	if bind == "" {
		bind = "127.0.0.1"
	}
	// Hard safety: refuse a non-loopback bind unless the operator really
	// asked (CORE_BIND). Default is 127.0.0.1.
	addr := net.JoinHostPort(bind, strconv.Itoa(cfg.corePort))
	srv := &http.Server{
		Addr:              addr,
		Handler:           newCoreHandler(cfg, pins, pool, stats),
		ReadHeaderTimeout: 8 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[edge] fetch-core listen 失敗 (%s): %v — Node 側フォールバックで続行", addr, err)
		return nil
	}
	rtEdge.up.Store(true)
	rtEdge.listen = addr
	log.Printf("[edge] fetch-core 起動 %s （動画情報・ストリーム取得の Go 基盤）", addr)
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[edge] fetch-core 停止: %v", err)
			rtEdge.up.Store(false)
		}
	}()
	return srv
}

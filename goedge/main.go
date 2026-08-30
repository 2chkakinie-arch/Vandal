// Command vandal-edge — Go accelerator front for the Vandal Node backend.
//
// 役割（Go 基盤による高速化レイヤー）:
//   - SPA シェルの静的アセット（/index.html, /app.js, /styles.css, /logo.png,
//     /vendor/hls.min.js）を起動時に Node バックエンドから 1 回だけ取り込み、
//     メモリ上に「生バイト + gzip 事前圧縮バイト」の両方を保持して即座に配信する
//     （リクエストごとの fs stat / gzip 圧縮 CPU を完全に無くす）。
//   - ETag / Last-Modified / If-None-Match / If-Modified-Since（304）を処理。
//   - それ以外のすべて（/api/*, SSE, 動画リレー…）は httputil.ReverseProxy で
//     ストリーミング透過（FlushInterval=-1 = バッファリング無し）。
//     Range 要求・POST 等も含め、バックエンドの応答を 1 バイトも改変しない。
//   - アセット取得に失敗した場合はそのパスも素通しプロキシに落ちるため、
//     どんな失敗時でも挙動は「従来どおりの Node 直配信」と同一に保たれる。
//
// 標準ライブラリのみ使用（go.mod に外部依存ゼロ）— Go さえあればオフラインで
// ビルドできる。Go 1.20+ で動作。
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

/* ------------------------------------------------------------------ 設定 */

type config struct {
	bind         string        // 公開バインドアドレス（既定 0.0.0.0）
	port         int           // 公開ポート（PORT env、既定 3000）
	origin       *url.URL      // Node バックエンド（EDGE_ORIGIN、既定 http://127.0.0.1:3101）
	refreshEvery time.Duration // アセット再取り込み間隔（EDGE_ASSET_REFRESH_MS、既定 300000ms、0=無効）
	bootTimeout  time.Duration // 初回アセット取り込みの総リトライ時間（既定 45s）
	healthGrace  time.Duration // 起動直後、origin down でも healthz を 200 にする猶予（既定 60s）
	verbose      bool
	coreBind     string // fetch-core バインド（既定 127.0.0.1 — 公開しない）
	corePort     int    // fetch-core ポート（CORE_PORT、0=無効）
	coreToken    string // 任意の共有秘密（X-Vandal-Core）
}

func envInt(name string, def, min, max int) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || v < min || v > max {
		return def
	}
	return v
}

func loadConfig() *config {
	port := envInt("PORT", 3000, 1, 65535)
	originRaw := strings.TrimSpace(os.Getenv("EDGE_ORIGIN"))
	if originRaw == "" {
		originRaw = "http://127.0.0.1:3101"
	}
	if !strings.Contains(originRaw, "://") {
		originRaw = "http://" + originRaw
	}
	origin, err := url.Parse(originRaw)
	if err != nil || origin.Host == "" {
		log.Fatalf("[edge] EDGE_ORIGIN が不正です: %q (%v)", originRaw, err)
	}
	bind := strings.TrimSpace(os.Getenv("EDGE_BIND"))
	if bind == "" {
		bind = "0.0.0.0"
	}
	coreBind := strings.TrimSpace(os.Getenv("CORE_BIND"))
	if coreBind == "" {
		coreBind = "127.0.0.1"
	}
	return &config{
		bind:         bind,
		port:         port,
		origin:       origin,
		refreshEvery: time.Duration(envInt("EDGE_ASSET_REFRESH_MS", 300000, 0, 24*3600*1000)) * time.Millisecond,
		bootTimeout:  time.Duration(envInt("EDGE_BOOT_TIMEOUT_MS", 45000, 1000, 10*60*1000)) * time.Millisecond,
		healthGrace:  time.Duration(envInt("EDGE_HEALTH_GRACE_MS", 60000, 0, 10*60*1000)) * time.Millisecond,
		verbose:      os.Getenv("EDGE_VERBOSE") == "1",
		coreBind:     coreBind,
		corePort:     envInt("CORE_PORT", 0, 0, 65535),
		coreToken:    strings.TrimSpace(os.Getenv("CORE_TOKEN")),
	}
}

/* ------------------------------------------------- アセットのメモリキャッシュ */

// 高速パス対象の静的アセット。Node 側の配信仕様（index.js / client-bundle.js）と
// 同一のパス・同一のキャッシュ意味論（js/css は max-age=3600、index.html は no-cache）
// を origin 応答ヘッダからそのまま引き継ぐため、鮮度挙動は従来と変わらない。
var assetPaths = []string{
	"/index.html",
	"/app.js",
	"/styles.css",
	"/logo.png",
	"/vendor/hls.min.js",
}

const (
	assetMaxBytes = 64 << 20 // 異常応答からメモリを守る上限（実際は数百KB級）
	gzipThreshold = 860      // Node の compression 既定に合わせる（未満は圧縮しない）
)

type asset struct {
	raw          []byte
	gz           []byte // raw が閾値未満なら nil
	contentType  string
	cacheControl string
	etag         string
	lastModified string
	fetchedAt    time.Time
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		tok := strings.SplitN(part, ";", 2)[0]
		if strings.EqualFold(strings.TrimSpace(tok), "gzip") {
			// q=0 指定（拒否）ではないか確認
			if strings.Contains(part, ";") {
				params := strings.Split(part, ";")[1]
				params = strings.ReplaceAll(params, " ", "")
				if strings.HasPrefix(params, "q=0") {
					continue
				}
			}
			return true
		}
	}
	return false
}

func etagMatch(header, etag string) bool {
	if header == "" || etag == "" {
		return false
	}
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == etag || strings.TrimPrefix(candidate, "W/") == strings.TrimPrefix(etag, "W/") {
			return true
		}
	}
	return false
}

func makeETag(raw []byte) string {
	sum := sha1.Sum(raw)
	return "\"a-" + strconv.Itoa(len(raw)) + "-" + hex.EncodeToString(sum[:8]) + "\""
}

type assetStore struct {
	mu sync.RWMutex
	m  map[string]*asset
}

func newAssetStore() *assetStore { return &assetStore{m: make(map[string]*asset)} }

func (s *assetStore) get(path string) *asset {
	if path == "/" {
		path = "/index.html"
	}
	s.mu.RLock()
	a := s.m[path]
	s.mu.RUnlock()
	return a
}

func (s *assetStore) put(path string, a *asset) {
	s.mu.Lock()
	s.m[path] = a
	s.mu.Unlock()
}

func (s *assetStore) count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

/* ----------------------------------------------------------- origin への取り込み */

type puller struct {
	cfg    *config
	client *http.Client
	store  *assetStore
}

func newPuller(cfg *config, store *assetStore) *puller {
	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     60 * time.Second,
		// 明示的に identity を要求するので自動 gzip は邪魔にしかならない
		DisableCompression: true,
	}
	return &puller{
		cfg:    cfg,
		client: &http.Client{Transport: tr, Timeout: 15 * time.Second},
		store:  store,
	}
}

func (p *puller) pullOne(path string) error {
	u := p.cfg.origin.Scheme + "://" + p.cfg.origin.Host + path
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	res, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		return &httpError{path: path, status: res.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, assetMaxBytes+1))
	if err != nil {
		return err
	}
	if len(body) > assetMaxBytes {
		return &httpError{path: path, status: -1} // 大きすぎる → キャッシュせずプロキシに任せる
	}
	a := &asset{
		raw:          body,
		contentType:  res.Header.Get("Content-Type"),
		cacheControl: res.Header.Get("Cache-Control"),
		etag:         res.Header.Get("ETag"),
		lastModified: res.Header.Get("Last-Modified"),
		fetchedAt:    time.Now(),
	}
	if a.contentType == "" {
		a.contentType = http.DetectContentType(body)
	}
	if a.etag == "" {
		a.etag = makeETag(body)
	}
	if a.lastModified == "" {
		a.lastModified = a.fetchedAt.UTC().Format(http.TimeFormat)
	}
	if len(body) >= gzipThreshold && wantsGzip(a.contentType) {
		var buf bytes.Buffer
		zw, zerr := gzip.NewWriterLevel(&buf, gzip.BestSpeed) // Node の level 1 相当
		if zerr == nil {
			if _, err := zw.Write(body); err == nil && zw.Close() == nil && buf.Len() < len(body) {
				a.gz = buf.Bytes()
			}
		}
	}
	p.store.put(path, a)
	return nil
}

type httpError struct {
	path   string
	status int
}

func (e *httpError) Error() string {
	return "pull " + e.path + ": status " + strconv.Itoa(e.status)
}

// js/css/json/html/svg などのテキストだけ圧縮する（png 等の非圧縮性バイナリは対象外）
func wantsGzip(contentType string) bool {
	ct := strings.ToLower(strings.SplitN(contentType, ";", 2)[0])
	return strings.HasPrefix(ct, "text/") ||
		strings.Contains(ct, "javascript") ||
		strings.Contains(ct, "json") ||
		strings.HasSuffix(ct, "+xml") ||
		ct == "image/svg+xml" ||
		ct == "application/wasm"
}

/* -------------------------------------------------------------- アセット配信 */

func serveAsset(w http.ResponseWriter, r *http.Request, a *asset, cfg *config) {
	h := w.Header()
	h.Set("Content-Type", a.contentType)
	if a.cacheControl != "" {
		h.Set("Cache-Control", a.cacheControl)
	}
	h.Set("ETag", a.etag)
	h.Set("Last-Modified", a.lastModified)
	h.Set("Vary", "Accept-Encoding")
	h.Set("X-Edge-Asset", "memory")

	// 条件付きリクエスト → 304
	if etagMatch(r.Header.Get("If-None-Match"), a.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if inm := r.Header.Get("If-None-Match"); inm == "" {
		if ims := r.Header.Get("If-Modified-Since"); ims != "" {
			if t, err := http.ParseTime(ims); err == nil {
				if lm, err2 := http.ParseTime(a.lastModified); err2 == nil && !lm.After(t.Add(1*time.Second)) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}

	body := a.raw
	if a.gz != nil && acceptsGzip(r) {
		body = a.gz
		h.Set("Content-Encoding", "gzip")
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil && cfg.verbose {
		log.Printf("[edge] asset write error: %v", err)
	}
}

/* ------------------------------------------------------------------ プロキシ */

func newOriginProxy(cfg *config) *httputil.ReverseProxy {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0,    // 動画リレー/SSE は長時間応答があり得る → 制限しない
		DisableCompression:    true, // クライアントの Accept-Encoding を改変せず素通し
	}
	proxy := httputil.NewSingleHostReverseProxy(cfg.origin)
	base := proxy.Director
	proxy.Director = func(r *http.Request) {
		base(r) // scheme/host 付け替え + X-Forwarded-For は ReverseProxy が自動付与
		// Host ヘッダはあえて触らない（r.Host がクライアント向けホストのまま透過し、
		// Node 直配信時と同一の値がバックエンドに届く）
		proto := "http"
		if r.TLS != nil {
			proto = "https"
		}
		if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
			proto = strings.Split(fwd, ",")[0]
			proto = strings.TrimSpace(proto)
		}
		r.Header.Set("X-Forwarded-Proto", proto)
		r.Header.Set("X-Forwarded-Host", r.Host)
	}
	proxy.Transport = tr
	proxy.BufferPool = &rawBufferPool{p: sync.Pool{New: func() interface{} {
		b := make([]byte, 32*1024)
		return &b
	}}}
	// 動画リレー（/api/stream）をバッファせず即時フラッシュ
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if cfg.verbose {
			log.Printf("[edge] upstream error %s %s: %v", r.Method, r.URL.Path, err)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, `{"error":"origin temporarily unavailable","code":"EDGE_UPSTREAM"}`)
	}
	return proxy
}

type rawBufferPool struct{ p sync.Pool }

func (bp *rawBufferPool) Get() []byte  { return *(bp.p.Get().(*[]byte)) }
func (bp *rawBufferPool) Put(b []byte) { bp.p.Put(&b) }

/* --------------------------------------------------------------- ヘルス監視 */

type originMonitor struct {
	cfg     *config
	client  *http.Client
	mu      sync.RWMutex
	up      bool
	lastRTT time.Duration
}

func newOriginMonitor(cfg *config) *originMonitor {
	return &originMonitor{
		cfg:    cfg,
		client: &http.Client{Timeout: 4 * time.Second},
	}
}

func (m *originMonitor) pingOnce() bool {
	u := m.cfg.origin.Scheme + "://" + m.cfg.origin.Host + "/api/health"
	t0 := time.Now()
	res, err := m.client.Get(u)
	up := false
	if err == nil {
		io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		up = res.StatusCode < 500
	}
	m.mu.Lock()
	prev := m.up
	m.up = up
	m.lastRTT = time.Since(t0)
	m.mu.Unlock()
	if prev != up {
		if up {
			log.Printf("[edge] origin が UP になりました (%s)", m.cfg.origin.Host)
		} else {
			log.Printf("[edge] origin が DOWN しました（プロキシ要求は 502 を返します）")
		}
	}
	return up
}

func (m *originMonitor) loop(ctx context.Context) {
	// Go 1.20 までは起動直後に即 ping、以後 5 秒間隔
	m.pingOnce()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.pingOnce()
		}
	}
}

func (m *originMonitor) status() (bool, time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.up, m.lastRTT
}

/* -------------------------------------------------------------------- main */

func main() {
	cfg := loadConfig()
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Printf("[edge] Vandal Go edge 起動中… listen=%s:%d origin=%s", cfg.bind, cfg.port, cfg.origin)

	store := newAssetStore()
	puller := newPuller(cfg, store)
	monitor := newOriginMonitor(cfg)
	proxy := newOriginProxy(cfg)
	pool := newClientPool()
	pins := newPinStore(pool)
	stats := &coreStats{}
	rtEdge.pins = pins
	rtEdge.pool = pool
	rtEdge.stats = stats
	coreSrv := startCore(cfg, pins, pool, stats)
	boot := time.Now()

	// 初回取り込み: bootTimeout の間リトライ。揃わなくても起動は止めない
	// （未取得アセットは自動でプロキシ透過＝従来の Node 直配信と同一挙動）
	go func() {
		deadline := time.Now().Add(cfg.bootTimeout)
		for {
			missing := 0
			var wg sync.WaitGroup
			var mu sync.Mutex
			for _, p := range assetPaths {
				if store.get(p) != nil {
					continue
				}
				wg.Add(1)
				go func(p string) {
					defer wg.Done()
					if err := puller.pullOne(p); err != nil {
						mu.Lock()
						missing++
						mu.Unlock()
						if cfg.verbose {
							log.Printf("[edge] asset pull pending %s: %v", p, err)
						}
					}
				}(p)
			}
			wg.Wait()
			if missing == 0 {
				log.Printf("[edge] 静的アセット %d 件をメモリに載せました（gzip 事前圧縮済み, %dms）", store.count(), time.Since(boot).Milliseconds())
				return
			}
			if time.Now().After(deadline) {
				log.Printf("[edge] 一部アセット未取得（%d 件）— そのパスは origin プロキシで配信（機能同等）", missing)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// 周期再取り込み（自己修復用・デフォルト 5 分）
	if cfg.refreshEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.refreshEvery)
			defer t.Stop()
			for range t.C {
				refreshed := 0
				for _, p := range assetPaths {
					if err := puller.pullOne(p); err == nil {
						refreshed++
					}
				}
				if cfg.verbose {
					log.Printf("[edge] asset refresh: %d/%d", refreshed, len(assetPaths))
				}
			}
		}()
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__edge/healthz" {
			serveHealth(w, cfg, monitor, store, boot)
			return
		}
		// 動画ストリーム高速パス: ピン済みなら Node を経由せず googlevideo へ。
		// ピンが無い・raw=・403 などは false を返して従来どおり Node へ落とす。
		if tryServePinnedStream(w, r, pins, stats) {
			return
		}
		// 高速パスは安全な条件のときだけ:
		//   GET/HEAD である・Range 指定なし・アセットがメモリにある
		// それ以外はすべて origin へ素通し（= 従来の挙動と完全一致）
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) && r.Header.Get("Range") == "" {
			if a := store.get(r.URL.Path); a != nil {
				serveAsset(w, r, a, cfg)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:              net.JoinHostPort(cfg.bind, strconv.Itoa(cfg.port)),
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
		ReadTimeout:       0, // 動画リレー（長時間 GET）を切らない
		WriteTimeout:      0, // SSE・動画長時間応答のため無制限
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	monCtx, stopMon := context.WithCancel(context.Background())
	go monitor.loop(monCtx)

	go func() {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Printf("[edge] シャットダウン中…")
		stopMon()
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if coreSrv != nil {
			_ = coreSrv.Shutdown(ctx)
		}
		_ = srv.Shutdown(ctx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[edge] listen 失敗: %v", err)
	}
	log.Printf("[edge] 停止しました")
}

/* --------------------------------------------------------------- healthz */

func serveHealth(w http.ResponseWriter, cfg *config, mon *originMonitor, store *assetStore, boot time.Time) {
	up, rtt := mon.status()
	// 起動直後の猶予時間内は origin 未確立でも 200（プラットフォームの healthcheck が
	// 起動中の間だけ失敗してデプロイを中断しないようにする）
	ok := up || time.Since(boot) < cfg.healthGrace
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	payload := map[string]interface{}{
		"ok":          ok,
		"mode":        "go-edge",
		"origin":      cfg.origin.Scheme + "://" + cfg.origin.Host,
		"originUp":    up,
		"originRttMs": float64(rtt.Microseconds()) / 1000.0,
		"assets":      store.count(),
		"assetTotal":  len(assetPaths),
		"uptimeMs":    time.Since(boot).Milliseconds(),
	}
	if rtEdge.stats != nil {
		pinN := 0
		if rtEdge.pins != nil {
			pinN = rtEdge.pins.count()
		}
		payload["core"] = rtEdge.stats.snapshot(pinN, rtEdge.listen, rtEdge.up.Load())
	}
	_ = json.NewEncoder(w).Encode(payload)
}

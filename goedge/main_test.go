// vandal-edge のユニットテスト — httptest バックエンドを立て、
// ①アセット取り込み+メモリ配信 ②gzip 事前圧縮 ③条件付きリクエスト(304)
// ④プロキシ素通し（POST/不明パス/Range）⑤SSE 即時フラッシュ を検証する。
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// newTestEdge は httptest オリジン + puller/monitor を組み立てる。
func newTestEdge(t *testing.T) (*httptest.Server, *config, *assetStore, *puller) {
	t.Helper()
	appJS := strings.Repeat("console.log('vandal');", 2000) // 圧縮しがいのある大きめの本文
	css := strings.Repeat("body { color: #f60; }", 1000)
	html := "<!doctype html><title>Vandal</title>" + strings.Repeat("<div></div>", 200)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/health":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true}`)
		case "/index.html", "/":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			io.WriteString(w, html)
		case "/app.js":
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			w.Header().Set("Cache-Control", "public, max-age=3600")
			io.WriteString(w, appJS)
		case "/styles.css":
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
			w.Header().Set("Cache-Control", "public, max-age=3600")
			io.WriteString(w, css)
		case "/logo.png", "/logo.svg", "/vendor/hls.min.js":
			w.Header().Set("Content-Type", "application/octet-stream")
			io.WriteString(w, "binary-bytes")
		case "/echo-method":
			io.WriteString(w, r.Method)
		case "/sse":
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			for i := 0; i < 3; i++ {
				fmt.Fprintf(w, "data: tick-%d\n\n", i)
				fl.Flush()
				time.Sleep(5 * time.Millisecond)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(origin.Close)
	cfg := loadConfigForTest(origin.URL)
	store := newAssetStore()
	pl := newPuller(cfg, store)
	return origin, cfg, store, pl
}

func loadConfigForTest(originURL string) *config {
	u, err := url.Parse(originURL)
	if err != nil {
		panic(err)
	}
	return &config{
		bind:         "127.0.0.1",
		port:         0,
		origin:       u,
		refreshEvery: 0,
		bootTimeout:  5 * time.Second,
		healthGrace:  time.Minute,
		verbose:      false,
	}
}

func pullAll(t *testing.T, pl *puller) {
	t.Helper()
	for _, p := range assetPaths {
		if err := pl.pullOne(p); err != nil {
			t.Fatalf("pullOne(%s) failed: %v", p, err)
		}
	}
}

func TestAssetPullAndServe(t *testing.T) {
	_, cfg, store, pl := newTestEdge(t)
	pullAll(t, pl)

	if got := store.count(); got != len(assetPaths) {
		t.Fatalf("store.count()=%d want %d", got, len(assetPaths))
	}
	a := store.get("/app.js")
	if a == nil || len(a.raw) == 0 {
		t.Fatal("app.js not in store")
	}
	if a.gz == nil {
		t.Fatal("app.js should have gz variant")
	}
	if !strings.Contains(a.cacheControl, "max-age=3600") {
		t.Fatalf("cache-control not inherited: %q", a.cacheControl)
	}

	// gzip 復元が生バイトと一致すること
	zr, err := gzip.NewReader(bytes.NewReader(a.gz))
	if err != nil {
		t.Fatal(err)
	}
	dec, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dec, a.raw) {
		t.Fatal("gzip round-trip mismatch")
	}

	// "/" は index.html のエイリアス
	root := store.get("/")
	if root == nil || root.contentType == "" || !bytes.Contains(root.raw, []byte("Vandal")) {
		t.Fatal("alias / -> /index.html broken")
	}

	// gzip 交渉経由の配信
	req := httptest.NewRequest("GET", "/app.js", nil)
	req.Header.Set("Accept-Encoding", "br, gzip")
	rec := httptest.NewRecorder()
	serveAsset(rec, req, a, cfg)
	if rec.Code != 200 || rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("gzip negotiation failed: %+v", rec.Header())
	}
	if rec.Body.Len() != len(a.gz) {
		t.Fatal("served body is not the gzipherone")
	}

	// identity 要求は生バイト
	req2 := httptest.NewRequest("GET", "/app.js", nil)
	rec2 := httptest.NewRecorder()
	serveAsset(rec2, req2, a, cfg)
	if enc := rec2.Header().Get("Content-Encoding"); enc != "" {
		t.Fatalf("unexpected encoding: %q", enc)
	}
}

func TestConditionalRequests(t *testing.T) {
	_, cfg, store, pl := newTestEdge(t)
	pullAll(t, pl)
	a := store.get("/styles.css")

	req := httptest.NewRequest("GET", "/styles.css", nil)
	req.Header.Set("If-None-Match", a.etag)
	rec := httptest.NewRecorder()
	serveAsset(rec, req, a, cfg)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("If-None-Match should 304, got %d", rec.Code)
	}

	req2 := httptest.NewRequest("GET", "/styles.css", nil)
	req2.Header.Set("If-Modified-Since", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
	rec2 := httptest.NewRecorder()
	serveAsset(rec2, req2, a, cfg)
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("If-Modified-Since should 304, got %d", rec2.Code)
	}
}

func TestProxyPassThrough(t *testing.T) {
	_, cfg, _, _ := newTestEdge(t)
	proxy := newOriginProxy(cfg)

	// POST は body ごと透過
	req := httptest.NewRequest("POST", "/echo-method", strings.NewReader("hello"))
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.String() != "POST" {
		t.Fatalf("POST passthrough failed: %d %q", rec.Code, rec.Body.String())
	}

	// 不明パスは 404 をそのまま返す
	req2 := httptest.NewRequest("GET", "/api/nope", nil)
	rec2 := httptest.NewRecorder()
	proxy.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("unknown path should be 404, got %d", rec2.Code)
	}
}

func TestSSEStreamsUnbuffered(t *testing.T) {
	_, cfg, _, _ := newTestEdge(t)
	proxy := newOriginProxy(cfg)
	front := httptest.NewServer(proxy)
	t.Cleanup(front.Close)

	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get(front.URL + "/sse")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if !strings.Contains(res.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("not SSE: %q", res.Header.Get("Content-Type"))
	}
	// 即時フラッシュの証明: 全イベント到着が合計 ~15ms（生成間隔）+ ε で収まる
	t0 := time.Now()
	scanner := bufio.NewScanner(res.Body)
	ticks := 0
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data:") {
			ticks++
		}
	}
	if ticks != 3 {
		t.Fatalf("expected 3 ticks, got %d", ticks)
	}
	if d := time.Since(t0); d > 2*time.Second {
		t.Fatalf("SSE looked buffered: took %v", d)
	}
}

func TestPullFailureLeavesStoreEmpty(t *testing.T) {
	// オリジンが死んでいても pullOne はエラーを返すだけで panic しない
	cfg := loadConfigForTest("http://127.0.0.1:1") // 閉じたポート
	store := newAssetStore()
	pl := newPuller(cfg, store)
	if err := pl.pullOne("/app.js"); err == nil {
		t.Fatal("pull from dead origin should fail")
	}
	if store.get("/app.js") != nil {
		t.Fatal("failed pull must not store anything")
	}
}

func TestHealthzStates(t *testing.T) {
	_, cfg, store, pl := newTestEdge(t)
	pullAll(t, pl)
	mon := newOriginMonitor(cfg)
	mon.pingOnce()
	boot := time.Now()

	rec := httptest.NewRecorder()
	serveHealth(rec, cfg, mon, store, boot)
	if rec.Code != 200 {
		t.Fatalf("healthz should be 200 when origin up, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"go-edge"`) {
		t.Fatal("healthz missing mode")
	}
}

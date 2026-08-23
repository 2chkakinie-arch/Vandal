// Stream pin store + public /api/stream fast path.
//
// After Node resolves a video's googlevideo URL map it POSTs the map to
// the loopback core. Subsequent browser Range requests for that video are
// answered here — Go dials googlevideo directly (or via the pinned proxy)
// so the bytes never bounce through Node:
//
//	before: browser → Go edge → Node → undici → googlevideo
//	after:  browser → Go edge → googlevideo
//
// Safety:
//   - only /api/stream?v=&itag= (never ?raw= — Node validates those)
//   - URL host must be allow-listed
//   - 403/410 drops the pin and returns false so Node can refresh
//   - first ~768 KiB are warmed in RAM for sub-ms TTFB
package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	hotWarmBytes = 768 * 1024
	hotTTL       = 6 * time.Minute
	maxPins      = 2000
	maxHot       = 48
)

func allowLocal() bool { return os.Getenv("EDGE_ALLOW_LOCAL") == "1" }

type pinEntry struct {
	mapURLs  map[string]string
	proxyURL string
	exp      time.Time
}

type hotBuf struct {
	buf   []byte
	full  bool
	total int64
	exp   time.Time
}

type pinStore struct {
	mu   sync.RWMutex
	m    map[string]*pinEntry
	hot  map[string]*hotBuf // "videoId:itag"
	pool *clientPool
}

func newPinStore(pool *clientPool) *pinStore {
	return &pinStore{
		m:    make(map[string]*pinEntry),
		hot:  make(map[string]*hotBuf),
		pool: pool,
	}
}

func (s *pinStore) count() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	n := len(s.m)
	s.mu.RUnlock()
	return n
}

func (s *pinStore) get(id string) *pinEntry {
	if s == nil || id == "" {
		return nil
	}
	s.mu.RLock()
	e := s.m[id]
	s.mu.RUnlock()
	if e == nil {
		return nil
	}
	if !e.exp.IsZero() && time.Now().After(e.exp) {
		s.del(id)
		return nil
	}
	return e
}

func (s *pinStore) del(id string) {
	if s == nil || id == "" {
		return
	}
	s.mu.Lock()
	delete(s.m, id)
	for k := range s.hot {
		if strings.HasPrefix(k, id+":") {
			delete(s.hot, k)
		}
	}
	s.mu.Unlock()
}

func (s *pinStore) set(in pinIn, pool *clientPool) error {
	if s == nil {
		return errors.New("no store")
	}
	if !validVideoID(in.VideoID) {
		return errors.New("bad id")
	}
	if len(in.Map) == 0 {
		return errors.New("empty map")
	}
	clean := make(map[string]string, len(in.Map))
	for k, u := range in.Map {
		if k == "" || u == "" {
			continue
		}
		if !urlAllowed(u) {
			return errors.New("url host not allowed")
		}
		clean[k] = u
	}
	if len(clean) == 0 {
		return errors.New("no usable urls")
	}
	if !proxyAllowed(in.ProxyURL) {
		return errors.New("proxy not allowed")
	}
	exp := time.Now().Add(5 * time.Hour)
	if in.Exp > 0 {
		exp = time.UnixMilli(in.Exp)
		if time.Until(exp) < 30*time.Second {
			exp = time.Now().Add(5 * time.Minute)
		}
	}
	s.mu.Lock()
	s.m[in.VideoID] = &pinEntry{mapURLs: clean, proxyURL: strings.TrimSpace(in.ProxyURL), exp: exp}
	// bound memory: drop the oldest expired / arbitrary extra
	if len(s.m) > maxPins {
		now := time.Now()
		for k, e := range s.m {
			if now.After(e.exp) {
				delete(s.m, k)
			}
			if len(s.m) <= maxPins {
				break
			}
		}
		for k := range s.m {
			if len(s.m) <= maxPins {
				break
			}
			delete(s.m, k)
		}
	}
	s.mu.Unlock()

	warmItag := in.WarmItag
	if warmItag == "" {
		warmItag = "18"
	}
	if u := pickItag(clean, warmItag); u != "" {
		go s.warm(in.VideoID, warmItag, u, strings.TrimSpace(in.ProxyURL))
	}
	_ = pool
	return nil
}

func pickItag(m map[string]string, itag string) string {
	if u := m[itag]; u != "" {
		return u
	}
	if u := m["18"]; u != "" {
		return u
	}
	for _, u := range m {
		if u != "" {
			return u
		}
	}
	return ""
}

func (s *pinStore) hotKey(v, itag string) string { return v + ":" + itag }

func (s *pinStore) getHot(v, itag string) *hotBuf {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	h := s.hot[s.hotKey(v, itag)]
	s.mu.RUnlock()
	if h == nil || time.Now().After(h.exp) {
		return nil
	}
	return h
}

func (s *pinStore) warm(v, itag, rawURL, proxy string) {
	if s == nil || s.pool == nil || rawURL == "" {
		return
	}
	if existing := s.getHot(v, itag); existing != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip")
	req.Header.Set("Range", "bytes=0-"+strconv.Itoa(hotWarmBytes-1))
	req.Header.Set("Accept", "*/*")
	res, err := s.pool.doRelay(req, proxy)
	if err != nil {
		return
	}
	defer drainAndClose(res)
	if res.StatusCode >= 400 {
		return
	}
	var total int64
	if cr := res.Header.Get("Content-Range"); cr != "" {
		// bytes 0-786431/1234567
		if i := strings.LastIndex(cr, "/"); i >= 0 {
			if n, err := strconv.ParseInt(cr[i+1:], 10, 64); err == nil {
				total = n
			}
		}
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, int64(hotWarmBytes)))
	if err != nil || len(raw) < 64*1024 {
		return
	}
	h := &hotBuf{
		buf:   raw,
		total: total,
		full:  total > 0 && total <= int64(len(raw)),
		exp:   time.Now().Add(hotTTL),
	}
	s.mu.Lock()
	s.hot[s.hotKey(v, itag)] = h
	for len(s.hot) > maxHot {
		for k := range s.hot {
			delete(s.hot, k)
			break
		}
	}
	s.mu.Unlock()
}

// serveHot writes a Range (or full GET for fully-cached shorts) from RAM.
// Returns false when the request is not fully covered — caller must relay.
func (s *pinStore) serveHot(w http.ResponseWriter, r *http.Request, v, itag string) bool {
	h := s.getHot(v, itag)
	if h == nil {
		return false
	}
	rng := r.Header.Get("Range")
	if rng == "" {
		if !h.full {
			return false
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Length", strconv.Itoa(len(h.buf)))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("X-Hot-Cache", "FULL")
		w.Header().Set("X-Edge-Stream", "go")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(h.buf)
		}
		return true
	}
	start, end, ok := parseBytesRange(rng, int64(len(h.buf)))
	if !ok {
		return false
	}
	// explicit end must be fully covered (MSE segments cannot be truncated)
	if strings.Contains(rng, "-") {
		parts := strings.SplitN(strings.TrimPrefix(rng, "bytes="), "-", 2)
		if len(parts) == 2 && parts[1] != "" {
			wantEnd, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil || wantEnd > int64(len(h.buf)-1) {
				return false
			}
		}
	}
	if start < 0 || start >= int64(len(h.buf)) || end < start {
		return false
	}
	slice := h.buf[start : end+1]
	total := "*"
	if h.full {
		total = strconv.Itoa(len(h.buf))
	}
	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Length", strconv.Itoa(len(slice)))
	w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(start+int64(len(slice))-1, 10)+"/"+total)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Header().Set("X-Hot-Cache", "HIT")
	w.Header().Set("X-Edge-Stream", "go")
	w.WriteHeader(http.StatusPartialContent)
	if r.Method != http.MethodHead {
		_, _ = w.Write(slice)
	}
	return true
}

func parseBytesRange(h string, bufLen int64) (start, end int64, ok bool) {
	h = strings.TrimSpace(h)
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	// only a single range
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	if parts[0] == "" {
		return 0, 0, false
	}
	s, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false
	}
	if parts[1] == "" {
		return s, bufLen - 1, true
	}
	e, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || e < s {
		return 0, 0, false
	}
	if e > bufLen-1 {
		e = bufLen - 1
	}
	return s, e, true
}

// tryServePinnedStream is the public-edge fast path. Returns true when the
// response has been fully written. Returns false to fall through to Node
// (no pin, raw=, 403 needing refresh, etc.).
func tryServePinnedStream(w http.ResponseWriter, r *http.Request, pins *pinStore, stats *coreStats) bool {
	if pins == nil {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	if r.URL.Path != "/api/stream" {
		return false
	}
	q := r.URL.Query()
	if q.Get("raw") != "" {
		return false // HLS segments stay on Node (proxy-pool validation)
	}
	v := q.Get("v")
	if !validVideoID(v) {
		return false
	}
	itag := q.Get("itag")
	if itag == "" {
		itag = "18"
	}
	ent := pins.get(v)
	if ent == nil {
		return false
	}
	rawURL := pickItag(ent.mapURLs, itag)
	if rawURL == "" || !urlAllowed(rawURL) {
		return false
	}
	if pins.serveHot(w, r, v, itag) {
		return true
	}
	if pins.pool == nil {
		return false
	}

	ctx := r.Context()
	req, err := http.NewRequestWithContext(ctx, r.Method, rawURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "com.google.android.youtube/21.26.364 (Linux; U; Android 11) gzip")
	req.Header.Set("Accept", "*/*")
	if rng := r.Header.Get("Range"); rng != "" {
		req.Header.Set("Range", rng)
	}
	res, err := pins.pool.doRelay(req, ent.proxyURL)
	if err != nil {
		return false
	}
	// 403/410 = expired or IP-bound miss → drop pin, let Node refresh.
	if res.StatusCode == http.StatusForbidden || res.StatusCode == http.StatusGone {
		drainAndClose(res)
		pins.del(v)
		return false
	}
	if res.StatusCode >= 400 {
		drainAndClose(res)
		return false
	}
	defer res.Body.Close()

	pass := []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "Cache-Control"}
	for _, k := range pass {
		if v := res.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "private, max-age=3600")
	}
	w.Header().Set("X-Edge-Stream", "go")
	status := res.StatusCode
	if status != http.StatusPartialContent {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if stats != nil {
		stats.relays.Add(1)
	}
	if r.Method == http.MethodHead {
		return true
	}
	// 256 KiB copy buffer reduces Go-scheduler overhead and syscall churn
	// on long video relays versus the 32 KiB io.Copy default. It is a pure
	// byte pump: no buffering of a whole response, no caching, identical
	// bytes reach the browser (just with lower per-byte overhead).
	buf := relayBufPool.Get().(*[]byte)
	_, _ = io.CopyBuffer(w, res.Body, *buf)
	relayBufPool.Put(buf)
	return true
}

// relayBufPool reuses 256 KiB scratch buffers across pinned-stream relays so
// we don't allocate a fresh buffer per video request.
var relayBufPool = sync.Pool{New: func() interface{} { b := make([]byte, 256*1024); return &b }}

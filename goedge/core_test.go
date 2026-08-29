// Fetch-core + stream-pin unit tests. No external network.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testCore(t *testing.T) (*httptest.Server, *pinStore, *clientPool) {
	t.Helper()
	t.Setenv("EDGE_ALLOW_LOCAL", "1")
	pool := newClientPool()
	pins := newPinStore(pool)
	stats := &coreStats{}
	cfg := &config{coreBind: "127.0.0.1", corePort: 1, coreToken: ""}
	h := newCoreHandler(cfg, pins, pool, stats)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, pins, pool
}

func postJSON(t *testing.T, dest, body string) (int, map[string]interface{}) {
	t.Helper()
	res, err := http.Post(dest, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var m map[string]interface{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &m)
	}
	return res.StatusCode, m
}

func TestHedgeFirstAcceptableCancelsLosers(t *testing.T) {
	var slowHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fast":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true,"who":"fast"}`)
		case "/slow":
			atomic.AddInt32(&slowHits, 1)
			time.Sleep(250 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"ok":true,"who":"slow"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)

	core, _, _ := testCore(t)
	body := `{
		"kind":"json",
		"requests":[
			{"id":"slow","method":"GET","url":"` + upstream.URL + `/slow","timeoutMs":2000},
			{"id":"fast","method":"GET","url":"` + upstream.URL + `/fast","timeoutMs":2000}
		]
	}`
	t0 := time.Now()
	st, m := postJSON(t, core.URL+"/v1/hedge", body)
	if st != 200 {
		t.Fatalf("status %d", st)
	}
	if m["ok"] != true || m["winner"] != "fast" {
		t.Fatalf("want fast winner, got %#v", m)
	}
	if !strings.Contains(m["body"].(string), `"fast"`) {
		t.Fatalf("body %v", m["body"])
	}
	if d := time.Since(t0); d > 200*time.Millisecond {
		t.Fatalf("hedge looked serial: %v", d)
	}
}

func TestHedgeRejectsOpenProxy(t *testing.T) {
	core, _, _ := testCore(t)
	body := `{"requests":[{"id":"x","method":"GET","url":"https://evil.example/secret","timeoutMs":1000}]}`
	st, m := postJSON(t, core.URL+"/v1/hedge", body)
	if st != 200 {
		t.Fatalf("status %d", st)
	}
	if m["ok"] == true {
		t.Fatal("open-proxy fetch must not succeed")
	}
	atts, _ := m["attempts"].([]interface{})
	if len(atts) == 0 {
		t.Fatal("expected attempt record")
	}
}

func TestHedgePlayerKind(t *testing.T) {
	okBody := `{"playabilityStatus":{"status":"OK"},"streamingData":{"formats":[{"itag":18,"url":"https://x"}]}}`
	loginBody := `{"playabilityStatus":{"status":"LOGIN_REQUIRED","reason":"Sign in"}}`
	unplayBody := `{"playabilityStatus":{"status":"UNPLAYABLE","reason":"Private video"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/login":
			io.WriteString(w, loginBody)
		case "/unplay":
			io.WriteString(w, unplayBody)
		case "/ok":
			time.Sleep(30 * time.Millisecond)
			io.WriteString(w, okBody)
		default:
			io.WriteString(w, loginBody)
		}
	}))
	t.Cleanup(upstream.Close)
	core, _, _ := testCore(t)

	st, m := postJSON(t, core.URL+"/v1/hedge", `{
		"kind":"player",
		"requests":[
			{"id":"login","method":"GET","url":"`+upstream.URL+`/login","timeoutMs":2000},
			{"id":"ok","method":"GET","url":"`+upstream.URL+`/ok","timeoutMs":2000}
		]
	}`)
	if st != 200 || m["ok"] != true || m["winner"] != "ok" {
		t.Fatalf("player hedge want ok, got %#v", m)
	}

	st, m = postJSON(t, core.URL+"/v1/hedge", `{
		"kind":"player",
		"requests":[{"id":"u","method":"GET","url":"`+upstream.URL+`/unplay","timeoutMs":2000}]
	}`)
	if st != 200 || m["ok"] == true {
		t.Fatalf("UNPLAYABLE must not be accepted: %#v", m)
	}
	if int(m["status"].(float64)) != 451 {
		t.Fatalf("want 451, got %#v", m["status"])
	}
}

func TestProbeParallel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "" {
			t.Errorf("probe must send Range")
		}
		if r.URL.Path == "/ok" {
			w.WriteHeader(http.StatusPartialContent)
			w.Write([]byte("xxxxxxxxxxxxxxxx"))
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(upstream.Close)
	core, _, _ := testCore(t)
	st, m := postJSON(t, core.URL+"/v1/probe", `{
		"probes":[
			{"id":"a","url":"`+upstream.URL+`/ok","timeoutMs":2000},
			{"id":"b","url":"`+upstream.URL+`/no","timeoutMs":2000}
		]
	}`)
	if st != 200 {
		t.Fatalf("status %d", st)
	}
	arr, _ := m["results"].([]interface{})
	if len(arr) != 2 {
		t.Fatalf("results %#v", m)
	}
	got := map[string]bool{}
	for _, it := range arr {
		mm := it.(map[string]interface{})
		got[mm["id"].(string)] = mm["ok"] == true
	}
	if !got["a"] || got["b"] {
		t.Fatalf("probe results %#v", got)
	}
}

func TestPinRejectsEvilHost(t *testing.T) {
	core, pins, _ := testCore(t)
	st, _ := postJSON(t, core.URL+"/v1/pin", `{
		"videoId":"dQw4w9WgXcQ",
		"map":{"18":"https://evil.example/videoplayback"}
	}`)
	if st != 400 {
		t.Fatalf("evil pin must 400, got %d", st)
	}
	if pins.get("dQw4w9WgXcQ") != nil {
		t.Fatal("evil url was stored")
	}
}

func TestPinAndHotAndRelay(t *testing.T) {
	payload := bytes.Repeat([]byte("A"), 128*1024)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Accept-Ranges", "bytes")
		n := strconv.Itoa(len(payload))
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			w.Header().Set("Content-Range", "bytes 0-"+strconv.Itoa(len(payload)-1)+"/"+n)
			w.Header().Set("Content-Length", n)
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload)
			return
		}
		w.Header().Set("Content-Length", n)
		w.Write(payload)
	}))
	t.Cleanup(upstream.Close)

	core, pins, _ := testCore(t)
	st, m := postJSON(t, core.URL+"/v1/pin", `{
		"videoId":"dQw4w9WgXcQ",
		"map":{"18":"`+upstream.URL+`/videoplayback"},
		"warmItag":"18"
	}`)
	if st != 200 || m["ok"] != true {
		t.Fatalf("pin failed %d %#v", st, m)
	}
	if pins.get("dQw4w9WgXcQ") == nil {
		t.Fatal("pin not stored")
	}

	deadline := time.Now().Add(2 * time.Second)
	for pins.getHot("dQw4w9WgXcQ", "18") == nil {
		if time.Now().After(deadline) {
			t.Fatal("warm did not land")
		}
		time.Sleep(20 * time.Millisecond)
	}

	req := httptest.NewRequest("GET", "/api/stream?v=dQw4w9WgXcQ&itag=18", nil)
	req.Header.Set("Range", "bytes=0-1023")
	rec := httptest.NewRecorder()
	if !tryServePinnedStream(rec, req, pins, &coreStats{}) {
		t.Fatal("hot path should serve")
	}
	if rec.Code != 206 || rec.Header().Get("X-Edge-Stream") != "go" {
		t.Fatalf("hot serve: %d %v", rec.Code, rec.Header())
	}
	if rec.Body.Len() != 1024 {
		t.Fatalf("got %d bytes", rec.Body.Len())
	}

	req2 := httptest.NewRequest("GET", "/api/stream?v=abcdefghijk&itag=18", nil)
	rec2 := httptest.NewRecorder()
	if tryServePinnedStream(rec2, req2, pins, &coreStats{}) {
		t.Fatal("unknown video must fall through")
	}

	req3 := httptest.NewRequest("GET", "/api/stream?v=dQw4w9WgXcQ&raw="+url.QueryEscape(upstream.URL+"/x"), nil)
	rec3 := httptest.NewRecorder()
	if tryServePinnedStream(rec3, req3, pins, &coreStats{}) {
		t.Fatal("raw= must fall through to Node")
	}
}

func TestCoreAuthToken(t *testing.T) {
	t.Setenv("EDGE_ALLOW_LOCAL", "1")
	pool := newClientPool()
	pins := newPinStore(pool)
	cfg := &config{coreToken: "s3cret"}
	srv := httptest.NewServer(newCoreHandler(cfg, pins, pool, &coreStats{}))
	t.Cleanup(srv.Close)

	res, err := http.Post(srv.URL+"/v1/hedge", "application/json", strings.NewReader(`{"requests":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("missing token should 403, got %d", res.StatusCode)
	}

	req, _ := http.NewRequest("POST", srv.URL+"/v1/hedge", strings.NewReader(`{"requests":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vandal-Core", "s3cret")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("good token should 200, got %d", res.StatusCode)
	}
}

func TestInspectPlayer(t *testing.T) {
	ok, meta, _ := inspectPlayer([]byte(`{"playabilityStatus":{"status":"OK"},"streamingData":{"formats":[{}]}}`))
	if !ok || meta.status != "OK" {
		t.Fatalf("ok formats: %v %#v", ok, meta)
	}
	ok, meta, msg := inspectPlayer([]byte(`{"playabilityStatus":{"status":"LOGIN_REQUIRED"}}`))
	if ok || meta.status != "LOGIN_REQUIRED" || msg == "" {
		t.Fatalf("login: %v %#v %s", ok, meta, msg)
	}
	ok, _, _ = inspectPlayer([]byte(`{"playabilityStatus":{"status":"OK"},"streamingData":{"hlsManifestUrl":"https://x"}}`))
	if !ok {
		t.Fatal("hls should accept")
	}
}

func TestHostAllowList(t *testing.T) {
	if !hostAllowed("rr1---sn-abc.googlevideo.com") {
		t.Fatal("googlevideo")
	}
	if !hostAllowed("www.youtube.com") {
		t.Fatal("youtube")
	}
	if hostAllowed("evil.example") {
		t.Fatal("evil must be denied")
	}
	t.Setenv("EDGE_ALLOW_LOCAL", "")
	if hostAllowed("127.0.0.1") {
		t.Fatal("loopback denied without flag")
	}
	t.Setenv("EDGE_ALLOW_LOCAL", "1")
	if !hostAllowed("127.0.0.1") {
		t.Fatal("loopback allowed with flag")
	}
}

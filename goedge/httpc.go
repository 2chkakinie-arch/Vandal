// HTTP client pool for the fetch core.
//
// Direct egress reuses a single Transport (HTTP/2 + keep-alive) so InnerTube
// POSTs after the first one skip the TCP/TLS handshake. Each HTTP proxy gets
// its own Transport (CONNECT cannot safely share the direct pool; most free
// proxies also lack H2).
//
// Standard library only — no extra modules.
package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	maxHedge        = 8
	maxBodyBytes    = 4 << 20 // 4 MiB — InnerTube JSON; anything bigger is a bug
	defaultTimeout  = 9 * time.Second
	dialTimeout     = 4 * time.Second
	tlsTimeout      = 5 * time.Second
	idleConnTimeout = 90 * time.Second
)

type clientPool struct {
	mu            sync.Mutex
	transports    map[string]*http.Transport
	relayTrans    map[string]*http.Transport
}

func newClientPool() *clientPool {
	return &clientPool{
		transports: make(map[string]*http.Transport),
		relayTrans: make(map[string]*http.Transport),
	}
}

func (p *clientPool) transport(proxy string) *http.Transport {
	return p.transportWith(proxy, false)
}

// relayTransport returns an HTTP transport tuned for long-running video byte
// relays: a dialer that raises SO_RCVBUF on the googlevideo TCP socket, and
// larger HTTP/2 stream read/write windows so high-bandwidth paths do not
// stall on flow-control. It is separate from the metadata transport so a
// misconfiguration can never affect InnerTube/JSON calls.
func (p *clientPool) relayTransport(proxy string) *http.Transport {
	return p.transportWith(proxy, true)
}

func (p *clientPool) transportWith(proxy string, relay bool) *http.Transport {
	key := proxy
	if key == "" {
		key = "direct"
	}
	store := p.transports
	if relay {
		store = p.relayTrans
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := store[key]; ok {
		return t
	}
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}
	dialCtx := dialer.DialContext
	if relay {
		// Wraps the default dialer to bump the kernel receive window on each
		// accepted googlevideo TCP connection (no-op on non-Linux targets).
		dialCtx = func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if err == nil {
				setRelaySockopts(c)
			}
			return c, err
		}
	}
	t := &http.Transport{
		DialContext:           dialCtx,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0, // per-request context owns the deadline
		DisableCompression:    true,
		ForceAttemptHTTP2:     proxy == "",
	}
	if relay {
		// Larger HTTP/2 flow-control windows keep the CDN pipe saturated on
		// high-BDP googlevideo links. Safe zero-value defaults for HTTP/1.
		// (Transport.ReadBufferSize/WriteBufferSize exist since Go 1.13;
		// go.mod requires 1.20+.)
		t.ReadBufferSize = 512 * 1024
		t.WriteBufferSize = 512 * 1024
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil && u.Host != "" {
			t.Proxy = http.ProxyURL(u)
		}
	}
	store[key] = t
	return t
}

func (p *clientPool) client(proxy string) *http.Client {
	return &http.Client{
		Transport: p.transport(proxy),
		// Follow a couple of redirects (YouTube occasionally 302s) but never
		// loop. Stream relay uses the same client; googlevideo Range 206 does
		// not redirect so this is cheap.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// relayClient returns an HTTP client whose transport is tuned for streaming
// video bytes: bumped SO_RCVBUF, larger HTTP/2 windows, and no body-size
// cap. It is intentionally separate from client() so relay tuning can never
// affect the InnerTube/JSON transport used for metadata.
func (p *clientPool) relayClient(proxy string) *http.Client {
	return &http.Client{
		Transport: p.relayTransport(proxy),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// do is a thin wrapper so callers can pass a context deadline without
// mutating the shared Transport.
func (p *clientPool) do(req *http.Request, proxy string) (*http.Response, error) {
	return p.client(proxy).Do(req)
}

// doRelay issues a request on the streaming-tuned transport. Used by the
// pinned-stream fast path so metadata requests and video byte relays never
// contend for the same connection pool settings.
func (p *clientPool) doRelay(req *http.Request, proxy string) (*http.Response, error) {
	return p.relayClient(proxy).Do(req)
}

func clampTimeout(ms int) time.Duration {
	if ms <= 0 {
		return defaultTimeout
	}
	if ms < 200 {
		return 200 * time.Millisecond
	}
	if ms > 60_000 {
		return 60 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

func drainAndClose(res *http.Response) {
	if res == nil || res.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
	_ = res.Body.Close()
}

func ctxWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, d)
}

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
	mu         sync.Mutex
	transports map[string]*http.Transport
}

func newClientPool() *clientPool {
	return &clientPool{transports: make(map[string]*http.Transport)}
}

func (p *clientPool) transport(proxy string) *http.Transport {
	key := proxy
	if key == "" {
		key = "direct"
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.transports[key]; ok {
		return t
	}
	t := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0, // per-request context owns the deadline
		DisableCompression:    true,
		ForceAttemptHTTP2:     proxy == "",
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil && u.Host != "" {
			t.Proxy = http.ProxyURL(u)
		}
	}
	p.transports[key] = t
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

// do is a thin wrapper so callers can pass a context deadline without
// mutating the shared Transport.
func (p *clientPool) do(req *http.Request, proxy string) (*http.Response, error) {
	return p.client(proxy).Do(req)
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

//go:build linux

package main

import "net"

// setRelaySockopts enlarges the kernel receive buffer on a TCP connection to
// googlevideo. A bigger SO_RCVBUF raises the TCP receive window, which on
// high-bandwidth / high-RTT paths (typical for googlevideo CDN nodes) lets the
// sender keep the pipe full instead of being limited by the default ~128 KiB
// window. Failures are non-fatal: the socket keeps its system default.
func setRelaySockopts(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(1 << 20) // 1 MiB
	}
}

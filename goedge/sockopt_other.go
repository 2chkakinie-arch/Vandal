//go:build !linux

package main

import "net"

// setRelaySockopts is a no-op on non-Linux targets. The standard library's
// SetReadBuffer maps to SO_RCVBUF only where supported; on other platforms
// the kernel defaults are kept as-is rather than risking build failures.
func setRelaySockopts(_ net.Conn) {}

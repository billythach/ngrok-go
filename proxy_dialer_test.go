package ngrok

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialerFromProxyURLHTTPProxy(t *testing.T) {
	target := startTCPEchoServer(t, "pong")

	authHeaderCh := make(chan string, 1)
	proxyAddr := startHTTPConnectProxy(t, authHeaderCh)

	proxyURL, err := url.Parse("http://user:pass@" + proxyAddr)
	require.NoError(t, err)

	dialer, err := dialerFromProxyURL(proxyURL, &net.Dialer{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", target)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)

	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "pong", string(buf))

	select {
	case h := <-authHeaderCh:
		assert.Equal(t, "Basic dXNlcjpwYXNz", h)
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not receive CONNECT request")
	}
}

func TestDialerFromProxyURLUnknownScheme(t *testing.T) {
	proxyURL, err := url.Parse("ftp://proxy.example.com:21")
	require.NoError(t, err)

	_, err = dialerFromProxyURL(proxyURL, &net.Dialer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown scheme")
}

func startTCPEchoServer(t *testing.T, response string) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close() //nolint:errcheck
				buf := make([]byte, 4)
				_, _ = io.ReadFull(c, buf)
				_, _ = c.Write([]byte(response))
			}(conn)
		}
	}()

	return ln.Addr().String()
}

func startHTTPConnectProxy(t *testing.T, authHeaderCh chan<- string) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close() //nolint:errcheck

				br := bufio.NewReader(c)
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				if req.Method != http.MethodConnect {
					_, _ = fmt.Fprint(c, "HTTP/1.1 405 Method Not Allowed\r\n\r\n")
					return
				}

				authHeaderCh <- req.Header.Get("Proxy-Authorization")

				targetConn, err := net.Dial("tcp", req.Host)
				if err != nil {
					_, _ = fmt.Fprint(c, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
					return
				}
				defer targetConn.Close() //nolint:errcheck

				_, _ = fmt.Fprint(c, "HTTP/1.1 200 Connection Established\r\n\r\n")

				go func() {
					_, _ = io.Copy(targetConn, br)
					_ = targetConn.Close()
				}()
				_, _ = io.Copy(c, targetConn)
			}(clientConn)
		}
	}()

	return ln.Addr().String()
}

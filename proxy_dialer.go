package ngrok

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	xproxy "golang.org/x/net/proxy"
)

func dialerFromProxyURL(proxyURL *url.URL, forward Dialer) (Dialer, error) {
	switch strings.ToLower(proxyURL.Scheme) {
	case "http", "https":
		return newHTTPConnectProxyDialer(proxyURL, forward), nil
	default:
		proxyDialer, err := xproxy.FromURL(proxyURL, forward)
		if err != nil {
			return nil, err
		}
		dialer, ok := proxyDialer.(Dialer)
		if !ok {
			return nil, fmt.Errorf("proxy dialer is not compatible with ngrok Dialer interface")
		}
		return dialer, nil
	}
}

type httpConnectProxyDialer struct {
	proxyAddr string
	useTLS    bool
	forward   Dialer
	auth      string
}

func newHTTPConnectProxyDialer(proxyURL *url.URL, forward Dialer) Dialer {
	d := &httpConnectProxyDialer{
		proxyAddr: proxyAddr(proxyURL),
		useTLS:    strings.EqualFold(proxyURL.Scheme, "https"),
		forward:   forward,
	}
	if user := proxyURL.User; user != nil {
		username := user.Username()
		password, _ := user.Password()
		token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		d.auth = "Basic " + token
	}
	return d
}

func (d *httpConnectProxyDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d *httpConnectProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" {
		return nil, fmt.Errorf("unsupported network %q for HTTP proxy dialer", network)
	}

	conn, err := d.forward.DialContext(ctx, "tcp", d.proxyAddr)
	if err != nil {
		return nil, err
	}

	if d.useTLS {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: hostOnly(d.proxyAddr), MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = tlsConn.Close()
			return nil, err
		}
		conn = tlsConn
	}

	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})
	defer stop()

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: make(http.Header),
	}
	if d.auth != "" {
		req.Header.Set("Proxy-Authorization", d.auth)
	}
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("proxy CONNECT failed: %s", resp.Status)
	}

	_ = resp.Body.Close()
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func proxyAddr(proxyURL *url.URL) string {
	if port := proxyURL.Port(); port != "" {
		return net.JoinHostPort(proxyURL.Hostname(), port)
	}
	if strings.EqualFold(proxyURL.Scheme, "https") {
		return net.JoinHostPort(proxyURL.Hostname(), "443")
	}
	return net.JoinHostPort(proxyURL.Hostname(), "80")
}

func hostOnly(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

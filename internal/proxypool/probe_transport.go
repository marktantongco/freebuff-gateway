package proxypool

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"freebuff-reverse/internal/channels"

	"golang.org/x/net/proxy"
)

const defaultProbeBodyPreviewBytes = 4096

type ProbeTransport struct {
	BodyPreviewBytes int
}

func NewProbeTransport() *ProbeTransport {
	return &ProbeTransport{BodyPreviewBytes: defaultProbeBodyPreviewBytes}
}

func (t *ProbeTransport) Do(ctx context.Context, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("proxypool: nil probe request")
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(probeCtx, method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	httpReq.Header = cloneProbeHeaders(req.Headers)

	transport, err := newProbeHTTPTransport(req.TransportProfile, timeout)
	if err != nil {
		return nil, err
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	started := time.Now()
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	previewBytes := t.BodyPreviewBytes
	if previewBytes <= 0 {
		previewBytes = defaultProbeBodyPreviewBytes
	}
	preview := body
	if len(preview) > previewBytes {
		preview = preview[:previewBytes]
	}
	return &channels.OutboundResponse{
		Status:          resp.StatusCode,
		Headers:         cloneProbeHeaders(resp.Header),
		Body:            body,
		BodyPreview:     preview,
		FirstResponseMS: time.Since(started).Milliseconds(),
	}, nil
}

func newProbeHTTPTransport(profile channels.TransportProfile, timeout time.Duration) (*http.Transport, error) {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: time.Second,
		IdleConnTimeout:       5 * time.Second,
		ForceAttemptHTTP2:     !profile.ForceHTTP1,
	}
	if profile.InsecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	proxyURL := strings.TrimSpace(profile.ProxyURL)
	if proxyURL == "" {
		return transport, nil
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("proxy url invalid: %w", err)
	}
	switch parsed.Scheme {
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
	case "socks5", "socks5h":
		auth := probeProxyAuth(parsed)
		socksDialer, err := proxy.SOCKS5("tcp", parsed.Host, auth, dialer)
		if err != nil {
			return nil, err
		}
		if contextDialer, ok := socksDialer.(proxy.ContextDialer); ok {
			transport.DialContext = contextDialer.DialContext
		} else {
			transport.DialContext = func(_ context.Context, network, address string) (net.Conn, error) {
				return socksDialer.Dial(network, address)
			}
		}
		transport.Proxy = nil
	default:
		return nil, fmt.Errorf("proxy scheme must be http, https, socks5 or socks5h")
	}
	return transport, nil
}

func probeProxyAuth(parsed *url.URL) *proxy.Auth {
	if parsed == nil || parsed.User == nil {
		return nil
	}
	password, _ := parsed.User.Password()
	return &proxy.Auth{
		User:     parsed.User.Username(),
		Password: password,
	}
}

func cloneProbeHeaders(headers http.Header) http.Header {
	out := make(http.Header, len(headers))
	for key, values := range headers {
		copied := make([]string, len(values))
		copy(copied, values)
		out[key] = copied
	}
	return out
}

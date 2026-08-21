package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"sync"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

const (
	defaultBodyPreviewBytes = 4096
	defaultTLSClientProfile = "chrome_146"
)

type Client struct {
	timeout          time.Duration
	bodyPreviewBytes int
	transportProfile channels.TransportProfile
	requestReuse     bool
}

type Option func(*Client)

func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		c.timeout = d
	}
}

func WithBodyPreviewBytes(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.bodyPreviewBytes = n
		}
	}
}

func WithTransportProfile(profile channels.TransportProfile) Option {
	return func(c *Client) {
		c.transportProfile = mergeTransportProfile(c.transportProfile, profile)
	}
}

func WithRequestReuse(enabled bool) Option {
	return func(c *Client) {
		c.requestReuse = enabled
	}
}

func New(opts ...Option) *Client {
	c := &Client{
		timeout:          60 * time.Second,
		bodyPreviewBytes: defaultBodyPreviewBytes,
		transportProfile: channels.TransportProfile{
			TLSClientProfile:        defaultTLSClientProfile,
			RandomTLSExtensionOrder: true,
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

type requestScopeKey struct{}

type requestScope struct {
	mu      sync.Mutex
	clients map[string]tlsclient.HttpClient
	closed  bool
	once    sync.Once
}

func (c *Client) WithRequestScope(ctx context.Context) (context.Context, func()) {
	if !c.requestReuse || ctx == nil {
		return ctx, func() {}
	}
	if _, ok := ctx.Value(requestScopeKey{}).(*requestScope); ok {
		return ctx, func() {}
	}
	scope := &requestScope{clients: make(map[string]tlsclient.HttpClient)}
	closeScope := scope.close
	go func() {
		<-ctx.Done()
		closeScope()
	}()
	return context.WithValue(ctx, requestScopeKey{}, scope), closeScope
}

func (c *Client) Do(ctx context.Context, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	started := time.Now()
	resp, client, cancel, closeIdleOnClose, err := c.execute(ctx, req, c.timeout)
	if err != nil {
		return nil, err
	}
	firstResponseMS := time.Since(started).Milliseconds()
	body, err := responseBodyOrEmpty(resp, cancel, client, closeIdleOnClose)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	preview := bodyBytes
	if len(preview) > c.bodyPreviewBytes {
		preview = preview[:c.bodyPreviewBytes]
	}
	return &channels.OutboundResponse{
		Status:          resp.StatusCode,
		Headers:         cloneResponseHeaders(resp.Header),
		Body:            bodyBytes,
		BodyPreview:     preview,
		FirstResponseMS: firstResponseMS,
	}, nil
}

func methodOrDefault(method string) string {
	if method == "" {
		return nethttp.MethodGet
	}
	return method
}

func (c *Client) DoStream(ctx context.Context, req *channels.OutboundRequest) (*channels.OutboundStreamResponse, error) {
	started := time.Now()
	resp, client, cancel, closeIdleOnClose, err := c.execute(ctx, req, 0)
	if err != nil {
		return nil, err
	}
	firstResponseMS := time.Since(started).Milliseconds()
	body, err := responseBodyOrEmpty(resp, cancel, client, closeIdleOnClose)
	if err != nil {
		return nil, err
	}
	headers := cloneResponseHeaders(resp.Header)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview, readErr := io.ReadAll(io.LimitReader(body, int64(c.bodyPreviewBytes)))
		closeErr := body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		return &channels.OutboundStreamResponse{
			Status:          resp.StatusCode,
			Headers:         headers,
			Body:            io.NopCloser(bytes.NewReader(preview)),
			BodyPreview:     preview,
			FirstResponseMS: firstResponseMS,
		}, nil
	}
	return &channels.OutboundStreamResponse{
		Status:          resp.StatusCode,
		Headers:         headers,
		Body:            body,
		FirstResponseMS: firstResponseMS,
	}, nil
}

func (c *Client) execute(ctx context.Context, req *channels.OutboundRequest, defaultTimeout time.Duration) (*fhttp.Response, tlsclient.HttpClient, context.CancelFunc, bool, error) {
	httpReq, cancel, err := c.buildHTTPRequest(ctx, req, defaultTimeout)
	if err != nil {
		return nil, nil, nil, false, err
	}
	profile := mergeTransportProfile(c.transportProfile, req.TransportProfile)
	client, reused, key, err := c.clientForProfile(ctx, profile)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, false, err
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if reused {
			closeScopedProfileClient(ctx, key, client)
		} else {
			client.CloseIdleConnections()
		}
		return nil, nil, nil, false, err
	}
	return resp, client, cancel, !reused, nil
}

func (c *Client) clientForProfile(ctx context.Context, profile channels.TransportProfile) (tlsclient.HttpClient, bool, string, error) {
	key := profileCacheKey(profile)
	scope, _ := ctx.Value(requestScopeKey{}).(*requestScope)
	if scope == nil || key == "" {
		client, err := newTLSClient(profile)
		return client, false, "", err
	}
	client, err := scope.client(key, profile)
	if err != nil {
		return nil, false, "", err
	}
	return client, true, key, nil
}

func profileCacheKey(profile channels.TransportProfile) string {
	if strings.TrimSpace(profile.ReuseKey) == "" {
		return ""
	}
	profileName := strings.TrimSpace(profile.TLSClientProfile)
	if profileName == "" {
		profileName = defaultTLSClientProfile
	}
	return fmt.Sprintf(
		"%s|tls=%s|proxy=%s|h1=%t|no_h3=%t|race=%t|rand=%t|skip=%t|no_v4=%t|no_v6=%t",
		profile.ReuseKey,
		profileName,
		profile.ProxyURL,
		profile.ForceHTTP1,
		profile.DisableHTTP3,
		profile.ProtocolRacing,
		profile.RandomTLSExtensionOrder,
		profile.InsecureSkipVerify,
		profile.DisableIPv4,
		profile.DisableIPv6,
	)
}

func (s *requestScope) client(key string, profile channels.TransportProfile) (tlsclient.HttpClient, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("transport: request scope is closed")
	}
	if client := s.clients[key]; client != nil {
		s.mu.Unlock()
		return client, nil
	}
	s.mu.Unlock()

	client, err := newTLSClient(profile)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		client.CloseIdleConnections()
		return nil, fmt.Errorf("transport: request scope is closed")
	}
	if existing := s.clients[key]; existing != nil {
		client.CloseIdleConnections()
		return existing, nil
	}
	s.clients[key] = client
	return client, nil
}

func (s *requestScope) close() {
	s.once.Do(func() {
		s.mu.Lock()
		clients := make([]tlsclient.HttpClient, 0, len(s.clients))
		for key, client := range s.clients {
			clients = append(clients, client)
			delete(s.clients, key)
		}
		s.closed = true
		s.mu.Unlock()
		for _, client := range clients {
			client.CloseIdleConnections()
		}
	})
}

func closeScopedProfileClient(ctx context.Context, key string, client tlsclient.HttpClient) {
	if client == nil {
		return
	}
	scope, _ := ctx.Value(requestScopeKey{}).(*requestScope)
	if scope == nil || key == "" {
		client.CloseIdleConnections()
		return
	}
	scope.mu.Lock()
	if scope.clients[key] == client {
		delete(scope.clients, key)
	}
	scope.mu.Unlock()
	client.CloseIdleConnections()
}

func (c *Client) buildHTTPRequest(ctx context.Context, req *channels.OutboundRequest, defaultTimeout time.Duration) (*fhttp.Request, context.CancelFunc, error) {
	if req == nil {
		return nil, nil, fmt.Errorf("transport: nil request")
	}
	if req.URL == "" {
		return nil, nil, fmt.Errorf("transport: empty url")
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	cancel := context.CancelFunc(nil)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}
	httpReq, err := fhttp.NewRequestWithContext(ctx, methodOrDefault(req.Method), req.URL, body)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, err
	}
	httpReq.Header = requestHeaders(req.Headers, mergeTransportProfile(c.transportProfile, req.TransportProfile))
	return httpReq, cancel, nil
}

func newTLSClient(profile channels.TransportProfile) (tlsclient.HttpClient, error) {
	profileName := strings.TrimSpace(profile.TLSClientProfile)
	if profileName == "" {
		profileName = defaultTLSClientProfile
	}
	clientProfile, ok := profiles.MappedTLSClients[profileName]
	if !ok {
		return nil, fmt.Errorf("transport: unknown tls client profile %q", profileName)
	}
	options := []tlsclient.HttpClientOption{
		tlsclient.WithClientProfile(clientProfile),
		tlsclient.WithTimeoutMilliseconds(0),
		tlsclient.WithNotFollowRedirects(),
		tlsclient.WithCatchPanics(),
	}
	if profile.ProxyURL != "" {
		options = append(options, tlsclient.WithProxyUrl(profile.ProxyURL))
	}
	if profile.ForceHTTP1 {
		options = append(options, tlsclient.WithForceHttp1())
	}
	if profile.DisableHTTP3 {
		options = append(options, tlsclient.WithDisableHttp3())
	}
	if profile.ProtocolRacing {
		options = append(options, tlsclient.WithProtocolRacing())
	}
	if profile.RandomTLSExtensionOrder {
		options = append(options, tlsclient.WithRandomTLSExtensionOrder())
	}
	if profile.InsecureSkipVerify {
		options = append(options, tlsclient.WithInsecureSkipVerify())
	}
	if profile.DisableIPv4 {
		options = append(options, tlsclient.WithDisableIPV4())
	}
	if profile.DisableIPv6 {
		options = append(options, tlsclient.WithDisableIPV6())
	}
	return tlsclient.NewHttpClient(nil, options...)
}

func requestHeaders(headers nethttp.Header, profile channels.TransportProfile) fhttp.Header {
	out := fhttp.Header{}
	for name, values := range headers {
		for _, value := range values {
			out.Add(name, value)
		}
	}
	if len(profile.HeaderOrder) > 0 {
		out[fhttp.HeaderOrderKey] = append([]string(nil), profile.HeaderOrder...)
	}
	if len(profile.PseudoHeaderOrder) > 0 {
		out[fhttp.PHeaderOrderKey] = append([]string(nil), profile.PseudoHeaderOrder...)
	}
	return out
}

func cloneResponseHeaders(headers fhttp.Header) nethttp.Header {
	out := nethttp.Header{}
	for name, values := range headers {
		if name == fhttp.HeaderOrderKey || name == fhttp.PHeaderOrderKey {
			continue
		}
		out[name] = append([]string(nil), values...)
	}
	return out
}

func mergeTransportProfile(base, override channels.TransportProfile) channels.TransportProfile {
	out := base
	if override.TLSClientProfile != "" {
		out.TLSClientProfile = override.TLSClientProfile
	}
	if override.ReuseKey != "" {
		out.ReuseKey = override.ReuseKey
	}
	if override.ProxyURL != "" {
		out.ProxyURL = override.ProxyURL
	}
	if len(override.HeaderOrder) > 0 {
		out.HeaderOrder = append([]string(nil), override.HeaderOrder...)
	}
	if len(override.PseudoHeaderOrder) > 0 {
		out.PseudoHeaderOrder = append([]string(nil), override.PseudoHeaderOrder...)
	}
	out.ForceHTTP1 = out.ForceHTTP1 || override.ForceHTTP1
	out.DisableHTTP3 = out.DisableHTTP3 || override.DisableHTTP3
	out.ProtocolRacing = out.ProtocolRacing || override.ProtocolRacing
	out.RandomTLSExtensionOrder = out.RandomTLSExtensionOrder || override.RandomTLSExtensionOrder
	out.InsecureSkipVerify = out.InsecureSkipVerify || override.InsecureSkipVerify
	out.DisableIPv4 = out.DisableIPv4 || override.DisableIPv4
	out.DisableIPv6 = out.DisableIPv6 || override.DisableIPv6
	return out
}

type responseReadCloser struct {
	io.ReadCloser
	cancel           context.CancelFunc
	client           tlsclient.HttpClient
	closeIdleOnClose bool
}

func responseBodyOrEmpty(resp *fhttp.Response, cancel context.CancelFunc, client tlsclient.HttpClient, closeIdleOnClose bool) (responseReadCloser, error) {
	if resp == nil {
		closeResponseResources(cancel, client, closeIdleOnClose)
		return responseReadCloser{}, fmt.Errorf("transport: nil response")
	}
	body := resp.Body
	if body == nil {
		body = io.NopCloser(bytes.NewReader(nil))
	}
	return responseReadCloser{ReadCloser: body, cancel: cancel, client: client, closeIdleOnClose: closeIdleOnClose}, nil
}

func closeResponseResources(cancel context.CancelFunc, client tlsclient.HttpClient, closeIdleOnClose bool) {
	if cancel != nil {
		cancel()
	}
	if client != nil && closeIdleOnClose {
		client.CloseIdleConnections()
	}
}

func (r responseReadCloser) Close() error {
	var err error
	if r.ReadCloser != nil {
		err = r.ReadCloser.Close()
	}
	closeResponseResources(r.cancel, r.client, r.closeIdleOnClose)
	return err
}

package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"freebuff-reverse/internal/channels"

	fhttp "github.com/bogdanfinn/fhttp"
)

func testTransportProfile() channels.TransportProfile {
	return channels.TransportProfile{
		TLSClientProfile:        "chrome_146",
		ForceHTTP1:              true,
		DisableHTTP3:            true,
		RandomTLSExtensionOrder: true,
		InsecureSkipVerify:      true,
	}
}

func TestClientDoStreamReturnsBeforeBodyCompletes(t *testing.T) {
	headersSent := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(headersSent)
		<-releaseBody
		_, _ = io.WriteString(w, "data: done\n\n")
	}))
	defer server.Close()
	defer func() {
		releaseOnce.Do(func() {
			close(releaseBody)
		})
	}()

	client := New(WithTransportProfile(testTransportProfile()))
	result := make(chan *channels.OutboundStreamResponse, 1)
	errs := make(chan error, 1)
	go func() {
		resp, err := client.DoStream(context.Background(), &channels.OutboundRequest{
			Method: http.MethodPost,
			URL:    server.URL,
		})
		if err != nil {
			errs <- err
			return
		}
		result <- resp
	}()

	select {
	case <-headersSent:
	case <-time.After(time.Second):
		t.Fatal("server did not flush headers")
	}

	var resp *channels.OutboundStreamResponse
	select {
	case err := <-errs:
		t.Fatalf("do stream: %v", err)
	case resp = <-result:
	case <-time.After(time.Second):
		t.Fatal("DoStream did not return after response headers")
	}
	defer resp.Body.Close()

	releaseOnce.Do(func() {
		close(releaseBody)
	})
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read stream body: %v", err)
	}
	if string(body) != "data: done\n\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestClientDoStreamCapturesNonOKPreview(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"bad request"}`)
	}))
	defer server.Close()

	client := New(WithTransportProfile(testTransportProfile()), WithBodyPreviewBytes(64))
	resp, err := client.DoStream(context.Background(), &channels.OutboundRequest{
		Method: http.MethodPost,
		URL:    server.URL,
	})
	if err != nil {
		t.Fatalf("do stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.Status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.Status)
	}
	if string(resp.BodyPreview) != `{"error":"bad request"}` {
		t.Fatalf("preview = %q", resp.BodyPreview)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != string(resp.BodyPreview) {
		t.Fatalf("body = %q preview=%q", body, resp.BodyPreview)
	}
}

func TestClientDoStreamDoesNotInheritClientTimeout(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(1500 * time.Millisecond)
		_, _ = io.WriteString(w, "data: done\n\n")
	}))
	defer server.Close()

	client := New(WithTimeout(time.Second), WithTransportProfile(testTransportProfile()))
	resp, err := client.DoStream(context.Background(), &channels.OutboundRequest{
		Method: http.MethodPost,
		URL:    server.URL,
	})
	if err != nil {
		t.Fatalf("do stream: %v", err)
	}

	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		t.Fatalf("read stream body: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close stream body: %v", closeErr)
	}
	if string(body) != "data: done\n\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestClientDoHonorsConfiguredTimeout(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, "late")
	}))
	defer server.Close()

	client := New(WithTransportProfile(testTransportProfile()), WithTimeout(5*time.Millisecond))
	_, err := client.Do(context.Background(), &channels.OutboundRequest{
		Method: http.MethodGet,
		URL:    server.URL,
	})
	if err == nil {
		t.Fatal("expected configured client timeout error")
	}
}

func TestResponseBodyOrEmptyHandlesNilBody(t *testing.T) {
	cancelled := false
	body, err := responseBodyOrEmpty(&fhttp.Response{StatusCode: http.StatusNoContent}, func() {
		cancelled = true
	}, nil, false)
	if err != nil {
		t.Fatalf("response body: %v", err)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("body length = %d, want 0", len(data))
	}
	if err := body.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if !cancelled {
		t.Fatal("cancel was not called")
	}
}

func TestResponseBodyOrEmptyRejectsNilResponse(t *testing.T) {
	cancelled := false
	_, err := responseBodyOrEmpty(nil, func() {
		cancelled = true
	}, nil, false)
	if err == nil {
		t.Fatal("expected nil response error")
	}
	if !cancelled {
		t.Fatal("cancel was not called")
	}
}

func TestClientRequestReuseCachesUntilScopeCloses(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	profile := testTransportProfile()
	profile.ReuseKey = "test_request_reuse"
	client := New(WithTransportProfile(profile), WithRequestReuse(true))
	ctx, closeScope := client.WithRequestScope(context.Background())

	for i := 0; i < 2; i++ {
		resp, err := client.Do(ctx, &channels.OutboundRequest{
			Method: http.MethodGet,
			URL:    server.URL,
		})
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if string(resp.Body) != "ok" {
			t.Fatalf("request %d body = %q", i, resp.Body)
		}
	}
	if got := scopedClientCount(ctx); got != 1 {
		t.Fatalf("scoped clients = %d, want 1", got)
	}
	closeScope()
	if got := scopedClientCount(ctx); got != 0 {
		t.Fatalf("scoped clients after close = %d, want 0", got)
	}
}

func TestClientRequestReuseAllowsConcurrentDo(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	profile := testTransportProfile()
	profile.ReuseKey = "test_concurrent_request_reuse"
	client := New(WithTransportProfile(profile), WithRequestReuse(true))
	ctx, closeScope := client.WithRequestScope(context.Background())
	defer closeScope()

	const requests = 4
	var wg sync.WaitGroup
	errs := make(chan string, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Do(ctx, &channels.OutboundRequest{
				Method: http.MethodGet,
				URL:    server.URL,
			})
			if err != nil {
				errs <- err.Error()
				return
			}
			if string(resp.Body) != "ok" {
				errs <- "unexpected response body"
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent request reuse did not complete")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := scopedClientCount(ctx); got != 1 {
		t.Fatalf("scoped clients = %d, want 1", got)
	}
}

func TestClientRequestReuseRequiresExplicitKey(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	client := New(WithTransportProfile(testTransportProfile()), WithRequestReuse(true))
	ctx, closeScope := client.WithRequestScope(context.Background())
	defer closeScope()
	for i := 0; i < 2; i++ {
		if _, err := client.Do(ctx, &channels.OutboundRequest{
			Method: http.MethodGet,
			URL:    server.URL,
		}); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if got := scopedClientCount(ctx); got != 0 {
		t.Fatalf("scoped clients = %d, want 0 without reuse key", got)
	}
}

func TestClientBuildHTTPRequestAppliesTransportProfile(t *testing.T) {
	client := New(WithTransportProfile(channels.TransportProfile{
		TLSClientProfile:  "chrome_146",
		ReuseKey:          "test_reuse",
		HeaderOrder:       []string{"user-agent", "accept"},
		PseudoHeaderOrder: []string{":method", ":authority", ":scheme", ":path"},
	}))
	req, cancel, err := client.buildHTTPRequest(context.Background(), &channels.OutboundRequest{
		Method: http.MethodGet,
		URL:    "https://example.test",
		Headers: http.Header{
			"Accept":     []string{"*/*"},
			"User-Agent": []string{"test-agent"},
		},
	}, 0)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if cancel != nil {
		defer cancel()
	}
	if got, want := req.Header[fhttp.HeaderOrderKey], []string{"user-agent", "accept"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("header order = %#v, want %#v", got, want)
	}
	if got, want := req.Header[fhttp.PHeaderOrderKey], []string{":method", ":authority", ":scheme", ":path"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pseudo header order = %#v, want %#v", got, want)
	}
}

func scopedClientCount(ctx context.Context) int {
	scope, _ := ctx.Value(requestScopeKey{}).(*requestScope)
	if scope == nil {
		return 0
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	return len(scope.clients)
}

package proxypool

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

func TestNormalizeAndRedactProxyURL(t *testing.T) {
	normalized, err := NormalizeProxyURL("HTTP://user:secret@Example.COM:7890")
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if normalized.URL != "http://user:secret@example.com:7890" {
		t.Fatalf("normalized URL = %q", normalized.URL)
	}
	if got := RedactProxyURL(normalized.URL); got != "http://user:***@example.com:7890" {
		t.Fatalf("redacted URL = %q", got)
	}
}

func TestRepoCRUDAndResolver(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	rec := &Record{
		Name:     "HK 01",
		ProxyURL: "socks5://127.0.0.1:1080",
		IsActive: true,
		Notes:    "imported",
	}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Scheme != "socks5" || got.Host != "127.0.0.1" || got.Port != 1080 || !got.IsActive {
		t.Fatalf("unexpected record: %+v", got)
	}
	resolver := NewResolver(repo)
	metadata, err := resolver.ResolveAccountMetadata(context.Background(), channels.Account{
		ID:       "acc-1",
		Metadata: map[string]any{MetadataProxyID: rec.ID},
	})
	if err != nil {
		t.Fatalf("resolve active: %v", err)
	}
	if metadata[MetadataProxyURL] != rec.ProxyURL {
		t.Fatalf("metadata proxy url = %v, want %q", metadata[MetadataProxyURL], rec.ProxyURL)
	}

	got.IsActive = false
	if err := repo.Update(got); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_, err = resolver.ResolveAccountMetadata(context.Background(), channels.Account{
		ID:       "acc-1",
		Metadata: map[string]any{MetadataProxyID: rec.ID},
	})
	if !errors.Is(err, channels.ErrAccountUnavailable) {
		t.Fatalf("disabled resolver error = %v, want account unavailable", err)
	}
}

func TestRepoRejectsDuplicateNormalizedProxy(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	first := &Record{ProxyURL: "HTTP://user:secret@Example.COM:7890", IsActive: true}
	if err := repo.Create(first); err != nil {
		t.Fatalf("create first: %v", err)
	}
	second := &Record{ProxyURL: "http://user:secret@example.com:7890", IsActive: true}
	if err := repo.Create(second); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("create duplicate error = %v, want ErrDuplicate", err)
	}
}

func TestImportProxyRecordsSkipsExistingAndSameBatchDuplicates(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	existing := &Record{Name: "Existing", ProxyURL: "http://user:secret@example.com:7890", IsActive: true}
	if err := repo.Create(existing); err != nil {
		t.Fatalf("create existing: %v", err)
	}
	result := ImportProxyRecords(repo, `
First ---- http://user:secret@example.com:7890
Fresh ---- socks5://127.0.0.1:1080
Fresh again ---- socks5://127.0.0.1:1080
`)
	if len(result.Created) != 1 {
		t.Fatalf("created = %d, want 1: %+v", len(result.Created), result.Created)
	}
	if len(result.Skipped) != 2 {
		t.Fatalf("skipped = %d, want 2: %+v", len(result.Skipped), result.Skipped)
	}
	if result.Skipped[0].Record.ID != existing.ID {
		t.Fatalf("first skipped record = %+v, want existing %s", result.Skipped[0].Record, existing.ID)
	}
	records, err := repo.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d, want 2", len(records))
	}
}

func TestRepoHealthPersistence(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	rec := &Record{ProxyURL: "http://user:secret@example.com:7890", IsActive: true}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repo.MarkUnhealthy(rec.ID, 100, "dial timeout"); err != nil {
		t.Fatalf("mark unhealthy: %v", err)
	}
	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get unhealthy: %v", err)
	}
	if got.HealthStatus != HealthUnhealthy || got.FailureCount != 1 || got.LastError != "dial timeout" {
		t.Fatalf("unhealthy record = %+v", got)
	}
	meta := ProbeMetadata{
		ExitIP:  "31.59.20.176",
		Country: "United States",
		Region:  "Florida",
		City:    "Miami",
	}
	if err := repo.MarkHealthy(rec.ID, 42, 200, meta); err != nil {
		t.Fatalf("mark healthy: %v", err)
	}
	got, err = repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get healthy: %v", err)
	}
	if got.HealthStatus != HealthHealthy || got.LatencyMS != 42 || got.FailureCount != 0 || got.LastError != "" {
		t.Fatalf("healthy record = %+v", got)
	}
	if got.ExitIP != meta.ExitIP || got.Country != meta.Country || got.Region != meta.Region || got.City != meta.City {
		t.Fatalf("healthy metadata = %+v, want %+v", got, meta)
	}
}

func TestCheckerRecordsProxyHealth(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	rec := &Record{ProxyURL: "http://user:secret@example.com:7890", IsActive: true}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	transport := &probeTransport{
		status: http.StatusOK,
		body: []byte(`{
			"status":"success",
			"country":"United States",
			"regionName":"Florida",
			"city":"Miami",
			"query":"31.59.20.176"
		}`),
	}
	checker := NewChecker(repo, transport, CheckerConfig{
		ProbeURL:    "http://ip-api.com/json/?fields=status,message,country,regionName,city,query",
		Concurrency: 1,
	})
	checker.CheckOnce(context.Background())
	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.HealthStatus != HealthHealthy || got.LatencyMS <= 0 || got.FailureCount != 0 {
		t.Fatalf("healthy record = %+v", got)
	}
	if got.ExitIP != "31.59.20.176" || got.Country != "United States" || got.Region != "Florida" || got.City != "Miami" {
		t.Fatalf("healthy location = %+v", got)
	}
	if transport.proxyURL != rec.ProxyURL {
		t.Fatalf("probe proxy URL = %q, want %q", transport.proxyURL, rec.ProxyURL)
	}

	transport.err = fmt.Errorf("proxy failed with %s", rec.ProxyURL)
	checker.CheckOnce(context.Background())
	got, err = repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get unhealthy: %v", err)
	}
	if got.HealthStatus != HealthUnhealthy || got.FailureCount != 1 {
		t.Fatalf("unhealthy record = %+v", got)
	}
	if strings.Contains(got.LastError, "secret") {
		t.Fatalf("last error leaked proxy secret: %q", got.LastError)
	}
}

func TestCheckerCheckRecordUpdatesOneInactiveProxy(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	rec := &Record{ProxyURL: "http://user:secret@example.com:7890", IsActive: false}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	transport := &probeTransport{
		status: http.StatusOK,
		body:   []byte(`{"status":"success","country":"United States","regionName":"Texas","city":"Dallas","query":"198.51.100.10"}`),
	}
	checker := NewChecker(repo, transport, CheckerConfig{Concurrency: 1})

	got, err := checker.CheckRecord(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("check record: %v", err)
	}

	if got.HealthStatus != HealthHealthy || got.LatencyMS <= 0 || got.FailureCount != 0 {
		t.Fatalf("checked record = %+v", got)
	}
	if got.ExitIP != "198.51.100.10" || got.Country != "United States" || got.Region != "Texas" || got.City != "Dallas" {
		t.Fatalf("checked location = %+v", got)
	}
}

func TestCheckerRecordsProviderLimitResponse(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	rec := &Record{ProxyURL: "http://user:secret@example.com:7890", IsActive: true}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	transport := &probeTransport{
		status: http.StatusPaymentRequired,
		body:   []byte("Bandwidth limit reached. Please upgrade to continue using the proxy."),
	}
	checker := NewChecker(repo, transport, CheckerConfig{Concurrency: 1})

	checker.CheckOnce(context.Background())

	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.HealthStatus != HealthUnhealthy || got.FailureCount != 1 || got.LatencyMS != 0 {
		t.Fatalf("limited proxy record = %+v", got)
	}
	if !strings.Contains(got.LastError, "402") || !strings.Contains(got.LastError, "Bandwidth limit reached") {
		t.Fatalf("last error = %q, want status and provider body", got.LastError)
	}
	if strings.Contains(got.LastError, "secret") {
		t.Fatalf("last error leaked proxy secret: %q", got.LastError)
	}
}

func TestProbeTransportUsesHTTPProxyAndReturnsLimitBody(t *testing.T) {
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "http://probe.test/json" {
			t.Fatalf("probe URL = %q", r.URL.String())
		}
		http.Error(w, "Bandwidth limit reached. Please upgrade to continue using the proxy.", http.StatusPaymentRequired)
	}))
	defer proxyServer.Close()

	transport := NewProbeTransport()
	resp, err := transport.Do(context.Background(), &channels.OutboundRequest{
		Method: http.MethodGet,
		URL:    "http://probe.test/json",
		TransportProfile: channels.TransportProfile{
			ProxyURL: proxyServer.URL,
		},
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("probe through proxy: %v", err)
	}
	if resp.Status != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want 402", resp.Status)
	}
	if !strings.Contains(string(resp.BodyPreview), "Bandwidth limit reached") {
		t.Fatalf("preview = %q", resp.BodyPreview)
	}
}

func TestCheckerTimesOutStalledProbe(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	repo := NewRepo(db)
	rec := &Record{ProxyURL: "http://user:secret@example.com:7890", IsActive: true}
	if err := repo.Create(rec); err != nil {
		t.Fatalf("create: %v", err)
	}
	transport := &probeTransport{delay: 100 * time.Millisecond}
	checker := NewChecker(repo, transport, CheckerConfig{
		ProbeURL:    "https://codebuff.test/api/healthz",
		Timeout:     5 * time.Millisecond,
		Concurrency: 1,
	})

	started := time.Now()
	checker.CheckOnce(context.Background())
	if elapsed := time.Since(started); elapsed >= 80*time.Millisecond {
		t.Fatalf("check took %s, want hard timeout before stalled transport returns", elapsed)
	}
	got, err := repo.Get(rec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.HealthStatus != HealthUnhealthy || got.FailureCount != 1 {
		t.Fatalf("timed out record = %+v", got)
	}
	if !strings.Contains(got.LastError, "timed out") {
		t.Fatalf("last error = %q, want timeout", got.LastError)
	}
}

func TestParseImportTextReportsLineFailures(t *testing.T) {
	candidates, failures := ParseImportText(`
# comment
HK 01----http://user:pass@example.com:7890
bad
socks5://127.0.0.1:1080
`)
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want 2: %+v", len(candidates), candidates)
	}
	if candidates[0].Name != "HK 01" {
		t.Fatalf("candidate name = %q", candidates[0].Name)
	}
	if len(failures) != 1 || failures[0].Line != 4 {
		t.Fatalf("failures = %+v, want one line 4 failure", failures)
	}
}

type probeTransport struct {
	status   int
	err      error
	proxyURL string
	delay    time.Duration
	body     []byte
}

func (t *probeTransport) Do(_ context.Context, req *channels.OutboundRequest) (*channels.OutboundResponse, error) {
	t.proxyURL = req.TransportProfile.ProxyURL
	if t.delay > 0 {
		time.Sleep(t.delay)
	}
	if t.err != nil {
		return nil, t.err
	}
	status := t.status
	if status == 0 {
		status = http.StatusOK
	}
	return &channels.OutboundResponse{
		Status:      status,
		Headers:     http.Header{},
		Body:        t.body,
		BodyPreview: t.body,
	}, nil
}

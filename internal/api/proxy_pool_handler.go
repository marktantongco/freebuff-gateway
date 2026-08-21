package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
)

type proxyEntryView struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	URLRedacted   string `json:"url_redacted"`
	Scheme        string `json:"scheme"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Username      string `json:"username,omitempty"`
	IsActive      bool   `json:"is_active"`
	Notes         string `json:"notes,omitempty"`
	HealthStatus  string `json:"health_status"`
	LatencyMS     *int64 `json:"latency_ms,omitempty"`
	LastCheckedAt int64  `json:"last_checked_at,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	FailureCount  int    `json:"failure_count"`
	ExitIP        string `json:"exit_ip,omitempty"`
	Country       string `json:"country,omitempty"`
	Region        string `json:"region,omitempty"`
	City          string `json:"city,omitempty"`
	CreatedAt     int64  `json:"created_at"`
	UpdatedAt     int64  `json:"updated_at"`
}

type proxyBindingView struct {
	ProxyID     string `json:"proxy_id"`
	Name        string `json:"name,omitempty"`
	URLRedacted string `json:"url_redacted,omitempty"`
	IsActive    bool   `json:"is_active"`
	Status      string `json:"status"`
}

type createProxyReq struct {
	Name     string `json:"name"`
	ProxyURL string `json:"proxy_url"`
	IsActive *bool  `json:"is_active"`
	Notes    string `json:"notes"`
}

type updateProxyReq struct {
	Name     *string `json:"name"`
	ProxyURL *string `json:"proxy_url"`
	IsActive *bool   `json:"is_active"`
	Notes    *string `json:"notes"`
}

type importProxiesReq struct {
	Text string `json:"text"`
}

type proxyImportSkippedView struct {
	Line   int            `json:"line"`
	Input  string         `json:"input"`
	Reason string         `json:"reason"`
	Proxy  proxyEntryView `json:"proxy"`
}

type importProxiesResp struct {
	Created        int                       `json:"created"`
	Skipped        int                       `json:"skipped"`
	Proxies        []proxyEntryView          `json:"proxies"`
	SkippedProxies []proxyImportSkippedView  `json:"skipped_proxies"`
	Failures       []proxypool.ImportFailure `json:"failures"`
}

func (h *AdminHandler) ListFreeBuffProxies(w http.ResponseWriter, _ *http.Request) {
	if h.proxies == nil {
		writeJSON(w, http.StatusOK, []proxyEntryView{})
		return
	}
	records, err := h.proxies.List()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, proxyEntryViews(records))
}

func (h *AdminHandler) CreateFreeBuffProxy(w http.ResponseWriter, r *http.Request) {
	if h.proxies == nil {
		writeJSONError(w, http.StatusInternalServerError, "proxy pool unavailable")
		return
	}
	var req createProxyReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	active := true
	if req.IsActive != nil {
		active = *req.IsActive
	}
	rec := &proxypool.Record{
		Name:     strings.TrimSpace(req.Name),
		ProxyURL: strings.TrimSpace(req.ProxyURL),
		IsActive: active,
		Notes:    strings.TrimSpace(req.Notes),
	}
	if err := h.proxies.Create(rec); err != nil {
		if errors.Is(err, proxypool.ErrDuplicate) {
			h.writeProxyPoolError(w, err)
			return
		}
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, proxyEntryViewFromRecord(rec))
}

func (h *AdminHandler) ImportFreeBuffProxies(w http.ResponseWriter, r *http.Request) {
	if h.proxies == nil {
		writeJSONError(w, http.StatusInternalServerError, "proxy pool unavailable")
		return
	}
	var req importProxiesReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	result := proxypool.ImportProxyRecords(h.proxies, req.Text)
	failures := result.Failures
	if failures == nil {
		failures = []proxypool.ImportFailure{}
	}
	writeJSON(w, http.StatusOK, importProxiesResp{
		Created:        len(result.Created),
		Skipped:        len(result.Skipped),
		Proxies:        proxyEntryViews(result.Created),
		SkippedProxies: proxyImportSkippedViews(result.Skipped),
		Failures:       failures,
	})
}

func (h *AdminHandler) UpdateFreeBuffProxy(w http.ResponseWriter, r *http.Request) {
	if h.proxies == nil {
		writeJSONError(w, http.StatusInternalServerError, "proxy pool unavailable")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	rec, err := h.proxies.Get(id)
	if err != nil {
		h.writeProxyPoolError(w, err)
		return
	}
	var req updateProxyReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Name != nil {
		rec.Name = strings.TrimSpace(*req.Name)
	}
	if req.ProxyURL != nil {
		rec.ProxyURL = strings.TrimSpace(*req.ProxyURL)
	}
	if req.IsActive != nil {
		rec.IsActive = *req.IsActive
	}
	if req.Notes != nil {
		rec.Notes = strings.TrimSpace(*req.Notes)
	}
	if err := h.proxies.Update(rec); err != nil {
		if errors.Is(err, proxypool.ErrDuplicate) {
			h.writeProxyPoolError(w, err)
			return
		}
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, proxyEntryViewFromRecord(rec))
}

func (h *AdminHandler) TestFreeBuffProxy(w http.ResponseWriter, r *http.Request) {
	if h.proxies == nil {
		writeJSONError(w, http.StatusInternalServerError, "proxy pool unavailable")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	checker := proxypool.NewChecker(h.proxies, proxypool.NewProbeTransport(), h.proxyCheckConfig)
	rec, err := checker.CheckRecord(r.Context(), id)
	if err != nil {
		h.writeProxyPoolError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, proxyEntryViewFromRecord(rec))
}

func (h *AdminHandler) DeleteFreeBuffProxy(w http.ResponseWriter, r *http.Request) {
	if h.proxies == nil {
		writeJSONError(w, http.StatusInternalServerError, "proxy pool unavailable")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	if err := h.proxies.Delete(id); err != nil {
		h.writeProxyPoolError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *AdminHandler) writeProxyPoolError(w http.ResponseWriter, err error) {
	if errors.Is(err, proxypool.ErrNotFound) {
		writeJSONError(w, http.StatusNotFound, "proxy not found")
		return
	}
	if errors.Is(err, proxypool.ErrDuplicate) {
		writeJSONError(w, http.StatusConflict, "proxy already exists")
		return
	}
	writeJSONError(w, http.StatusInternalServerError, err.Error())
}

func proxyEntryViews(records []*proxypool.Record) []proxyEntryView {
	out := make([]proxyEntryView, 0, len(records))
	for _, rec := range records {
		out = append(out, proxyEntryViewFromRecord(rec))
	}
	return out
}

func proxyImportSkippedViews(records []proxypool.ImportSkipped) []proxyImportSkippedView {
	out := make([]proxyImportSkippedView, 0, len(records))
	for _, rec := range records {
		out = append(out, proxyImportSkippedView{
			Line:   rec.Line,
			Input:  rec.Input,
			Reason: rec.Reason,
			Proxy:  proxyEntryViewFromRecord(rec.Record),
		})
	}
	return out
}

func proxyEntryViewFromRecord(rec *proxypool.Record) proxyEntryView {
	if rec == nil {
		return proxyEntryView{}
	}
	var latency *int64
	if rec.LatencyMS > 0 {
		v := rec.LatencyMS
		latency = &v
	}
	return proxyEntryView{
		ID:            rec.ID,
		Name:          rec.Name,
		URLRedacted:   rec.RedactedURL(),
		Scheme:        rec.Scheme,
		Host:          rec.Host,
		Port:          rec.Port,
		Username:      rec.Username,
		IsActive:      rec.IsActive,
		Notes:         rec.Notes,
		HealthStatus:  rec.HealthStatus,
		LatencyMS:     latency,
		LastCheckedAt: rec.LastCheckedAt,
		LastError:     rec.LastError,
		FailureCount:  rec.FailureCount,
		ExitIP:        rec.ExitIP,
		Country:       rec.Country,
		Region:        rec.Region,
		City:          rec.City,
		CreatedAt:     rec.CreatedAt,
		UpdatedAt:     rec.UpdatedAt,
	}
}

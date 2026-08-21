package api

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/orchestration"
)

type ProxyHandler struct {
	registry *channels.Registry
	runner   *orchestration.Runner
}

func NewProxyHandler(reg *channels.Registry, runner *orchestration.Runner) *ProxyHandler {
	return &ProxyHandler{registry: reg, runner: runner}
}

func (h *ProxyHandler) Handle(w http.ResponseWriter, r *http.Request) {
	channelID := r.PathValue("id")
	if channelID == "" {
		writeJSONError(w, http.StatusNotFound, "missing channel id")
		return
	}
	h.handleChannel(w, r, channelID, "/channels/"+channelID)
}

func (h *ProxyHandler) HandleFreeBuff(w http.ResponseWriter, r *http.Request) {
	h.handleChannel(w, r, freeBuffChannelID, "")
}

func (h *ProxyHandler) handleChannel(w http.ResponseWriter, r *http.Request, channelID, prefix string) {
	if _, ok := h.registry.Get(channelID); !ok {
		writeJSONError(w, http.StatusNotFound, "channel not registered: "+channelID)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}

	path := r.URL.Path
	if prefix != "" {
		path = strings.TrimPrefix(path, prefix)
	}
	if path == "" {
		path = "/"
	}

	in := &channels.InboundRequest{
		ChannelID: channelID,
		Method:    r.Method,
		Path:      path,
		RawQuery:  r.URL.RawQuery,
		Headers:   r.Header.Clone(),
		Body:      body,
	}

	if requestWantsStream(body) {
		h.handleStream(w, r, in, channelID)
		return
	}

	outcome, err := h.runner.Execute(r.Context(), in)
	if err != nil {
		writeProxyExecutionError(w, err)
		return
	}

	for k, vs := range outcome.Response.Headers {
		if shouldDropHeader(k) {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Reverse-Channel", channelID)
	w.Header().Set("X-Reverse-Class", outcome.Class.String())
	w.WriteHeader(outcome.Response.Status)
	_, _ = w.Write(outcome.Response.Body)
}

func (h *ProxyHandler) handleStream(w http.ResponseWriter, r *http.Request, in *channels.InboundRequest, channelID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	execution, err := h.runner.ExecuteStream(r.Context(), in)
	if err != nil {
		writeProxyExecutionError(w, err)
		return
	}
	for k, vs := range execution.Headers {
		if shouldDropHeader(k) {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Reverse-Channel", channelID)
	w.Header().Set("X-Reverse-Class", execution.Class.String())
	w.WriteHeader(execution.Status)
	flusher.Flush()
	if _, err := execution.Pump(responseStreamSink{w: w, f: flusher}); err != nil {
		log.Printf("http stream pump %s %s channel=%s status=%d class=%s: %v", r.Method, r.URL.Path, channelID, execution.Status, execution.Class.String(), err)
	}
}

type responseStreamSink struct {
	w http.ResponseWriter
	f http.Flusher
}

func (s responseStreamSink) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	s.f.Flush()
	return n, err
}

func (s responseStreamSink) Flush() {
	s.f.Flush()
}

func requestWantsStream(body []byte) bool {
	var payload struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return payload.Stream
}

func shouldDropHeader(name string) bool {
	switch strings.ToLower(name) {
	case "content-length", "transfer-encoding", "connection", "keep-alive",
		"proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade":
		return true
	}
	return false
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

func writeProxyExecutionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, channels.ErrCapacityLimited):
		writeJSONError(w, http.StatusTooManyRequests, "capacity limited")
	case errors.Is(err, accounts.ErrQuotaExceeded):
		writeJSONError(w, http.StatusServiceUnavailable, "quota exceeded")
	default:
		writeJSONError(w, http.StatusBadGateway, err.Error())
	}
}

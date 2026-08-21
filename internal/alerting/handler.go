package alerting

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Handler provides HTTP endpoints for alert management.
type Handler struct {
	manager *Manager
}

// NewHandler creates a new alert handler.
func NewHandler(manager *Manager) *Handler {
	return &Handler{manager: manager}
}

// RegisterRoutes registers alert routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/alerts", h.handleAlerts)
	mux.HandleFunc("/api/alerts/", h.handleAlertByID)
	mux.HandleFunc("/api/alerts/history", h.handleHistory)
	mux.HandleFunc("/api/alerts/stats", h.handleStats)
}

func (h *Handler) handleAlerts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listAlerts(w, r)
	case http.MethodPost:
		h.createManualAlert(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) listAlerts(w http.ResponseWriter, r *http.Request) {
	stateFilter := State(r.URL.Query().Get("state"))
	severityFilter := Severity(r.URL.Query().Get("severity"))

	alerts := h.manager.GetAlerts(stateFilter, severityFilter)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"alerts": alerts,
		"count":  len(alerts),
	})
}

func (h *Handler) createManualAlert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string            `json:"name"`
		Severity Severity          `json:"severity"`
		Message  string            `json:"message"`
		Source   string            `json:"source"`
		Labels   map[string]string `json:"labels"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if req.Severity == "" {
		req.Severity = SeverityWarning
	}
	if req.Source == "" {
		req.Source = "manual"
	}

	alert := &Alert{
		Name:     req.Name,
		Severity: req.Severity,
		State:    StateFiring,
		Source:   req.Source,
		Message:  req.Message,
		Labels:   req.Labels,
	}

	h.manager.mu.Lock()
	h.manager.alerts[alert.Fingerprint()] = alert
	h.manager.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(alert)
}

func (h *Handler) handleAlertByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/api/alerts/"):]
	if id == "" || id == "history" || id == "stats" {
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getAlert(w, id)
	case http.MethodPost:
		h.actionAlert(w, r, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) getAlert(w http.ResponseWriter, id string) {
	h.manager.mu.RLock()
	defer h.manager.mu.RUnlock()

	for _, a := range h.manager.alerts {
		if a.ID == id {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(a)
			return
		}
	}

	http.Error(w, "alert not found", http.StatusNotFound)
}

func (h *Handler) actionAlert(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		Action string `json:"action"`
		User   string `json:"user"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	var err error
	switch req.Action {
	case "acknowledge":
		err = h.manager.Acknowledge(id, req.User)
	case "silence":
		err = h.manager.Silence(id)
	default:
		http.Error(w, "unknown action: "+req.Action, http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	history := h.manager.GetHistory(limit)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"history": history,
		"count":   len(history),
	})
}

func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := h.manager.Stats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

package usermgmt

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Handler handles user management HTTP endpoints.
type Handler struct {
	repo *Repo
}

// NewHandler creates a new user management handler.
func NewHandler(repo *Repo) *Handler {
	return &Handler{repo: repo}
}

// ListUsers returns all admin users.
func (h *Handler) ListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.repo.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := make([]UserResponse, 0, len(users))
	for _, u := range users {
		resp = append(resp, u.ToResponse())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": resp})
}

// GetUser returns a single user by ID.
func (h *Handler) GetUser(w http.ResponseWriter, r *http.Request) {
	id := extractPathID(r, "/api/admin/users/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "user id required")
		return
	}
	u, err := h.repo.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, u.ToResponse())
}

// CreateUser creates a new admin user.
func (h *Handler) CreateUser(w http.ResponseWriter, r *http.Request) {
	var req UserCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Username == "" {
		writeError(w, http.StatusBadRequest, "username is required")
		return
	}
	if req.Password == "" {
		writeError(w, http.StatusBadRequest, "password is required")
		return
	}
	if len(req.Password) < 6 {
		writeError(w, http.StatusBadRequest, "password must be at least 6 characters")
		return
	}
	role := req.Role
	if role == "" {
		role = "viewer"
	}
	id, err := h.repo.Create(req.Password, role, req.Username)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	u, _ := h.repo.Get(id)
	writeJSON(w, http.StatusCreated, u.ToResponse())
}

// UpdateUser updates an existing user.
func (h *Handler) UpdateUser(w http.ResponseWriter, r *http.Request) {
	id := extractPathID(r, "/api/admin/users/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "user id required")
		return
	}
	var req struct {
		Password string `json:"password,omitempty"`
		Role     string `json:"role,omitempty"`
		Active   *bool  `json:"active,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Password != "" && len(req.Password) < 6 {
		writeError(w, http.StatusBadRequest, "password must be at least 6 characters")
		return
	}
	if err := h.repo.Update(id, req.Password, req.Role, req.Active); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	u, _ := h.repo.Get(id)
	writeJSON(w, http.StatusOK, u.ToResponse())
}

// DeleteUser removes a user.
func (h *Handler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	id := extractPathID(r, "/api/admin/users/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "user id required")
		return
	}
	if err := h.repo.Delete(id); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
}

// ChangePassword changes a user's own password.
func (h *Handler) ChangePassword(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.OldPassword == "" || req.NewPassword == "" {
		writeError(w, http.StatusBadRequest, "old_password and new_password required")
		return
	}
	if len(req.NewPassword) < 6 {
		writeError(w, http.StatusBadRequest, "new password must be at least 6 characters")
		return
	}
	// Get user ID from session cookie
	id := userIDFromRequest(r)
	if id == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if err := h.repo.ChangePassword(id, req.OldPassword, req.NewPassword); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "password changed"})
}

// ─── Helpers ──────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func extractPathID(r *http.Request, prefix string) string {
	path := r.URL.Path
	if strings.HasPrefix(path, prefix) {
		id := strings.TrimPrefix(path, prefix)
		// Remove trailing slash
		id = strings.TrimRight(id, "/")
		if id != "" {
			return id
		}
	}
	return ""
}

func userIDFromRequest(r *http.Request) string {
	// Extract from cookie session — we'll need the authenticator to resolve this
	// For now, accept user_id header from authenticated middleware
	return r.Header.Get("X-User-ID")
}

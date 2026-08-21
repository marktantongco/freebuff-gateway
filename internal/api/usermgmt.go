package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/marktantongco/freebuff-gateway/internal/usermgmt"
)

// UserManagementHandler wraps usermgmt.Handler for the router.
type UserManagementHandler struct {
	repo *usermgmt.Repo
}

// NewUserManagementHandler creates a new user management handler.
func NewUserManagementHandler(repo *usermgmt.Repo) *UserManagementHandler {
	return &UserManagementHandler{repo: repo}
}

// ListUsers returns all admin users.
func (h *UserManagementHandler) ListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.repo.List()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := make([]usermgmt.UserResponse, 0, len(users))
	for _, u := range users {
		resp = append(resp, u.ToResponse())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"users": resp})
}

// GetUser returns a single user by ID from the URL path.
func (h *UserManagementHandler) GetUser(w http.ResponseWriter, r *http.Request) {
	id := extractUserID(r)
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "user id required")
		return
	}
	u, err := h.repo.Get(id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, u.ToResponse())
}

// CreateUser creates a new admin user.
func (h *UserManagementHandler) CreateUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Username == "" {
		writeJSONError(w, http.StatusBadRequest, "username is required")
		return
	}
	if req.Password == "" {
		writeJSONError(w, http.StatusBadRequest, "password is required")
		return
	}
	if len(req.Password) < 6 {
		writeJSONError(w, http.StatusBadRequest, "password must be at least 6 characters")
		return
	}
	role := req.Role
	if role == "" {
		role = "viewer"
	}
	id, err := h.repo.Create(req.Password, role, req.Username)
	if err != nil {
		writeJSONError(w, http.StatusConflict, err.Error())
		return
	}
	u, _ := h.repo.Get(id)
	writeJSON(w, http.StatusCreated, u.ToResponse())
}

// UpdateUser updates an existing user.
func (h *UserManagementHandler) UpdateUser(w http.ResponseWriter, r *http.Request) {
	id := extractUserID(r)
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "user id required")
		return
	}
	var req struct {
		Password string `json:"password,omitempty"`
		Role     string `json:"role,omitempty"`
		Active   *bool  `json:"active,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Password != "" && len(req.Password) < 6 {
		writeJSONError(w, http.StatusBadRequest, "password must be at least 6 characters")
		return
	}
	if err := h.repo.Update(id, req.Password, req.Role, req.Active); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	u, _ := h.repo.Get(id)
	writeJSON(w, http.StatusOK, u.ToResponse())
}

// DeleteUser removes a user.
func (h *UserManagementHandler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	id := extractUserID(r)
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "user id required")
		return
	}
	if err := h.repo.Delete(id); err != nil {
		writeJSONError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
}

// ChangePassword changes the current user's password.
func (h *UserManagementHandler) ChangePassword(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.OldPassword == "" || req.NewPassword == "" {
		writeJSONError(w, http.StatusBadRequest, "old_password and new_password required")
		return
	}
	if len(req.NewPassword) < 6 {
		writeJSONError(w, http.StatusBadRequest, "new password must be at least 6 characters")
		return
	}
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if err := h.repo.ChangePassword(userID, req.OldPassword, req.NewPassword); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "password changed"})
}

// extractUserID gets the user ID from the URL path.
// Handles both /api/admin/users/{id} and /api/admin/users/{id}/
func extractUserID(r *http.Request) string {
	prefix := "/api/admin/users/"
	if strings.HasPrefix(r.URL.Path, prefix) {
		id := strings.TrimPrefix(r.URL.Path, prefix)
		id = strings.TrimRight(id, "/")
		return id
	}
	return ""
}

package api

import (
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// BuildRouter creates the API mux with all routes.
func BuildRouter(
	admin *AdminHandler,
	proxy *ProxyHandler,
	web fs.FS,
	adminAuth *AdminAuthenticator,
	apiKeyAuth *APIKeyAuthenticator,
	userHandler *UserManagementHandler,
) *http.ServeMux {
	mux := http.NewServeMux()

	requireAdmin := func(next http.HandlerFunc) http.HandlerFunc { return next }
	if adminAuth != nil {
		mux.HandleFunc("POST /api/admin/login", adminAuth.Login)
		mux.HandleFunc("POST /api/admin/logout", adminAuth.Logout)
		mux.HandleFunc("GET /api/admin/me", adminAuth.Me)
		requireAdmin = adminAuth.Require
	}

	mux.HandleFunc("GET /api/admin/channels", requireAdmin(admin.ListChannels))
	mux.HandleFunc("PUT /api/admin/channels/{id}/config", requireAdmin(admin.UpdateChannelConfig))
	mux.HandleFunc("GET /api/admin/freebuff/models", requireAdmin(admin.ListFreeBuffModels))
	mux.HandleFunc("GET /api/admin/freebuff/proxies", requireAdmin(admin.ListFreeBuffProxies))
	mux.HandleFunc("POST /api/admin/freebuff/proxies", requireAdmin(admin.CreateFreeBuffProxy))
	mux.HandleFunc("POST /api/admin/freebuff/proxies/import", requireAdmin(admin.ImportFreeBuffProxies))
	mux.HandleFunc("POST /api/admin/freebuff/proxies/{id}/test", requireAdmin(admin.TestFreeBuffProxy))
	mux.HandleFunc("PUT /api/admin/freebuff/proxies/{id}", requireAdmin(admin.UpdateFreeBuffProxy))
	mux.HandleFunc("DELETE /api/admin/freebuff/proxies/{id}", requireAdmin(admin.DeleteFreeBuffProxy))
	mux.HandleFunc("GET /api/admin/freebuff/accounts", requireAdmin(admin.ListFreeBuffAccounts))
	mux.HandleFunc("GET /api/admin/freebuff/scheduler", requireAdmin(admin.ListFreeBuffScheduler))
	mux.HandleFunc("POST /api/admin/freebuff/accounts/github-protocol-login", requireAdmin(admin.FreeBuffGitHubProtocolLogin))
	mux.HandleFunc("GET /api/admin/freebuff/accounts/github-protocol-login/{jobID}", requireAdmin(admin.GetFreeBuffGitHubProtocolLogin))
	mux.HandleFunc("POST /api/admin/freebuff/accounts/refresh", requireAdmin(admin.RefreshFreeBuffAccounts))
	mux.HandleFunc("POST /api/admin/freebuff/accounts/{id}/refresh", requireAdmin(admin.RefreshFreeBuffAccount))
	mux.HandleFunc("GET /api/admin/auth-keys", requireAdmin(admin.ListAuthKeys))
	mux.HandleFunc("POST /api/admin/auth-keys", requireAdmin(admin.CreateAuthKey))
	mux.HandleFunc("DELETE /api/admin/auth-keys/{id}", requireAdmin(admin.DeleteAuthKey))
	mux.HandleFunc("POST /api/admin/channels/{id}/account-logins", requireAdmin(admin.StartAccountLogin))
	mux.HandleFunc("GET /api/admin/channels/{id}/account-logins/{sessionID}", requireAdmin(admin.PollAccountLogin))
	mux.HandleFunc("POST /api/admin/channels/{id}/account-logins/{sessionID}/complete", requireAdmin(admin.CompleteAccountLogin))
	mux.HandleFunc("GET /api/admin/accounts", requireAdmin(admin.ListAccounts))
	mux.HandleFunc("POST /api/admin/accounts", requireAdmin(admin.CreateAccount))
	mux.HandleFunc("POST /api/admin/accounts/batch", requireAdmin(admin.BatchUpdateAccounts))
	mux.HandleFunc("PUT /api/admin/accounts/{id}", requireAdmin(admin.UpdateAccount))
	mux.HandleFunc("DELETE /api/admin/accounts/{id}", requireAdmin(admin.DeleteAccount))
	mux.HandleFunc("GET /api/admin/sessions", requireAdmin(admin.ListSessions))
	mux.HandleFunc("GET /api/admin/logs", requireAdmin(admin.ListLogs))
	mux.HandleFunc("GET /api/admin/system-logs", requireAdmin(admin.ListSystemLogs))
	mux.HandleFunc("GET /api/admin/metrics", requireAdmin(admin.ListMetrics))
	mux.HandleFunc("GET /api/admin/metrics/series", requireAdmin(admin.ListMetricSeries))
	mux.HandleFunc("GET /api/admin/usage/summary", requireAdmin(admin.ListUsageSummary))
	mux.HandleFunc("GET /api/admin/usage/accounts", requireAdmin(admin.ListUsageAccounts))
	mux.HandleFunc("GET /api/admin/usage/events", requireAdmin(admin.ListUsageEvents))

	// User management routes
	if userHandler != nil {
		mux.HandleFunc("GET /api/admin/users", requireAdmin(userHandler.ListUsers))
		mux.HandleFunc("POST /api/admin/users", requireAdmin(userHandler.CreateUser))
		mux.HandleFunc("GET /api/admin/users/", requireAdmin(userHandler.GetUser))
		mux.HandleFunc("PUT /api/admin/users/", requireAdmin(userHandler.UpdateUser))
		mux.HandleFunc("DELETE /api/admin/users/", requireAdmin(userHandler.DeleteUser))
		mux.HandleFunc("POST /api/admin/users/change-password", requireAdmin(userHandler.ChangePassword))
	}

	requireAPIKey := func(next http.HandlerFunc) http.HandlerFunc { return next }
	if apiKeyAuth != nil {
		requireAPIKey = apiKeyAuth.Require
	}
	models := NewModelHandler(admin.registry)
	mux.HandleFunc("GET /v1/models", requireAPIKey(models.ListModels))
	mux.HandleFunc("GET /v1/models/{$}", requireAPIKey(models.ListModels))
	mux.HandleFunc("GET /v1/models/{modelID...}", requireAPIKey(models.GetModel))
	mux.HandleFunc("POST /v1/chat/completions", requireAPIKey(proxy.HandleFreeBuff))
	mux.HandleFunc("POST /v1/messages", requireAPIKey(proxy.HandleFreeBuff))
	mux.HandleFunc("/channels/{id}/", requireAPIKey(proxy.Handle))
	mux.HandleFunc("/channels/{id}", requireAPIKey(proxy.Handle))

	if web != nil {
		mux.Handle("/dashboard", dashboardHandler(web))
		mux.Handle("/", webAppHandler(web))
	}

	return mux
}

func webAppHandler(web fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(web))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		requestPath := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if requestPath != "" && strings.Contains(path.Base(requestPath), ".") {
			if _, err := fs.Stat(web, requestPath); err != nil {
				http.NotFound(w, r)
				return
			}
			fileServer.ServeHTTP(w, r)
			return
		}
		serveIndexHTML(web, w, r)
	})
}

func serveIndexHTML(web fs.FS, w http.ResponseWriter, r *http.Request) {
	body, err := fs.ReadFile(web, "index.html")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(body)
}

func dashboardHandler(web fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		body, err := fs.ReadFile(web, "dashboard.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if r.Method == http.MethodHead {
			return
		}
		w.Write(body)
	})
}

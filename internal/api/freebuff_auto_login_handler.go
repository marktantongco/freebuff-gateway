package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/idgen"
	"github.com/marktantongco/freebuff-gateway/internal/proxypool"
	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
)

const (
	defaultFreeBuffAutoLoginTimeoutSeconds = 240

	freeBuffAutoLoginJobRetention = time.Hour

	freeBuffGitHubLoginMethodProtocol = "github_protocol"
)

var freeBuffAutoLoginSensitiveQueryRE = regexp.MustCompile(`(?i)([?&](?:auth_code|code|state|token|authToken|fingerprintHash)=)[^&\s"']+`)

type freeBuffGitHubAutoLoginReq struct {
	Credentials    string `json:"credentials"`
	MethodID       string `json:"method_id,omitempty"`
	Proxy          string `json:"proxy,omitempty"`
	ProxyID        string `json:"proxy_id,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	SkipRefresh    bool   `json:"skip_refresh,omitempty"`
}

type freeBuffGitHubAutoLoginResp struct {
	Status      string                            `json:"status"`
	MethodID    string                            `json:"method_id,omitempty"`
	JobID       string                            `json:"job_id,omitempty"`
	Completed   bool                              `json:"completed"`
	StartedAt   int64                             `json:"started_at,omitempty"`
	CompletedAt int64                             `json:"completed_at,omitempty"`
	Error       string                            `json:"error,omitempty"`
	Imported    []freeBuffGitHubAutoLoginImported `json:"imported"`
	Failures    []freeBuffGitHubAutoLoginFailure  `json:"failures"`
	Accounts    []freeBuffAccountView             `json:"accounts"`
}

type freeBuffAutoLoginJob struct {
	id          string
	methodID    string
	status      string
	startedAt   int64
	completedAt int64
	error       string
	result      *freeBuffGitHubAutoLoginResp
}

type freeBuffGitHubAutoLoginImported struct {
	Username  string `json:"username"`
	AccountID string `json:"account_id"`
	Status    string `json:"status"`
	Refreshed bool   `json:"refreshed"`
	ElapsedMS int    `json:"elapsed_ms"`
}

type freeBuffGitHubAutoLoginFailure struct {
	Username string `json:"username"`
	Status   string `json:"status"`
	Error    string `json:"error"`
}

type freeBuffAutoLoginHTTPError struct {
	status  int
	message string
}

func (e freeBuffAutoLoginHTTPError) Error() string { return e.message }

type freeBuffGitHubProtocolRunner interface {
	RunGitHubProtocolLogin(context.Context, string, channels.Transport, channels.TransportProfile) (*channels.AccountCredentialImport, error)
}

type freeBuffGitHubProtocolStatusError interface {
	ProtocolLoginStatus() string
}

func (h *AdminHandler) FreeBuffGitHubProtocolLogin(w http.ResponseWriter, r *http.Request) {
	h.freeBuffGitHubCredentialLogin(w, r, freeBuffGitHubLoginMethodProtocol)
}

func (h *AdminHandler) freeBuffGitHubCredentialLogin(w http.ResponseWriter, r *http.Request, methodID string) {
	if h.accounts == nil || h.accounts.Repo() == nil {
		writeJSONError(w, http.StatusInternalServerError, "accounts unavailable")
		return
	}

	var req freeBuffGitHubAutoLoginReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	req.Credentials = strings.TrimSpace(req.Credentials)
	items := freeBuffAutoLoginCredentialItems(req.Credentials)
	if len(items) == 0 {
		writeJSONError(w, http.StatusBadRequest, "credentials required")
		return
	}
	req.MethodID = methodID
	if err := normalizeFreeBuffAutoLoginReq(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ProxyID) != "" {
		proxy, err := h.activeProxyRecord(r.Context(), req.ProxyID)
		if err != nil {
			h.writeProxyBindingError(w, err)
			return
		}
		req.ProxyID = proxy.ID
		req.Proxy = proxy.ProxyURL
	}

	result := h.startFreeBuffGitHubAutoLoginJob(req, items)
	writeJSON(w, http.StatusAccepted, result)
}

func (h *AdminHandler) GetFreeBuffGitHubProtocolLogin(w http.ResponseWriter, r *http.Request) {
	h.getFreeBuffGitHubCredentialLogin(w, r)
}

func (h *AdminHandler) getFreeBuffGitHubCredentialLogin(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("jobID"))
	if jobID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing job id")
		return
	}
	result, ok := h.freeBuffGitHubAutoLoginJobSnapshot(jobID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "github login job not found")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func normalizeFreeBuffAutoLoginReq(req *freeBuffGitHubAutoLoginReq) error {
	req.MethodID = strings.TrimSpace(req.MethodID)
	if req.MethodID == "" {
		req.MethodID = freeBuffGitHubLoginMethodProtocol
	}
	if req.MethodID != freeBuffGitHubLoginMethodProtocol {
		return fmt.Errorf("unsupported github login method")
	}
	req.ProxyID = strings.TrimSpace(req.ProxyID)
	req.Proxy = strings.TrimSpace(req.Proxy)
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = defaultFreeBuffAutoLoginTimeoutSeconds
	}
	if req.TimeoutSeconds < 30 {
		req.TimeoutSeconds = 30
	}
	return nil
}

func (h *AdminHandler) runFreeBuffGitHubProtocolLogin(ctx context.Context, req freeBuffGitHubAutoLoginReq, items []string) (*freeBuffGitHubAutoLoginResp, error) {
	if h.registry == nil {
		return nil, freeBuffAutoLoginHTTPError{status: http.StatusInternalServerError, message: "channel registry unavailable"}
	}
	if h.transport == nil {
		return nil, freeBuffAutoLoginHTTPError{status: http.StatusInternalServerError, message: "transport unavailable"}
	}
	adapter, ok := h.registry.Get(freebuffstate.ChannelID)
	if !ok {
		return nil, freeBuffAutoLoginHTTPError{status: http.StatusInternalServerError, message: "freebuff channel not found"}
	}
	runner, ok := adapter.(freeBuffGitHubProtocolRunner)
	if !ok {
		return nil, freeBuffAutoLoginHTTPError{status: http.StatusInternalServerError, message: "freebuff channel does not support github protocol login"}
	}

	resp := &freeBuffGitHubAutoLoginResp{
		Status:   "ok",
		MethodID: req.MethodID,
		Imported: []freeBuffGitHubAutoLoginImported{},
		Failures: []freeBuffGitHubAutoLoginFailure{},
		Accounts: []freeBuffAccountView{},
	}
	for _, item := range items {
		started := time.Now()
		username := freeBuffAutoLoginCredentialUsername(item)
		itemCtx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
		result, err := runner.RunGitHubProtocolLogin(itemCtx, item, h.transport, channels.TransportProfile{
			ProxyURL: req.Proxy,
		})
		cancel()
		if err != nil {
			resp.Failures = append(resp.Failures, freeBuffGitHubAutoLoginFailure{
				Username: username,
				Status:   freeBuffGitHubProtocolFailureStatus(err),
				Error:    freeBuffAutoLoginSanitizeText(err.Error()),
			})
			continue
		}
		accountID, created, err := h.upsertFreeBuffGitHubProtocolAccount(ctx, username, result, req.ProxyID)
		if err != nil {
			resp.Failures = append(resp.Failures, freeBuffGitHubAutoLoginFailure{
				Username: username,
				Status:   "failed",
				Error:    "account import failed",
			})
			continue
		}
		refreshed := false
		if !req.SkipRefresh {
			if refreshErr := h.refreshImportedFreeBuffAccount(ctx, accountID); refreshErr != nil {
				resp.Failures = append(resp.Failures, freeBuffGitHubAutoLoginFailure{
					Username: username,
					Status:   "refresh_failed",
					Error:    freeBuffAutoLoginSanitizeText(refreshErr.Error()),
				})
			} else if h.freebuffStates != nil {
				refreshed = true
			}
		}
		status := "updated"
		if created {
			status = "created"
		}
		resp.Imported = append(resp.Imported, freeBuffGitHubAutoLoginImported{
			Username:  username,
			AccountID: accountID,
			Status:    status,
			Refreshed: refreshed,
			ElapsedMS: int(time.Since(started).Milliseconds()),
		})
	}
	resp.Status = freeBuffProtocolLoginFinalStatus(len(resp.Imported), len(resp.Failures))
	return resp, nil
}

func (h *AdminHandler) upsertFreeBuffGitHubProtocolAccount(ctx context.Context, githubLogin string, result *channels.AccountCredentialImport, proxyID string) (string, bool, error) {
	if result == nil || strings.TrimSpace(result.Credential) == "" {
		return "", false, errors.New("freebuff github protocol login returned no token")
	}
	records, err := h.accounts.Repo().ListByChannel(freebuffstate.ChannelID)
	if err != nil {
		return "", false, err
	}
	name := firstNonEmptyTrimmed(result.Name, githubLogin, "freebuff-github-protocol")
	metadata := sanitizedFreeBuffProtocolMetadata(result.Metadata)
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["auth_method"] = freeBuffGitHubLoginMethodProtocol
	if strings.TrimSpace(githubLogin) != "" {
		metadata["github_login"] = strings.TrimSpace(githubLogin)
	}
	if proxyID != "" {
		metadata = proxypool.SetProxyID(metadata, proxyID)
	}
	if existing := findFreeBuffGitHubProtocolAccount(records, githubLogin, result); existing != nil {
		existing.Name = name
		existing.Credential = strings.TrimSpace(result.Credential)
		existing.IsActive = true
		existing.Metadata = mergeMetadata(existing.Metadata, metadata)
		if proxyID != "" {
			existing.Metadata = proxypool.SetProxyID(existing.Metadata, proxyID)
		}
		if err := h.accounts.Repo().Update(existing); err != nil {
			return "", false, err
		}
		return existing.ID, false, nil
	}
	rec := &accounts.Record{
		ChannelID:  freebuffstate.ChannelID,
		Name:       name,
		Credential: strings.TrimSpace(result.Credential),
		IsActive:   true,
		Metadata:   metadata,
	}
	if err := h.accounts.Repo().Create(rec); err != nil {
		return "", false, err
	}
	return rec.ID, true, nil
}

func (h *AdminHandler) refreshImportedFreeBuffAccount(ctx context.Context, accountID string) error {
	if h.freebuffStates == nil {
		return nil
	}
	rec, err := h.accounts.Repo().Get(accountID)
	if err != nil {
		return err
	}
	return h.refreshFreeBuffRecord(ctx, rec)
}

func findFreeBuffGitHubProtocolAccount(records []*accounts.Record, githubLogin string, result *channels.AccountCredentialImport) *accounts.Record {
	loginLower := strings.ToLower(strings.TrimSpace(githubLogin))
	token := strings.TrimSpace(result.Credential)
	nameLower := strings.ToLower(strings.TrimSpace(result.Name))
	userID := firstNonEmptyTrimmed(
		metadataString(result.Metadata, "github_user_id"),
		metadataString(result.Metadata, "freebuff_user_id"),
	)
	emailLower := strings.ToLower(firstNonEmptyTrimmed(
		metadataString(result.Metadata, "github_user_email"),
		metadataString(result.Metadata, "freebuff_user_email"),
	))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		if token != "" && rec.Credential == token {
			return rec
		}
		recNameLower := strings.ToLower(strings.TrimSpace(rec.Name))
		if loginLower != "" && recNameLower == loginLower {
			return rec
		}
		if nameLower != "" && recNameLower == nameLower {
			return rec
		}
		if loginLower != "" && strings.ToLower(strings.TrimSpace(metadataString(rec.Metadata, "github_login"))) == loginLower {
			return rec
		}
		if userID != "" && (strings.TrimSpace(metadataString(rec.Metadata, "github_user_id")) == userID || strings.TrimSpace(metadataString(rec.Metadata, "freebuff_user_id")) == userID) {
			return rec
		}
		if emailLower != "" && (strings.ToLower(strings.TrimSpace(metadataString(rec.Metadata, "github_user_email"))) == emailLower || strings.ToLower(strings.TrimSpace(metadataString(rec.Metadata, "freebuff_user_email"))) == emailLower) {
			return rec
		}
	}
	return nil
}

func freeBuffGitHubProtocolFailureStatus(err error) string {
	var statusErr freeBuffGitHubProtocolStatusError
	if errors.As(err, &statusErr) && strings.TrimSpace(statusErr.ProtocolLoginStatus()) != "" {
		return strings.TrimSpace(statusErr.ProtocolLoginStatus())
	}
	return "failed"
}

func freeBuffProtocolLoginFinalStatus(imported, failures int) string {
	switch {
	case imported > 0 && failures > 0:
		return "partial_failed"
	case imported > 0:
		return "ok"
	default:
		return "failed"
	}
}

func sanitizedFreeBuffProtocolMetadata(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" || freeBuffProtocolMetadataKeySensitive(key) {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func freeBuffProtocolMetadataKeySensitive(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, marker := range []string{
		"password",
		"secret",
		"token",
		"cookie",
		"csrf",
		"totp",
		"otp",
		"auth_code",
		"state",
	} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func (h *AdminHandler) startFreeBuffGitHubAutoLoginJob(req freeBuffGitHubAutoLoginReq, items []string) freeBuffGitHubAutoLoginResp {
	now := time.Now()
	job := &freeBuffAutoLoginJob{
		id:        idgen.New(),
		methodID:  req.MethodID,
		status:    "running",
		startedAt: now.Unix(),
	}
	h.autoLoginMu.Lock()
	if h.autoLoginJobs == nil {
		h.autoLoginJobs = make(map[string]*freeBuffAutoLoginJob)
	}
	h.pruneFreeBuffAutoLoginJobsLocked(now)
	h.autoLoginJobs[job.id] = job
	result := freeBuffGitHubAutoLoginJobResponse(job)
	h.autoLoginMu.Unlock()

	metadata := map[string]any{
		"account_count":   len(items),
		"method_id":       req.MethodID,
		"timeout_seconds": req.TimeoutSeconds,
		"skip_refresh":    req.SkipRefresh,
	}
	if req.ProxyID != "" {
		metadata["proxy_id"] = req.ProxyID
	}
	if redactedProxy := proxypool.RedactProxyURL(req.Proxy); redactedProxy != "" {
		metadata["proxy"] = redactedProxy
	}
	h.appendFreeBuffProtocolLoginLog(job.id, systemlogs.LevelInfo, "job_accepted", freeBuffGitHubLoginLabel(req.MethodID)+" job accepted", metadata)

	go h.runFreeBuffGitHubAutoLoginJob(job.id, req, len(items), append([]string(nil), items...))
	return result
}

func (h *AdminHandler) runFreeBuffGitHubAutoLoginJob(jobID string, req freeBuffGitHubAutoLoginReq, accountCount int, items []string) {
	status := "failed"
	publicError := ""
	var result *freeBuffGitHubAutoLoginResp

	h.appendFreeBuffProtocolLoginLog(jobID, systemlogs.LevelInfo, "protocol_started", "GitHub protocol-login started", map[string]any{
		"account_count": accountCount,
		"method_id":     req.MethodID,
		"proxy_id":      req.ProxyID,
	})
	out, err := h.runFreeBuffGitHubProtocolLogin(context.Background(), req, items)
	if err != nil {
		publicError = freeBuffAutoLoginPublicError(err)
		log.Printf("freebuff github login job %s method=%s failed: %s", jobID, req.MethodID, publicError)
		h.appendFreeBuffProtocolLoginLog(jobID, systemlogs.LevelError, "job_failed", publicError, map[string]any{
			"account_count": accountCount,
			"method_id":     req.MethodID,
		})
	} else {
		result = out
		result.MethodID = req.MethodID
		redactFreeBuffAutoLoginSecrets(result, items)
		summaryMetadata := freeBuffAutoLoginSummaryMetadata(result, accountCount)
		h.appendFreeBuffProtocolLoginLog(jobID, freeBuffAutoLoginStatusLogLevel(result.Status, len(result.Failures)), "protocol_completed", "GitHub protocol-login completed", summaryMetadata)
		views, err := h.freeBuffAccountViewsByID(context.Background(), freeBuffAutoLoginAccountIDs(result.Imported))
		if err != nil {
			publicError = "load imported accounts failed"
			log.Printf("freebuff github login job %s method=%s failed while loading accounts: %v", jobID, req.MethodID, err)
			h.appendFreeBuffProtocolLoginLog(jobID, systemlogs.LevelError, "job_failed", publicError, map[string]any{
				"account_count": accountCount,
				"cause":         err.Error(),
				"method_id":     req.MethodID,
			})
		} else {
			result.Accounts = views
			status = strings.TrimSpace(result.Status)
			if status == "" {
				status = "completed"
			}
			h.appendFreeBuffProtocolLoginLog(jobID, freeBuffAutoLoginStatusLogLevel(status, len(result.Failures)), freeBuffAutoLoginStatusEvent(status, len(result.Failures)), freeBuffGitHubLoginLabel(req.MethodID)+" job finished", freeBuffAutoLoginSummaryMetadata(result, accountCount))
			log.Printf(
				"freebuff github login job %s method=%s completed status=%s imported=%d failures=%d first_error=%q",
				jobID,
				req.MethodID,
				status,
				len(result.Imported),
				len(result.Failures),
				freeBuffAutoLoginFirstFailure(result.Failures),
			)
		}
	}

	completedAt := time.Now().Unix()
	h.autoLoginMu.Lock()
	if job, ok := h.autoLoginJobs[jobID]; ok {
		job.status = status
		job.completedAt = completedAt
		job.error = publicError
		job.result = result
	}
	h.autoLoginMu.Unlock()
}

func freeBuffGitHubLoginLabel(methodID string) string {
	if methodID == freeBuffGitHubLoginMethodProtocol {
		return "GitHub protocol-login"
	}
	return "GitHub login"
}

func freeBuffAutoLoginSummaryMetadata(result *freeBuffGitHubAutoLoginResp, accountCount int) map[string]any {
	metadata := map[string]any{
		"account_count": accountCount,
	}
	if result == nil {
		return metadata
	}
	status := strings.TrimSpace(result.Status)
	if status != "" {
		metadata["status"] = status
	}
	if result.MethodID != "" {
		metadata["method_id"] = result.MethodID
	}
	metadata["imported"] = len(result.Imported)
	metadata["failures"] = len(result.Failures)
	if firstFailure := freeBuffAutoLoginFirstFailure(result.Failures); firstFailure != "" {
		metadata["first_failure"] = firstFailure
	}
	if samples := freeBuffAutoLoginFailureSamples(result.Failures, 5); len(samples) > 0 {
		metadata["failure_samples"] = samples
	}
	return metadata
}

func freeBuffAutoLoginStatusLogLevel(status string, failureCount int) string {
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "failed" || status == "error" {
		return systemlogs.LevelError
	}
	if failureCount > 0 || status == "partial_failed" || status == "partial" {
		return systemlogs.LevelWarn
	}
	return systemlogs.LevelInfo
}

func freeBuffAutoLoginStatusEvent(status string, failureCount int) string {
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "failed" || status == "error" {
		return "job_failed"
	}
	if failureCount > 0 || status == "partial_failed" || status == "partial" {
		return "job_partial_failed"
	}
	return "job_completed"
}

func (h *AdminHandler) freeBuffGitHubAutoLoginJobSnapshot(jobID string) (freeBuffGitHubAutoLoginResp, bool) {
	h.autoLoginMu.Lock()
	defer h.autoLoginMu.Unlock()
	job, ok := h.autoLoginJobs[jobID]
	if !ok {
		return freeBuffGitHubAutoLoginResp{}, false
	}
	return freeBuffGitHubAutoLoginJobResponse(job), true
}

func (h *AdminHandler) pruneFreeBuffAutoLoginJobsLocked(now time.Time) {
	if len(h.autoLoginJobs) == 0 {
		return
	}
	cutoff := now.Add(-freeBuffAutoLoginJobRetention).Unix()
	for id, job := range h.autoLoginJobs {
		if job != nil && job.completedAt > 0 && job.completedAt < cutoff {
			delete(h.autoLoginJobs, id)
		}
	}
}

func freeBuffGitHubAutoLoginJobResponse(job *freeBuffAutoLoginJob) freeBuffGitHubAutoLoginResp {
	resp := freeBuffGitHubAutoLoginResp{
		Status:    "running",
		Imported:  []freeBuffGitHubAutoLoginImported{},
		Failures:  []freeBuffGitHubAutoLoginFailure{},
		Accounts:  []freeBuffAccountView{},
		Completed: false,
	}
	if job == nil {
		return resp
	}
	resp.JobID = job.id
	resp.MethodID = job.methodID
	resp.Status = job.status
	resp.StartedAt = job.startedAt
	resp.CompletedAt = job.completedAt
	resp.Error = job.error
	resp.Completed = job.completedAt > 0
	if job.result == nil {
		if resp.Status == "" {
			resp.Status = "running"
		}
		return resp
	}
	resp.Status = job.result.Status
	if job.result.MethodID != "" {
		resp.MethodID = job.result.MethodID
	}
	if strings.TrimSpace(resp.Status) == "" {
		resp.Status = job.status
	}
	resp.Imported = append([]freeBuffGitHubAutoLoginImported(nil), job.result.Imported...)
	resp.Failures = append([]freeBuffGitHubAutoLoginFailure(nil), job.result.Failures...)
	resp.Accounts = append([]freeBuffAccountView(nil), job.result.Accounts...)
	return resp
}

func freeBuffAutoLoginPublicError(err error) string {
	var httpErr freeBuffAutoLoginHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.message
	}
	return "freebuff github protocol login failed"
}

func freeBuffAutoLoginFirstFailure(failures []freeBuffGitHubAutoLoginFailure) string {
	if len(failures) == 0 {
		return ""
	}
	msg := freeBuffAutoLoginSanitizeText(failures[0].Error)
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

func freeBuffAutoLoginFailureSamples(failures []freeBuffGitHubAutoLoginFailure, limit int) []map[string]string {
	if limit <= 0 || len(failures) == 0 {
		return nil
	}
	out := make([]map[string]string, 0, min(limit, len(failures)))
	for i, failure := range failures {
		if i >= limit {
			break
		}
		out = append(out, map[string]string{
			"username": strings.TrimSpace(failure.Username),
			"status":   strings.TrimSpace(failure.Status),
			"error":    freeBuffAutoLoginSanitizeText(failure.Error),
		})
	}
	return out
}

func freeBuffAutoLoginSanitizeText(value string) string {
	value = strings.TrimSpace(value)
	return freeBuffAutoLoginSensitiveQueryRE.ReplaceAllString(value, "$1[redacted]")
}

func (h *AdminHandler) freeBuffAccountViewsByID(ctx context.Context, ids []string) ([]freeBuffAccountView, error) {
	if len(ids) == 0 {
		return []freeBuffAccountView{}, nil
	}
	if h.accounts == nil || h.accounts.Repo() == nil {
		return nil, errors.New("accounts unavailable")
	}
	runtimes := h.accountRuntimeMap()
	out := make([]freeBuffAccountView, 0, len(ids))
	seen := map[string]struct{}{}
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		rec, err := h.accounts.Repo().Get(id)
		if errors.Is(err, accounts.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if rec.ChannelID != freebuffstate.ChannelID {
			continue
		}
		view, err := h.freeBuffAccountView(ctx, rec, runtimes)
		if err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, nil
}

func freeBuffAutoLoginAccountIDs(imported []freeBuffGitHubAutoLoginImported) []string {
	out := make([]string, 0, len(imported))
	for _, item := range imported {
		out = append(out, item.AccountID)
	}
	return out
}

func freeBuffAutoLoginCredentialItems(text string) []string {
	items := []string{}
	normalized := strings.NewReplacer("\r\n", "\n", "\r", "\n", "，", ",").Replace(text)
	for _, line := range strings.Split(normalized, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, item := range strings.Split(line, ",") {
			item = strings.TrimSpace(item)
			if item != "" {
				items = append(items, item)
			}
		}
	}
	return items
}

func freeBuffAutoLoginCredentialUsername(item string) string {
	parts := strings.Split(strings.TrimSpace(item), "----")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func metadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func redactFreeBuffAutoLoginSecrets(result *freeBuffGitHubAutoLoginResp, items []string) {
	if result == nil {
		return
	}
	secrets := freeBuffAutoLoginSecretFragments(items)
	for i := range result.Failures {
		for _, secret := range secrets {
			result.Failures[i].Error = strings.ReplaceAll(result.Failures[i].Error, secret, "[redacted]")
		}
		result.Failures[i].Error = freeBuffAutoLoginSanitizeText(result.Failures[i].Error)
	}
}

func freeBuffAutoLoginSecretFragments(items []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, item := range items {
		parts := strings.Split(item, "----")
		if len(parts) != 3 {
			continue
		}
		for _, secret := range []string{strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])} {
			if len(secret) < 3 {
				continue
			}
			if _, ok := seen[secret]; ok {
				continue
			}
			seen[secret] = struct{}{}
			out = append(out, secret)
		}
	}
	return out
}

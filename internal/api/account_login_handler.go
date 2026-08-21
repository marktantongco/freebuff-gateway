package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

const maxAdminJSONBodyBytes = 32 << 20

type accountLoginStartReq struct {
	MethodID    string         `json:"method_id"`
	Name        string         `json:"name"`
	Priority    int            `json:"priority"`
	RPMLimit    int            `json:"rpm_limit"`
	QuotaTotal  int64          `json:"quota_total"`
	QuotaPeriod string         `json:"quota_period"`
	IsActive    *bool          `json:"is_active"`
	Metadata    map[string]any `json:"metadata"`
}

type accountLoginCompleteReq struct {
	CallbackURL string `json:"callback_url"`
}

type accountLoginDraft struct {
	ChannelID        string
	MethodID         string
	Name             string
	Priority         int
	RPMLimit         int
	QuotaTotal       int64
	QuotaPeriod      string
	QuotaUsed        int64
	QuotaPeriodStart int64
	IsActive         bool
	Metadata         map[string]any
	ExpiresAt        int64
}

type accountLoginStatusResp struct {
	Status    string           `json:"status"`
	Completed bool             `json:"completed"`
	Account   *accounts.Record `json:"account,omitempty"`
	UserName  string           `json:"user_name,omitempty"`
	UserEmail string           `json:"user_email,omitempty"`
	UserID    string           `json:"user_id,omitempty"`
}

func accountAuthMethods(a channels.ChannelAdapter) []channels.AccountAuthMethod {
	if flow, ok := a.(channels.AccountOnboarder); ok {
		methods := flow.AccountAuthMethods()
		if len(methods) > 0 {
			return append([]channels.AccountAuthMethod(nil), methods...)
		}
	}
	return channels.DefaultAccountAuthMethods()
}

func (h *AdminHandler) StartAccountLogin(w http.ResponseWriter, r *http.Request) {
	channelID := strings.TrimSpace(r.PathValue("id"))
	if channelID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	adapter, ok := h.registry.Get(channelID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	flow, ok := adapter.(channels.AccountOnboarder)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "channel does not support account login")
		return
	}
	if h.transport == nil {
		writeJSONError(w, http.StatusInternalServerError, "transport unavailable")
		return
	}

	var req accountLoginStartReq
	if err := decodeAdminJSON(r, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	req.MethodID = strings.TrimSpace(req.MethodID)
	req.Name = strings.TrimSpace(req.Name)
	if req.MethodID == "" {
		writeJSONError(w, http.StatusBadRequest, "method_id required")
		return
	}

	method, ok := findAuthMethod(accountAuthMethods(adapter), req.MethodID)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "unknown auth method")
		return
	}
	if method.Kind != channels.AccountAuthKindExternalLink {
		writeJSONError(w, http.StatusBadRequest, "auth method does not start external login")
		return
	}

	active := true
	if req.IsActive != nil {
		active = *req.IsActive
	}
	quotaTotal, quotaPeriod, quotaUsed, quotaStart, err := normalizeQuota(req.QuotaTotal, req.QuotaPeriod, time.Now())
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	start, err := flow.StartAccountLogin(r.Context(), req.MethodID, h.transport)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	if start == nil || strings.TrimSpace(start.SessionID) == "" {
		writeJSONError(w, http.StatusBadGateway, "login session missing")
		return
	}
	if start.CompletionMode == "" {
		start.CompletionMode = method.CompletionMode
	}
	if start.CompletionMode == "" {
		start.CompletionMode = channels.AccountLoginCompletionPoll
	}

	h.saveLoginDraft(channelID, start.SessionID, accountLoginDraft{
		ChannelID:        channelID,
		MethodID:         req.MethodID,
		Name:             req.Name,
		Priority:         req.Priority,
		RPMLimit:         req.RPMLimit,
		QuotaTotal:       quotaTotal,
		QuotaPeriod:      quotaPeriod,
		QuotaUsed:        quotaUsed,
		QuotaPeriodStart: quotaStart,
		IsActive:         active,
		Metadata:         cloneMetadata(req.Metadata),
		ExpiresAt:        start.ExpiresAt,
	})
	writeJSON(w, http.StatusOK, start)
}

func (h *AdminHandler) PollAccountLogin(w http.ResponseWriter, r *http.Request) {
	h.finishAccountLogin(w, r, false)
}

func (h *AdminHandler) CompleteAccountLogin(w http.ResponseWriter, r *http.Request) {
	h.finishAccountLogin(w, r, true)
}

func (h *AdminHandler) finishAccountLogin(w http.ResponseWriter, r *http.Request, withCallback bool) {
	channelID := strings.TrimSpace(r.PathValue("id"))
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	if channelID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing id")
		return
	}
	if sessionID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing sessionID")
		return
	}
	adapter, ok := h.registry.Get(channelID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "channel not found")
		return
	}
	flow, ok := adapter.(channels.AccountOnboarder)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "channel does not support account login")
		return
	}
	if h.transport == nil {
		writeJSONError(w, http.StatusInternalServerError, "transport unavailable")
		return
	}
	draft, ok := h.loginDraft(channelID, sessionID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "login session not found")
		return
	}
	if draft.ExpiresAt > 0 && time.Now().Unix() > draft.ExpiresAt {
		h.deleteLoginDraft(channelID, sessionID)
		writeJSON(w, http.StatusOK, accountLoginStatusResp{Status: "expired", Completed: false})
		return
	}

	var result *channels.AccountLoginResult
	var err error
	if withCallback {
		var req accountLoginCompleteReq
		if err := decodeAdminJSON(r, &req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if strings.TrimSpace(req.CallbackURL) == "" {
			writeJSONError(w, http.StatusBadRequest, "callback_url required")
			return
		}
		result, err = flow.CompleteAccountLogin(r.Context(), sessionID, channels.AccountLoginCompleteRequest{
			CallbackURL: strings.TrimSpace(req.CallbackURL),
		}, h.transport)
	} else {
		result, err = flow.PollAccountLogin(r.Context(), sessionID, h.transport)
	}
	if err != nil {
		if errors.Is(err, channels.ErrAccountLoginPending) {
			writeJSON(w, http.StatusOK, accountLoginStatusResp{Status: "pending", Completed: false})
			return
		}
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	if result == nil || !result.Completed {
		writeJSON(w, http.StatusOK, accountLoginStatusResp{Status: "pending", Completed: false})
		return
	}
	if strings.TrimSpace(result.Credential) == "" {
		writeJSONError(w, http.StatusBadGateway, "login completed without credential")
		return
	}

	rec := &accounts.Record{
		ChannelID:        draft.ChannelID,
		Name:             accountLoginRecordName(draft, result),
		Credential:       result.Credential,
		Priority:         draft.Priority,
		RPMLimit:         draft.RPMLimit,
		QuotaTotal:       draft.QuotaTotal,
		QuotaPeriod:      draft.QuotaPeriod,
		QuotaUsed:        draft.QuotaUsed,
		QuotaPeriodStart: draft.QuotaPeriodStart,
		IsActive:         draft.IsActive,
		Metadata:         loginMetadata(draft, result),
	}
	if err := h.accounts.Repo().Create(rec); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.deleteLoginDraft(channelID, sessionID)
	writeJSON(w, http.StatusOK, accountLoginStatusResp{
		Status:    "completed",
		Completed: true,
		Account:   rec,
		UserName:  result.UserName,
		UserEmail: result.UserEmail,
		UserID:    result.UserID,
	})
}

func decodeAdminJSON(r *http.Request, v any) error {
	return json.NewDecoder(io.LimitReader(r.Body, maxAdminJSONBodyBytes)).Decode(v)
}

func findAuthMethod(methods []channels.AccountAuthMethod, id string) (channels.AccountAuthMethod, bool) {
	for _, method := range methods {
		if method.ID == id {
			return method, true
		}
	}
	return channels.AccountAuthMethod{}, false
}

func (h *AdminHandler) saveLoginDraft(channelID, sessionID string, draft accountLoginDraft) {
	h.loginMu.Lock()
	h.loginDrafts[loginDraftKey(channelID, sessionID)] = draft
	h.loginMu.Unlock()
}

func (h *AdminHandler) loginDraft(channelID, sessionID string) (accountLoginDraft, bool) {
	h.loginMu.Lock()
	defer h.loginMu.Unlock()
	draft, ok := h.loginDrafts[loginDraftKey(channelID, sessionID)]
	return draft, ok
}

func (h *AdminHandler) deleteLoginDraft(channelID, sessionID string) {
	h.loginMu.Lock()
	delete(h.loginDrafts, loginDraftKey(channelID, sessionID))
	h.loginMu.Unlock()
}

func loginDraftKey(channelID, sessionID string) string {
	return channelID + "|" + sessionID
}

func cloneMetadata(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func loginMetadata(draft accountLoginDraft, result *channels.AccountLoginResult) map[string]any {
	out := mergeMetadata(draft.Metadata, result.Metadata)
	if out == nil {
		out = map[string]any{}
	}
	if _, ok := out["auth_method"]; !ok {
		out["auth_method"] = draft.MethodID
	}
	return out
}

func mergeMetadata(base, overlay map[string]any) map[string]any {
	out := cloneMetadata(base)
	if len(overlay) == 0 {
		return out
	}
	if out == nil {
		out = map[string]any{}
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}

func accountLoginRecordName(draft accountLoginDraft, result *channels.AccountLoginResult) string {
	for _, candidate := range []string{
		result.UserName,
		result.UserEmail,
		result.UserID,
		draft.Name,
		draft.ChannelID + "-" + draft.MethodID,
		draft.ChannelID,
	} {
		if name := strings.TrimSpace(candidate); name != "" {
			return name
		}
	}
	return "external-account"
}

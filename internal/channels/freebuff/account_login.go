package freebuff

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"freebuff-reverse/internal/channels"
)

const (
	freebuffCredentialMethodID = "credential"
	freebuffGitHubMethodID     = "github"
	freebuffLoginUserAgent     = "ai-sdk/openai-compatible/2.0.42/codebuff"
	freebuffLoginPollSeconds   = 3
)

type accountLoginSession struct {
	FingerprintID   string
	FingerprintHash string
	ExpiresAtRaw    int64
	ExpiresAtUnix   int64
	LoginURL        string
}

func (a *Adapter) AccountAuthMethods() []channels.AccountAuthMethod {
	return []channels.AccountAuthMethod{
		{
			ID:                    freebuffCredentialMethodID,
			Label:                 "Credential",
			Kind:                  channels.AccountAuthKindCredential,
			RequiresCredential:    true,
			CredentialInputMode:   channels.AccountCredentialInputModeImport,
			CredentialPlaceholder: "Paste FreeBuff credential JSON, authToken, or token",
			CredentialImportHint:  "Paste the exported credential JSON or a raw FreeBuff auth token.",
			CredentialDropTitle:   "Drop credential file or click to select",
			CredentialDropHint:    "Supports .json / .txt",
		},
		{
			ID:                 freebuffGitHubMethodID,
			Label:              "GitHub",
			Description:        "Authorize with FreeBuff GitHub login",
			Kind:               channels.AccountAuthKindExternalLink,
			CompletionMode:     channels.AccountLoginCompletionPoll,
			RequiresCredential: false,
		},
		{
			ID:                    freebuffGitHubProtocolMethodID,
			Label:                 "GitHub Protocol",
			Description:           "Batch login FreeBuff with the protocol-level GitHub password and TOTP flow",
			Kind:                  channels.AccountAuthKindCredential,
			RequiresCredential:    true,
			CredentialInputMode:   channels.AccountCredentialInputModeGitHubProtocol,
			CredentialPlaceholder: "github_user----github_password----totp_secret",
			CredentialImportHint:  "Paste one GitHub credential triple per line. The protocol path sends credentials only at runtime and stores only the FreeBuff token.",
			CredentialDropTitle:   "Drop GitHub credential file or click to select",
			CredentialDropHint:    "One github_user----github_password----totp_secret per line",
		},
	}
}

func (a *Adapter) ImportAccountCredential(raw string) (*channels.AccountCredentialImport, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("freebuff: credential required")
	}

	credential := trimmed
	name := ""
	email := ""
	userID := ""
	metadata := map[string]any{"auth_method": freebuffCredentialMethodID}

	var parsed any
	if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
		switch value := parsed.(type) {
		case string:
			credential = strings.TrimSpace(value)
		case map[string]any:
			var ok bool
			credential, name, email, userID, ok = freebuffCredentialFields(value)
			if !ok {
				return nil, fmt.Errorf("freebuff: credential JSON missing token")
			}
		case []any:
			if len(value) != 1 {
				return nil, fmt.Errorf("freebuff: credential import expects one account")
			}
			obj, ok := value[0].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("freebuff: credential import item must be an object")
			}
			credential, name, email, userID, ok = freebuffCredentialFields(obj)
			if !ok {
				return nil, fmt.Errorf("freebuff: credential JSON missing token")
			}
		default:
			return nil, fmt.Errorf("freebuff: credential JSON must be an object or token string")
		}
	}

	credential = strings.TrimSpace(credential)
	if credential == "" {
		return nil, fmt.Errorf("freebuff: credential required")
	}
	displayName := name
	if name == "" {
		name = firstNonEmpty(email, userID, "freebuff-credential")
	}
	if userID != "" {
		metadata["freebuff_user_id"] = userID
	}
	if displayName != "" {
		metadata["freebuff_user_name"] = displayName
	}
	if email != "" {
		metadata["freebuff_user_email"] = email
	}

	return &channels.AccountCredentialImport{
		Name:       name,
		Credential: credential,
		Metadata:   metadata,
	}, nil
}

func (a *Adapter) StartAccountLogin(ctx context.Context, methodID string, tp channels.Transport) (*channels.AccountLoginStartResult, error) {
	if methodID != freebuffGitHubMethodID {
		return nil, fmt.Errorf("freebuff: unsupported account login method %q", methodID)
	}
	if tp == nil {
		return nil, fmt.Errorf("freebuff: nil transport")
	}
	return a.startGitHubLogin(ctx, tp)
}

func (a *Adapter) PollAccountLogin(ctx context.Context, sessionID string, tp channels.Transport) (*channels.AccountLoginResult, error) {
	if tp == nil {
		return nil, fmt.Errorf("freebuff: nil transport")
	}
	return a.pollGitHubLogin(ctx, sessionID, tp)
}

func (a *Adapter) CompleteAccountLogin(_ context.Context, _ string, _ channels.AccountLoginCompleteRequest, _ channels.Transport) (*channels.AccountLoginResult, error) {
	return nil, fmt.Errorf("freebuff: manual callback login is not supported")
}

func (a *Adapter) startGitHubLogin(ctx context.Context, tp channels.Transport) (*channels.AccountLoginStartResult, error) {
	return a.startGitHubLoginWithProfile(ctx, tp, channels.TransportProfile{})
}

func (a *Adapter) startGitHubLoginWithProfile(ctx context.Context, tp channels.Transport, profile channels.TransportProfile) (*channels.AccountLoginStartResult, error) {
	fingerprintID, err := newFingerprintID()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]any{"fingerprintId": fingerprintID})
	if err != nil {
		return nil, fmt.Errorf("freebuff: marshal login code request: %w", err)
	}
	resp, err := tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodPost,
		URL:              joinURL(a.authBaseURL, "/api/auth/cli/code"),
		Headers:          freebuffLoginHeaders(true),
		Body:             payload,
		Timeout:          30 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffLoginTransportProfile(true), profile),
	})
	if err != nil {
		return nil, fmt.Errorf("freebuff: start login: %w", err)
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return nil, fmt.Errorf("freebuff: start login failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	var decoded struct {
		FingerprintID   string `json:"fingerprintId"`
		FingerprintHash string `json:"fingerprintHash"`
		LoginURL        string `json:"loginUrl"`
		ExpiresAt       int64  `json:"expiresAt"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return nil, fmt.Errorf("freebuff: decode login code: %w", err)
	}
	if decoded.FingerprintHash == "" || decoded.LoginURL == "" || decoded.ExpiresAt <= 0 {
		return nil, fmt.Errorf("freebuff: login code response missing fields")
	}
	if decoded.FingerprintID != "" {
		fingerprintID = decoded.FingerprintID
	}
	expiresAtUnix := freebuffExpiresAtUnix(decoded.ExpiresAt)
	a.saveLoginSession(accountLoginSession{
		FingerprintID:   fingerprintID,
		FingerprintHash: decoded.FingerprintHash,
		ExpiresAtRaw:    decoded.ExpiresAt,
		ExpiresAtUnix:   expiresAtUnix,
		LoginURL:        decoded.LoginURL,
	})
	return &channels.AccountLoginStartResult{
		SessionID:        fingerprintID,
		LoginURL:         decoded.LoginURL,
		ExpiresAt:        expiresAtUnix,
		PollAfterSeconds: freebuffLoginPollSeconds,
		CompletionMode:   channels.AccountLoginCompletionPoll,
	}, nil
}

func (a *Adapter) pollGitHubLogin(ctx context.Context, sessionID string, tp channels.Transport) (*channels.AccountLoginResult, error) {
	return a.pollGitHubLoginWithProfile(ctx, sessionID, tp, channels.TransportProfile{})
}

func (a *Adapter) pollGitHubLoginWithProfile(ctx context.Context, sessionID string, tp channels.Transport, profile channels.TransportProfile) (*channels.AccountLoginResult, error) {
	session, ok := a.loginSession(sessionID)
	if !ok {
		return nil, fmt.Errorf("freebuff: login session not found")
	}
	if session.ExpiresAtUnix > 0 && time.Now().Unix() > session.ExpiresAtUnix {
		a.deleteLoginSession(sessionID)
		return nil, fmt.Errorf("freebuff: login session expired")
	}
	resp, err := tp.Do(ctx, &channels.OutboundRequest{
		Method:           http.MethodGet,
		URL:              a.loginStatusURL(session),
		Headers:          freebuffLoginHeaders(false),
		Timeout:          30 * time.Second,
		TransportProfile: mergeFreeBuffProxyProfile(freebuffLoginTransportProfile(false), profile),
	})
	if err != nil {
		return nil, fmt.Errorf("freebuff: poll login: %w", err)
	}
	if resp.Status >= 500 {
		return nil, fmt.Errorf("freebuff: poll login failed: status %d: %s", resp.Status, string(resp.BodyPreview))
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return &channels.AccountLoginResult{Completed: false}, nil
	}

	var decoded struct {
		AuthToken string `json:"authToken"`
		User      struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			Email     string `json:"email"`
			AuthToken string `json:"authToken"`
		} `json:"user"`
	}
	dec := json.NewDecoder(bytes.NewReader(resp.Body))
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("freebuff: decode login status: %w", err)
	}
	token := strings.TrimSpace(decoded.AuthToken)
	if token == "" {
		token = strings.TrimSpace(decoded.User.AuthToken)
	}
	if token == "" {
		return &channels.AccountLoginResult{Completed: false}, nil
	}
	a.deleteLoginSession(sessionID)
	metadata := map[string]any{
		"auth_method":             freebuffGitHubMethodID,
		"freebuff_fingerprint_id": session.FingerprintID,
	}
	if decoded.User.ID != "" {
		metadata["github_user_id"] = decoded.User.ID
	}
	if decoded.User.Name != "" {
		metadata["github_user_name"] = decoded.User.Name
	}
	if decoded.User.Email != "" {
		metadata["github_user_email"] = decoded.User.Email
	}
	return &channels.AccountLoginResult{
		Completed:  true,
		Credential: token,
		UserName:   decoded.User.Name,
		UserEmail:  decoded.User.Email,
		UserID:     decoded.User.ID,
		Metadata:   metadata,
	}, nil
}

func (a *Adapter) loginStatusURL(session accountLoginSession) string {
	u, err := url.Parse(joinURL(a.authBaseURL, "/api/auth/cli/status"))
	if err != nil {
		return joinURL(a.authBaseURL, "/api/auth/cli/status")
	}
	q := u.Query()
	q.Set("fingerprintId", session.FingerprintID)
	q.Set("fingerprintHash", session.FingerprintHash)
	q.Set("expiresAt", fmt.Sprintf("%d", session.ExpiresAtRaw))
	u.RawQuery = q.Encode()
	return u.String()
}

func (a *Adapter) saveLoginSession(session accountLoginSession) {
	a.mu.Lock()
	a.loginSessions[session.FingerprintID] = session
	a.mu.Unlock()
}

func (a *Adapter) loginSession(sessionID string) (accountLoginSession, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	session, ok := a.loginSessions[sessionID]
	return session, ok
}

func (a *Adapter) deleteLoginSession(sessionID string) {
	a.mu.Lock()
	delete(a.loginSessions, sessionID)
	a.mu.Unlock()
}

func newFingerprintID() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("freebuff: generate fingerprint: %w", err)
	}
	return "enhanced-" + hex.EncodeToString(b[:]), nil
}

func freebuffLoginHeaders(withContentType bool) http.Header {
	h := http.Header{}
	h.Set("User-Agent", freebuffLoginUserAgent)
	if withContentType {
		h.Set("Content-Type", "application/json")
	}
	return h
}

func freebuffLoginTransportProfile(withContentType bool) channels.TransportProfile {
	order := []string{"user-agent"}
	if withContentType {
		order = append(order, "content-type")
	}
	return channels.TransportProfile{
		TLSClientProfile:        "chrome_146",
		RandomTLSExtensionOrder: true,
		DisableHTTP3:            true,
		HeaderOrder:             order,
		PseudoHeaderOrder:       []string{":method", ":authority", ":scheme", ":path"},
	}
}

func freebuffExpiresAtUnix(raw int64) int64 {
	if raw > 1_000_000_000_000 {
		return raw / 1000
	}
	return raw
}

func freebuffCredentialFields(obj map[string]any) (credential, name, email, userID string, ok bool) {
	user := objectField(obj, "user")
	data := objectField(obj, "data")
	dataUser := objectField(data, "user")

	credential = firstNonEmpty(
		stringField(obj, "authToken", "auth_token", "token", "credential", "access_token", "accessToken", "refresh_token", "refreshToken"),
		stringField(user, "authToken", "auth_token", "token", "credential"),
		stringField(data, "authToken", "auth_token", "token", "credential", "access_token", "accessToken"),
		stringField(dataUser, "authToken", "auth_token", "token", "credential"),
	)
	if credential == "" {
		return "", "", "", "", false
	}

	name = firstNonEmpty(
		stringField(obj, "name", "account_name", "accountName", "user_name", "userName", "username"),
		stringField(user, "name", "account_name", "accountName", "user_name", "userName", "username"),
		stringField(data, "name", "account_name", "accountName", "user_name", "userName", "username"),
		stringField(dataUser, "name", "account_name", "accountName", "user_name", "userName", "username"),
	)
	email = firstNonEmpty(
		stringField(obj, "email", "oauth_email"),
		stringField(user, "email", "oauth_email"),
		stringField(data, "email", "oauth_email"),
		stringField(dataUser, "email", "oauth_email"),
	)
	userID = firstNonEmpty(
		stringField(obj, "id", "user_id", "userId", "account_id", "accountId"),
		stringField(user, "id", "user_id", "userId", "account_id", "accountId"),
		stringField(data, "id", "user_id", "userId", "account_id", "accountId"),
		stringField(dataUser, "id", "user_id", "userId", "account_id", "accountId"),
	)
	if name == "" {
		name = firstNonEmpty(email, userID)
	}
	return strings.TrimSpace(credential), name, email, userID, true
}

func objectField(obj map[string]any, key string) map[string]any {
	if obj == nil {
		return nil
	}
	value, ok := obj[key]
	if !ok {
		return nil
	}
	nested, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return nested
}

func stringField(obj map[string]any, keys ...string) string {
	if obj == nil {
		return ""
	}
	for _, key := range keys {
		value, ok := obj[key]
		if !ok {
			continue
		}
		if s, ok := value.(string); ok {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

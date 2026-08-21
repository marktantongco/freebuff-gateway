package channels

import (
	"context"
	"errors"
)

const (
	AccountAuthKindCredential   = "credential"
	AccountAuthKindExternalLink = "external_link"

	AccountLoginCompletionPoll           = "poll"
	AccountLoginCompletionManualCallback = "manual_callback"

	AccountCredentialInputModeImport         = "import"
	AccountCredentialInputModeGitHubProtocol = "github_protocol"
)

var ErrAccountLoginPending = errors.New("channels: account login pending")

type AccountAuthMethod struct {
	ID                    string `json:"id"`
	Label                 string `json:"label"`
	Description           string `json:"description,omitempty"`
	Kind                  string `json:"kind"`
	CompletionMode        string `json:"completion_mode,omitempty"`
	RequiresCredential    bool   `json:"requires_credential"`
	CallbackPlaceholder   string `json:"callback_placeholder,omitempty"`
	CredentialInputMode   string `json:"credential_input_mode,omitempty"`
	CredentialPlaceholder string `json:"credential_placeholder,omitempty"`
	CredentialImportHint  string `json:"credential_import_hint,omitempty"`
	CredentialDropTitle   string `json:"credential_drop_title,omitempty"`
	CredentialDropHint    string `json:"credential_drop_hint,omitempty"`
}

type AccountLoginStartResult struct {
	SessionID        string `json:"session_id"`
	LoginURL         string `json:"login_url"`
	RedirectURI      string `json:"redirect_uri,omitempty"`
	ExpiresAt        int64  `json:"expires_at"`
	PollAfterSeconds int    `json:"poll_after_seconds"`
	CompletionMode   string `json:"completion_mode,omitempty"`
	CallbackRequired bool   `json:"callback_required,omitempty"`
}

type AccountLoginCompleteRequest struct {
	CallbackURL string `json:"callback_url"`
}

type AccountLoginResult struct {
	Completed  bool           `json:"completed"`
	Credential string         `json:"-"`
	UserName   string         `json:"user_name,omitempty"`
	UserEmail  string         `json:"user_email,omitempty"`
	UserID     string         `json:"user_id,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

type AccountCredentialImport struct {
	Name       string
	Credential string
	Metadata   map[string]any
}

type AccountCredentialImporter interface {
	ImportAccountCredential(raw string) (*AccountCredentialImport, error)
}

type AccountOnboarder interface {
	AccountAuthMethods() []AccountAuthMethod
	StartAccountLogin(ctx context.Context, methodID string, tp Transport) (*AccountLoginStartResult, error)
	PollAccountLogin(ctx context.Context, sessionID string, tp Transport) (*AccountLoginResult, error)
	CompleteAccountLogin(ctx context.Context, sessionID string, req AccountLoginCompleteRequest, tp Transport) (*AccountLoginResult, error)
}

func DefaultAccountAuthMethods() []AccountAuthMethod {
	return []AccountAuthMethod{
		{
			ID:                 AccountAuthKindCredential,
			Label:              "Credential",
			Kind:               AccountAuthKindCredential,
			RequiresCredential: true,
		},
	}
}

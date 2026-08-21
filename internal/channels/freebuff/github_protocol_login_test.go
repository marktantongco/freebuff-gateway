package freebuff

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

func TestGitHubProtocolTOTPAtUsesRFCVector(t *testing.T) {
	code, err := githubProtocolTOTPAt("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ", time.Unix(59, 0))
	if err != nil {
		t.Fatalf("totp: %v", err)
	}
	if code != "287082" {
		t.Fatalf("code = %s, want 287082", code)
	}
}

func TestGitHubProtocolExtractsLoginAndTOTPForms(t *testing.T) {
	loginForm, ok := githubProtocolFindLoginForm([]byte(`
		<form action="/session" method="post">
			<input type="hidden" name="authenticity_token" value="csrf-login">
			<input type="hidden" name="timestamp" value="123">
			<input type="text" name="required_field_abcd" hidden="hidden">
			<input name="login">
			<input name="password">
		</form>`))
	if !ok {
		t.Fatal("login form not found")
	}
	if loginForm.Fields.Get("authenticity_token") != "csrf-login" || loginForm.Fields.Get("timestamp") != "123" {
		t.Fatalf("login fields = %+v", loginForm.Fields)
	}
	if _, ok := loginForm.Fields["required_field_abcd"]; !ok {
		t.Fatalf("dynamic required field missing: %+v", loginForm.Fields)
	}

	totpForm, ok := githubProtocolFindTOTPForm([]byte(`
		<form action="/sessions/two-factor" method="post">
			<input type="hidden" name="authenticity_token" value="csrf-totp">
			<input name="app_otp">
		</form>`))
	if !ok {
		t.Fatal("totp form not found")
	}
	if totpForm.Fields.Get("authenticity_token") != "csrf-totp" {
		t.Fatalf("totp fields = %+v", totpForm.Fields)
	}
}

func TestGitHubProtocolOAuthCallbackFromMetaRefresh(t *testing.T) {
	body := []byte(`<html><head><meta http-equiv="refresh" content="0;url=https://freebuff.test/api/auth/callback/github?code=abc&amp;state=xyz"></head></html>`)
	got := githubProtocolOAuthCallbackURL(body)
	if got != "https://freebuff.test/api/auth/callback/github?code=abc&state=xyz" {
		t.Fatalf("callback url = %q", got)
	}
}

func TestRunGitHubProtocolLoginInputHappyPathPollsCLIStatusToken(t *testing.T) {
	a := New(
		WithAuthBaseURL("https://freebuff.test"),
		WithGitHubBaseURL("https://github.test"),
	)
	tp := &sequenceTransport{t: t}
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		switch idx {
		case 0:
			if req.Method != http.MethodPost || mustPath(t, req.URL) != "/api/auth/cli/code" {
				t.Fatalf("request %d = %s %s", idx, req.Method, req.URL)
			}
			return jsonResponse(200, map[string]any{
				"fingerprintId":   "enhanced-protocol-test",
				"fingerprintHash": "hash-1",
				"loginUrl":        "https://freebuff.test/login?auth_code=auth-1",
				"expiresAt":       time.Now().Add(time.Minute).UnixMilli(),
			}), nil
		case 1:
			if req.Method != http.MethodGet || mustPath(t, req.URL) != "/login" {
				t.Fatalf("request %d = %s %s", idx, req.Method, req.URL)
			}
			return githubProtocolHTML(200, `<button>Continue with GitHub</button>`, nil), nil
		case 2:
			return jsonResponse(200, map[string]any{"github": map[string]any{"id": "github"}}), nil
		case 3:
			return jsonResponse(200, map[string]any{"csrfToken": "csrf-freebuff"}), nil
		case 4:
			values, _ := url.ParseQuery(string(req.Body))
			if values.Get("callbackUrl") != "/onboard?auth_code=auth-1" {
				t.Fatalf("callbackUrl = %q", values.Get("callbackUrl"))
			}
			return jsonResponse(200, map[string]any{"url": "https://github.test/login/oauth/authorize?client_id=client-1&state=state-1"}), nil
		case 5:
			return githubProtocolRedirect("https://github.test/login?return_to=%2Flogin%2Foauth%2Fauthorize"), nil
		case 6:
			return githubProtocolHTML(200, `<form action="/session" method="post">
				<input type="hidden" name="authenticity_token" value="csrf-login">
				<input type="hidden" name="return_to" value="/login/oauth/authorize">
				<input type="hidden" name="timestamp" value="123">
				<input type="hidden" name="timestamp_secret" value="secret">
				<input name="login">
				<input name="password">
				<input name="webauthn-support" value="unknown">
				<input name="webauthn-iuvpaa-support" value="unknown">
				<input name="javascript-support" value="unknown">
			</form>`, nil), nil
		case 7:
			if req.Method != http.MethodGet || mustPath(t, req.URL) != "/u2f/login_fragment" {
				t.Fatalf("request %d = %s %s", idx, req.Method, req.URL)
			}
			return githubProtocolHTML(200, `fragment`, nil), nil
		case 8:
			values, _ := url.ParseQuery(string(req.Body))
			if values.Get("login") != "ada" || values.Get("password") != "pw" {
				t.Fatalf("password form = %s", string(req.Body))
			}
			if values.Get("webauthn-support") != "supported" || values.Get("javascript-support") != "true" {
				t.Fatalf("capability fields = %s", string(req.Body))
			}
			return githubProtocolRedirect("https://github.test/sessions/two-factor/app"), nil
		case 9:
			return githubProtocolHTML(200, `<form action="/sessions/two-factor" method="post">
				<input type="hidden" name="authenticity_token" value="csrf-totp">
				<input name="app_otp">
			</form>`, nil), nil
		case 10:
			values, _ := url.ParseQuery(string(req.Body))
			if values.Get("app_otp") != "287082" {
				t.Fatalf("app_otp = %q, want 287082", values.Get("app_otp"))
			}
			return githubProtocolRedirect("https://github.test/login/oauth/authorize?client_id=client-1&state=state-1"), nil
		case 11:
			return githubProtocolHTML(200, `<meta http-equiv="refresh" content="0;url=https://freebuff.test/api/auth/callback/github?code=code-1&amp;state=state-1">`, nil), nil
		case 12:
			return githubProtocolRedirect("https://freebuff.test/onboard?auth_code=auth-1"), nil
		case 13:
			return githubProtocolHTML(200, `<html>onboard</html>`, nil), nil
		case 14:
			if req.Method != http.MethodGet || mustPath(t, req.URL) != "/api/auth/cli/status" {
				t.Fatalf("request %d = %s %s", idx, req.Method, req.URL)
			}
			return jsonResponse(200, map[string]any{
				"user": map[string]any{
					"id":        "user-1",
					"name":      "Ada",
					"email":     "ada@example.test",
					"authToken": "freebuff-token",
				},
			}), nil
		default:
			t.Fatalf("unexpected request %d: %s %s", idx, req.Method, req.URL)
		}
		return nil, nil
	}

	result, err := a.RunGitHubProtocolLoginInput(context.Background(), GitHubProtocolLoginInput{
		Username:   "ada",
		Password:   "pw",
		TOTPSecret: "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
		Now:        func() time.Time { return time.Unix(59, 0) },
		TransportProfile: channels.TransportProfile{
			ProxyURL: "http://proxy.test:8080",
		},
	}, tp)
	if err != nil {
		t.Fatalf("protocol login: %v", err)
	}
	if result.Status != GitHubProtocolLoginStatusSuccess || result.Credential != "freebuff-token" {
		t.Fatalf("result = %+v", result)
	}
	if result.Metadata["auth_method"] != freebuffGitHubProtocolMethodID || result.Metadata["github_login"] != "ada" || result.Metadata["freebuff_fingerprint_id"] != "enhanced-protocol-test" {
		t.Fatalf("metadata = %+v", result.Metadata)
	}
	if len(tp.requests) != 15 {
		t.Fatalf("request count = %d, want 15", len(tp.requests))
	}
	for _, req := range tp.requests {
		if strings.Contains(req.URL, "pw") || strings.Contains(req.URL, "GEZDG") {
			t.Fatalf("request url leaked secret: %s", req.URL)
		}
		if req.URL != "https://freebuff.test/api/auth/cli/code" && req.Headers.Get("User-Agent") == "" {
			t.Fatalf("missing user agent for %s", req.URL)
		}
		if req.TransportProfile.ProxyURL != "http://proxy.test:8080" {
			t.Fatalf("proxy profile for %s = %q", req.URL, req.TransportProfile.ProxyURL)
		}
	}
}

func TestRunGitHubProtocolLoginClassifiesCaptcha(t *testing.T) {
	resp := &githubProtocolResponse{
		Status: http.StatusOK,
		URL:    "https://github.test/session",
		Body:   []byte(`<html>Captcha verification required</html>`),
	}
	result := githubProtocolClassifyChallenge(resp, "password")
	if result == nil || result.Status != GitHubProtocolLoginStatusCaptchaRequired {
		t.Fatalf("classification = %+v", result)
	}
}

func githubProtocolRedirect(location string) *channels.OutboundResponse {
	return githubProtocolHTML(http.StatusFound, "", http.Header{"Location": []string{location}})
}

func githubProtocolHTML(status int, body string, headers http.Header) *channels.OutboundResponse {
	if headers == nil {
		headers = http.Header{}
	}
	raw := []byte(body)
	return &channels.OutboundResponse{
		Status:      status,
		Headers:     headers,
		Body:        raw,
		BodyPreview: raw,
	}
}

package freebuff

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	stdhtml "html"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"

	xhtml "golang.org/x/net/html"
)

const (
	freebuffGitHubProtocolMethodID = "github_protocol"

	defaultGitHubProtocolGitHubBaseURL = "https://github.com"
	githubProtocolRequestTimeout       = 30 * time.Second
	githubProtocolMaxRedirects         = 12
	githubProtocolLoginPollInterval    = 2 * time.Second
)

const (
	GitHubProtocolLoginStatusSuccess                    = "success"
	GitHubProtocolLoginStatusInvalidCredentials         = "invalid_credentials"
	GitHubProtocolLoginStatusInvalidTOTP                = "invalid_totp"
	GitHubProtocolLoginStatusAwaitingDeviceVerification = "awaiting_device_verification"
	GitHubProtocolLoginStatusCaptchaRequired            = "captcha_required"
	GitHubProtocolLoginStatusPasskeyRequired            = "passkey_required"
	GitHubProtocolLoginStatusTrustedDeviceRequired      = "trusted_device_required"
	GitHubProtocolLoginStatusRateLimited                = "rate_limited"
	GitHubProtocolLoginStatusFingerprintBlocked         = "fingerprint_or_security_blocked"
	GitHubProtocolLoginStatusProxyOrNetworkError        = "proxy_or_network_error"
	GitHubProtocolLoginStatusUnexpectedRedirect         = "unexpected_redirect"
	GitHubProtocolLoginStatusParseError                 = "parse_error"
	GitHubProtocolLoginStatusUnknownChallenge           = "unknown_challenge"
)

type GitHubProtocolLoginInput struct {
	Username         string
	Password         string
	TOTPSecret       string
	ProxyURL         string
	GitHubBaseURL    string
	TransportProfile channels.TransportProfile
	Now              func() time.Time
}

type GitHubProtocolLoginResult struct {
	Status     string
	Reason     string
	Credential string
	UserName   string
	UserEmail  string
	UserID     string
	Metadata   map[string]any
}

type GitHubProtocolLoginError struct {
	Status string
	Reason string
}

func (e GitHubProtocolLoginError) Error() string {
	status := strings.TrimSpace(e.Status)
	if status == "" {
		status = "failed"
	}
	reason := githubProtocolSanitizeReason(e.Reason)
	return "freebuff github protocol login " + status + ": " + reason
}

func (e GitHubProtocolLoginError) ProtocolLoginStatus() string {
	return strings.TrimSpace(e.Status)
}

type githubProtocolFingerprint struct {
	ID                    string
	ChromeMajor           int
	UserAgent             string
	SecCHUA               string
	SecCHUAMobile         string
	SecCHUAPlatform       string
	AcceptLanguage        string
	WebAuthnSupport       string
	WebAuthnIUVPAASupport string
	JavaScriptSupport     string
	TransportProfile      channels.TransportProfile
}

type githubProtocolHTTPClient struct {
	tp  channels.Transport
	jar *cookiejar.Jar
	fp  githubProtocolFingerprint
}

type githubProtocolResponse struct {
	Status  int
	URL     string
	Headers http.Header
	Body    []byte
}

type githubProtocolForm struct {
	Action string
	Method string
	Fields url.Values
}

func (a *Adapter) RunGitHubProtocolLogin(ctx context.Context, rawTriple string, tp channels.Transport, profile channels.TransportProfile) (*channels.AccountCredentialImport, error) {
	input, err := githubProtocolCredentialInput(rawTriple, profile)
	if err != nil {
		return nil, err
	}
	result, err := a.RunGitHubProtocolLoginInput(ctx, input, tp)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, GitHubProtocolLoginError{
			Status: GitHubProtocolLoginStatusUnknownChallenge,
			Reason: "github protocol login returned no result",
		}
	}
	status := strings.TrimSpace(result.Status)
	credential := strings.TrimSpace(result.Credential)
	if status == "" && credential != "" {
		status = GitHubProtocolLoginStatusSuccess
	}
	if status != GitHubProtocolLoginStatusSuccess || credential == "" {
		reason := result.Reason
		if reason == "" && credential == "" {
			reason = "freebuff session token missing"
		}
		return nil, GitHubProtocolLoginError{Status: status, Reason: reason}
	}
	metadata := map[string]any{}
	for key, value := range result.Metadata {
		metadata[key] = value
	}
	metadata["auth_method"] = freebuffGitHubProtocolMethodID
	if input.Username != "" {
		metadata["github_login"] = input.Username
	}
	if result.UserID != "" {
		metadata["github_user_id"] = result.UserID
	}
	if result.UserName != "" {
		metadata["github_user_name"] = result.UserName
	}
	if result.UserEmail != "" {
		metadata["github_user_email"] = result.UserEmail
	}
	name := firstNonEmpty(result.UserName, result.UserEmail, result.UserID, input.Username, "freebuff-github-protocol")
	return &channels.AccountCredentialImport{
		Name:       name,
		Credential: credential,
		Metadata:   metadata,
	}, nil
}

func githubProtocolCredentialInput(rawTriple string, profile channels.TransportProfile) (GitHubProtocolLoginInput, error) {
	parts := strings.Split(strings.TrimSpace(rawTriple), "----")
	if len(parts) != 3 {
		return GitHubProtocolLoginInput{}, GitHubProtocolLoginError{
			Status: GitHubProtocolLoginStatusParseError,
			Reason: "github credential triple must include username, password, and totp secret",
		}
	}
	input := GitHubProtocolLoginInput{
		Username:         strings.TrimSpace(parts[0]),
		Password:         strings.TrimSpace(parts[1]),
		TOTPSecret:       strings.TrimSpace(parts[2]),
		ProxyURL:         strings.TrimSpace(profile.ProxyURL),
		TransportProfile: profile,
	}
	if input.Username == "" || input.Password == "" || input.TOTPSecret == "" {
		return GitHubProtocolLoginInput{}, GitHubProtocolLoginError{
			Status: GitHubProtocolLoginStatusParseError,
			Reason: "github credential triple must include username, password, and totp secret",
		}
	}
	return input, nil
}

func (a *Adapter) RunGitHubProtocolLoginInput(ctx context.Context, input GitHubProtocolLoginInput, tp channels.Transport) (*GitHubProtocolLoginResult, error) {
	input.Username = strings.TrimSpace(input.Username)
	input.Password = strings.TrimSpace(input.Password)
	input.TOTPSecret = strings.TrimSpace(input.TOTPSecret)
	if input.Username == "" || input.Password == "" || input.TOTPSecret == "" {
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "github credential triple must include username, password, and totp secret"), nil
	}
	if tp == nil {
		return nil, fmt.Errorf("freebuff: github protocol login: nil transport")
	}
	if scoped, ok := tp.(channels.RequestScopedTransport); ok {
		var closeScope func()
		ctx, closeScope = scoped.WithRequestScope(ctx)
		defer closeScope()
	}

	fp, err := newGitHubProtocolFingerprint(input)
	if err != nil {
		return nil, err
	}
	jar, _ := cookiejar.New(nil)
	client := githubProtocolHTTPClient{tp: tp, jar: jar, fp: fp}

	authBaseURL := normalizeBaseURL(a.authBaseURL)
	if authBaseURL == "" {
		authBaseURL = defaultAuthBaseURL
	}
	githubBaseURL := normalizeBaseURL(input.GitHubBaseURL)
	if githubBaseURL == "" {
		githubBaseURL = normalizeBaseURL(a.githubURL)
	}
	if githubBaseURL == "" {
		githubBaseURL = defaultGitHubProtocolGitHubBaseURL
	}

	start, err := a.startGitHubLoginWithProfile(ctx, tp, input.TransportProfile)
	if err != nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "freebuff cli login code request failed"), nil
	}
	session, ok := a.loginSession(start.SessionID)
	if !ok {
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff cli login session missing"), nil
	}
	defer a.deleteLoginSession(session.FingerprintID)

	if _, err := client.get(ctx, start.LoginURL, "", client.navigationHeaders("")); err != nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "freebuff login page request failed"), nil
	}
	if result := client.freeBuffProviders(ctx, authBaseURL); result != nil {
		return result, nil
	}
	csrfToken, result := client.freeBuffCSRF(ctx, authBaseURL)
	if result != nil {
		return result, nil
	}
	callbackURL := githubProtocolCallbackURLFromLoginURL(start.LoginURL)
	signInURL, result := client.freeBuffGitHubSignIn(ctx, authBaseURL, csrfToken, callbackURL, start.LoginURL)
	if result != nil {
		return result, nil
	}

	loginResp, result := client.followGET(ctx, signInURL, "", githubProtocolMaxRedirects)
	if result != nil {
		return result, nil
	}
	loginForm, ok := githubProtocolFindLoginForm(loginResp.Body)
	if !ok {
		if classified := githubProtocolClassifyChallenge(loginResp, "login"); classified != nil {
			return classified, nil
		}
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "github login form not found at "+githubProtocolURLPath(loginResp.URL)), nil
	}
	_ = client.githubU2FFragment(ctx, githubBaseURL, loginResp.URL)

	passwordResp, result := client.submitGitHubPassword(ctx, loginResp.URL, loginForm, input)
	if result != nil {
		return result, nil
	}
	passwordResp, result = client.followRedirectsFrom(ctx, passwordResp, githubProtocolMaxRedirects)
	if result != nil {
		return result, nil
	}

	if totpForm, ok := githubProtocolFindTOTPForm(passwordResp.Body); ok || strings.Contains(githubProtocolURLPath(passwordResp.URL), "/sessions/two-factor") {
		if !ok {
			return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "github two-factor form not found at "+githubProtocolURLPath(passwordResp.URL)), nil
		}
		code, err := githubProtocolTOTPAt(input.TOTPSecret, githubProtocolNow(input))
		if err != nil {
			return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "invalid totp secret"), nil
		}
		totpResp, result := client.submitGitHubTOTP(ctx, passwordResp.URL, totpForm, code)
		if result != nil {
			return result, nil
		}
		totpResp, result = client.followRedirectsFrom(ctx, totpResp, githubProtocolMaxRedirects)
		if result != nil {
			return result, nil
		}
		totpResp, result = client.followOAuthCallback(ctx, totpResp)
		if result != nil {
			return result, nil
		}
		if classified := githubProtocolClassifyAfterTOTP(totpResp); classified != nil {
			return classified, nil
		}
		return a.pollGitHubProtocolLoginResult(ctx, session, tp, input.TransportProfile, input.Username)
	}

	passwordResp, result = client.followOAuthCallback(ctx, passwordResp)
	if result != nil {
		return result, nil
	}
	if classified := githubProtocolClassifyAfterPassword(passwordResp); classified != nil {
		return classified, nil
	}
	return a.pollGitHubProtocolLoginResult(ctx, session, tp, input.TransportProfile, input.Username)
}

func newGitHubProtocolFingerprint(input GitHubProtocolLoginInput) (githubProtocolFingerprint, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return githubProtocolFingerprint{}, fmt.Errorf("freebuff: github protocol fingerprint: %w", err)
	}
	id := "github-protocol-" + hex.EncodeToString(b[:])
	major := 146
	profile := channels.TransportProfile{
		TLSClientProfile:        "chrome_146",
		ReuseKey:                id,
		RandomTLSExtensionOrder: true,
		DisableHTTP3:            true,
		HeaderOrder:             githubProtocolHeaderOrder(),
		PseudoHeaderOrder:       []string{":method", ":authority", ":scheme", ":path"},
	}
	profile = mergeGitHubProtocolTransportProfile(profile, input.TransportProfile)
	if proxyURL := strings.TrimSpace(input.ProxyURL); proxyURL != "" {
		profile.ProxyURL = proxyURL
	}
	return githubProtocolFingerprint{
		ID:                    id,
		ChromeMajor:           major,
		UserAgent:             fmt.Sprintf("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%d.0.0.0 Safari/537.36", major),
		SecCHUA:               fmt.Sprintf(`"Chromium";v="%d", "Google Chrome";v="%d", "Not_A Brand";v="99"`, major, major),
		SecCHUAMobile:         "?0",
		SecCHUAPlatform:       `"Windows"`,
		AcceptLanguage:        "en-US,en;q=0.9",
		WebAuthnSupport:       "supported",
		WebAuthnIUVPAASupport: "unsupported",
		JavaScriptSupport:     "true",
		TransportProfile:      profile,
	}, nil
}

func mergeGitHubProtocolTransportProfile(base, override channels.TransportProfile) channels.TransportProfile {
	if override.TLSClientProfile != "" {
		base.TLSClientProfile = override.TLSClientProfile
	}
	if override.ReuseKey != "" {
		base.ReuseKey = override.ReuseKey
	}
	if override.ProxyURL != "" {
		base.ProxyURL = override.ProxyURL
	}
	if len(override.HeaderOrder) > 0 {
		base.HeaderOrder = append([]string(nil), override.HeaderOrder...)
	}
	if len(override.PseudoHeaderOrder) > 0 {
		base.PseudoHeaderOrder = append([]string(nil), override.PseudoHeaderOrder...)
	}
	base.ForceHTTP1 = base.ForceHTTP1 || override.ForceHTTP1
	base.DisableHTTP3 = base.DisableHTTP3 || override.DisableHTTP3
	base.ProtocolRacing = base.ProtocolRacing || override.ProtocolRacing
	base.RandomTLSExtensionOrder = base.RandomTLSExtensionOrder || override.RandomTLSExtensionOrder
	base.InsecureSkipVerify = base.InsecureSkipVerify || override.InsecureSkipVerify
	base.DisableIPv4 = base.DisableIPv4 || override.DisableIPv4
	base.DisableIPv6 = base.DisableIPv6 || override.DisableIPv6
	return base
}

func (a *Adapter) pollGitHubProtocolLoginResult(ctx context.Context, session accountLoginSession, tp channels.Transport, profile channels.TransportProfile, githubLogin string) (*GitHubProtocolLoginResult, error) {
	deadline := time.Now().Add(githubProtocolRequestTimeout)
	if session.ExpiresAtUnix > 0 {
		expires := time.Unix(session.ExpiresAtUnix, 0).Add(-5 * time.Second)
		if expires.Before(deadline) {
			deadline = expires
		}
	}
	for {
		result, err := a.pollGitHubLoginWithProfile(ctx, session.FingerprintID, tp, profile)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Completed && strings.TrimSpace(result.Credential) != "" {
			metadata := map[string]any{
				"auth_method":                         freebuffGitHubProtocolMethodID,
				"github_login":                        strings.TrimSpace(githubLogin),
				"freebuff_fingerprint_id":             session.FingerprintID,
				"freebuff_protocol_login_imported_at": time.Now().Unix(),
			}
			for key, value := range result.Metadata {
				metadata[key] = value
			}
			metadata["auth_method"] = freebuffGitHubProtocolMethodID
			if strings.TrimSpace(githubLogin) != "" {
				metadata["github_login"] = strings.TrimSpace(githubLogin)
			}
			return &GitHubProtocolLoginResult{
				Status:     GitHubProtocolLoginStatusSuccess,
				Credential: strings.TrimSpace(result.Credential),
				UserName:   result.UserName,
				UserEmail:  result.UserEmail,
				UserID:     result.UserID,
				Metadata:   metadata,
			}, nil
		}
		if !time.Now().Before(deadline) {
			return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff cli status did not return auth token"), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(githubProtocolLoginPollInterval):
		}
	}
}

func githubProtocolHeaderOrder() []string {
	return []string{
		"host",
		"connection",
		"cache-control",
		"sec-ch-ua",
		"sec-ch-ua-mobile",
		"sec-ch-ua-platform",
		"upgrade-insecure-requests",
		"user-agent",
		"accept",
		"sec-fetch-site",
		"sec-fetch-mode",
		"sec-fetch-user",
		"sec-fetch-dest",
		"referer",
		"accept-encoding",
		"accept-language",
		"origin",
		"content-type",
		"cookie",
	}
}

func (c githubProtocolHTTPClient) freeBuffProviders(ctx context.Context, authBaseURL string) *GitHubProtocolLoginResult {
	resp, err := c.get(ctx, joinURL(authBaseURL, "/api/auth/providers"), "", c.jsonHeaders("", "same-origin"))
	if err != nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "freebuff providers request failed")
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff providers status "+strconv.Itoa(resp.Status))
	}
	return nil
}

func (c githubProtocolHTTPClient) freeBuffCSRF(ctx context.Context, authBaseURL string) (string, *GitHubProtocolLoginResult) {
	resp, err := c.get(ctx, joinURL(authBaseURL, "/api/auth/csrf"), "", c.jsonHeaders("", "same-origin"))
	if err != nil {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "freebuff csrf request failed")
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff csrf status "+strconv.Itoa(resp.Status))
	}
	var decoded struct {
		CSRFToken string `json:"csrfToken"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff csrf response is not json")
	}
	if strings.TrimSpace(decoded.CSRFToken) == "" {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff csrf token missing")
	}
	return strings.TrimSpace(decoded.CSRFToken), nil
}

func (c githubProtocolHTTPClient) freeBuffGitHubSignIn(ctx context.Context, authBaseURL, csrfToken, callbackURL, loginURL string) (string, *GitHubProtocolLoginResult) {
	form := url.Values{}
	form.Set("callbackUrl", callbackURL)
	form.Set("csrfToken", csrfToken)
	form.Set("json", "true")
	signInURL := joinURL(authBaseURL, "/api/auth/signin/github")
	resp, err := c.postForm(ctx, signInURL, form, authBaseURL, loginURL)
	if err != nil {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "freebuff github signin request failed")
	}
	if loc := githubProtocolRedirectLocation(resp); loc != "" {
		return loc, nil
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff github signin status "+strconv.Itoa(resp.Status))
	}
	var decoded struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff github signin response is not json")
	}
	if strings.TrimSpace(decoded.URL) == "" {
		return "", githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff github signin url missing")
	}
	return strings.TrimSpace(decoded.URL), nil
}

func (c githubProtocolHTTPClient) githubU2FFragment(ctx context.Context, githubBaseURL, referer string) error {
	_, err := c.get(ctx, joinURL(githubBaseURL, "/u2f/login_fragment"), referer, c.ajaxHeaders(referer))
	return err
}

func (c githubProtocolHTTPClient) submitGitHubPassword(ctx context.Context, pageURL string, form githubProtocolForm, input GitHubProtocolLoginInput) (*githubProtocolResponse, *GitHubProtocolLoginResult) {
	values := githubProtocolCloneValues(form.Fields)
	values.Set("login", input.Username)
	values.Set("password", input.Password)
	githubProtocolSetCapability(values, "webauthn-support", c.fp.WebAuthnSupport)
	githubProtocolSetCapability(values, "webauthn-iuvpaa-support", c.fp.WebAuthnIUVPAASupport)
	githubProtocolSetCapability(values, "javascript-support", c.fp.JavaScriptSupport)
	if values.Get("commit") == "" {
		values.Set("commit", "Sign in")
	}
	actionURL := githubProtocolFormAction(pageURL, form)
	origin := githubProtocolOrigin(actionURL)
	resp, err := c.postForm(ctx, actionURL, values, origin, pageURL)
	if err != nil {
		return nil, githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "github password submit failed")
	}
	return resp, nil
}

func (c githubProtocolHTTPClient) submitGitHubTOTP(ctx context.Context, pageURL string, form githubProtocolForm, code string) (*githubProtocolResponse, *GitHubProtocolLoginResult) {
	values := githubProtocolCloneValues(form.Fields)
	values.Set("app_otp", code)
	if _, ok := values["otp"]; ok {
		values.Set("otp", code)
	}
	actionURL := githubProtocolFormAction(pageURL, form)
	origin := githubProtocolOrigin(actionURL)
	resp, err := c.postForm(ctx, actionURL, values, origin, pageURL)
	if err != nil {
		return nil, githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "github totp submit failed")
	}
	return resp, nil
}

func (c githubProtocolHTTPClient) freeBuffSession(ctx context.Context, authBaseURL, githubLogin string) (*GitHubProtocolLoginResult, error) {
	resp, err := c.get(ctx, joinURL(authBaseURL, "/api/auth/session"), joinURL(authBaseURL, "/onboard"), c.jsonHeaders(joinURL(authBaseURL, "/onboard"), "same-origin"))
	if err != nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "freebuff session request failed"), nil
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff session status "+strconv.Itoa(resp.Status)), nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff session response is not json"), nil
	}
	credential, name, email, userID, ok := freebuffCredentialFields(decoded)
	if !ok || strings.TrimSpace(credential) == "" {
		return githubProtocolFailure(GitHubProtocolLoginStatusParseError, "freebuff session token missing"), nil
	}
	metadata := map[string]any{
		"auth_method":                         freebuffGitHubProtocolMethodID,
		"github_login":                        strings.TrimSpace(githubLogin),
		"freebuff_protocol_login_imported_at": time.Now().Unix(),
	}
	if userID != "" {
		metadata["github_user_id"] = userID
	}
	if name != "" {
		metadata["github_user_name"] = name
	}
	if email != "" {
		metadata["github_user_email"] = email
	}
	return &GitHubProtocolLoginResult{
		Status:     GitHubProtocolLoginStatusSuccess,
		Credential: strings.TrimSpace(credential),
		UserName:   name,
		UserEmail:  email,
		UserID:     userID,
		Metadata:   metadata,
	}, nil
}

func (c githubProtocolHTTPClient) followGET(ctx context.Context, rawURL, referer string, limit int) (*githubProtocolResponse, *GitHubProtocolLoginResult) {
	current := strings.TrimSpace(rawURL)
	for i := 0; i <= limit; i++ {
		resp, err := c.get(ctx, current, referer, c.navigationHeaders(referer))
		if err != nil {
			return nil, githubProtocolFailure(GitHubProtocolLoginStatusProxyOrNetworkError, "request failed at "+githubProtocolURLPath(current))
		}
		loc := githubProtocolRedirectLocation(resp)
		if loc == "" {
			return resp, nil
		}
		next, err := githubProtocolResolveLocation(resp.URL, loc)
		if err != nil {
			return nil, githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "invalid redirect at "+githubProtocolURLPath(resp.URL))
		}
		referer = resp.URL
		current = next
	}
	return nil, githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "too many redirects at "+githubProtocolURLPath(current))
}

func (c githubProtocolHTTPClient) followRedirectsFrom(ctx context.Context, resp *githubProtocolResponse, limit int) (*githubProtocolResponse, *GitHubProtocolLoginResult) {
	if resp == nil {
		return nil, githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "missing response")
	}
	loc := githubProtocolRedirectLocation(resp)
	if loc == "" {
		return resp, nil
	}
	next, err := githubProtocolResolveLocation(resp.URL, loc)
	if err != nil {
		return nil, githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "invalid redirect at "+githubProtocolURLPath(resp.URL))
	}
	return c.followGET(ctx, next, resp.URL, limit)
}

func (c githubProtocolHTTPClient) followOAuthCallback(ctx context.Context, resp *githubProtocolResponse) (*githubProtocolResponse, *GitHubProtocolLoginResult) {
	if resp == nil {
		return nil, githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "missing oauth response")
	}
	callbackURL := githubProtocolOAuthCallbackURL(resp.Body)
	if callbackURL == "" {
		return resp, nil
	}
	next, err := githubProtocolResolveLocation(resp.URL, callbackURL)
	if err != nil {
		return nil, githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "invalid oauth callback redirect")
	}
	return c.followGET(ctx, next, resp.URL, githubProtocolMaxRedirects)
}

func (c githubProtocolHTTPClient) get(ctx context.Context, rawURL, referer string, headers http.Header) (*githubProtocolResponse, error) {
	return c.do(ctx, http.MethodGet, rawURL, headers, nil)
}

func (c githubProtocolHTTPClient) postForm(ctx context.Context, rawURL string, values url.Values, origin, referer string) (*githubProtocolResponse, error) {
	headers := c.formHeaders(origin, referer)
	return c.do(ctx, http.MethodPost, rawURL, headers, []byte(values.Encode()))
}

func (c githubProtocolHTTPClient) do(ctx context.Context, method, rawURL string, headers http.Header, body []byte) (*githubProtocolResponse, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	reqHeaders := headers.Clone()
	if len(c.jar.Cookies(u)) > 0 {
		reqHeaders.Set("Cookie", githubProtocolCookieHeader(c.jar.Cookies(u)))
	}
	resp, err := c.tp.Do(ctx, &channels.OutboundRequest{
		Method:           method,
		URL:              rawURL,
		Headers:          reqHeaders,
		Body:             body,
		Timeout:          githubProtocolRequestTimeout,
		TransportProfile: c.fp.TransportProfile,
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("freebuff: github protocol nil response")
	}
	httpResp := http.Response{Header: resp.Headers}
	if cookies := httpResp.Cookies(); len(cookies) > 0 {
		c.jar.SetCookies(u, cookies)
	}
	return &githubProtocolResponse{
		Status:  resp.Status,
		URL:     rawURL,
		Headers: resp.Headers,
		Body:    append([]byte(nil), resp.Body...),
	}, nil
}

func (c githubProtocolHTTPClient) baseHeaders(referer string) http.Header {
	h := http.Header{}
	h.Set("User-Agent", c.fp.UserAgent)
	h.Set("Accept-Language", c.fp.AcceptLanguage)
	h.Set("sec-ch-ua", c.fp.SecCHUA)
	h.Set("sec-ch-ua-mobile", c.fp.SecCHUAMobile)
	h.Set("sec-ch-ua-platform", c.fp.SecCHUAPlatform)
	h.Set("Accept-Encoding", "identity")
	if strings.TrimSpace(referer) != "" {
		h.Set("Referer", strings.TrimSpace(referer))
	}
	return h
}

func (c githubProtocolHTTPClient) navigationHeaders(referer string) http.Header {
	h := c.baseHeaders(referer)
	h.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	h.Set("Upgrade-Insecure-Requests", "1")
	h.Set("Sec-Fetch-Mode", "navigate")
	h.Set("Sec-Fetch-Dest", "document")
	h.Set("Sec-Fetch-User", "?1")
	h.Set("Sec-Fetch-Site", githubProtocolFetchSite(referer))
	return h
}

func (c githubProtocolHTTPClient) jsonHeaders(referer, site string) http.Header {
	h := c.baseHeaders(referer)
	h.Set("Accept", "application/json, text/plain, */*")
	h.Set("Sec-Fetch-Mode", "cors")
	h.Set("Sec-Fetch-Dest", "empty")
	h.Set("Sec-Fetch-Site", firstNonEmpty(site, "same-origin"))
	return h
}

func (c githubProtocolHTTPClient) ajaxHeaders(referer string) http.Header {
	h := c.baseHeaders(referer)
	h.Set("Accept", "text/html, */*; q=0.01")
	h.Set("X-Requested-With", "XMLHttpRequest")
	h.Set("Sec-Fetch-Mode", "cors")
	h.Set("Sec-Fetch-Dest", "empty")
	h.Set("Sec-Fetch-Site", githubProtocolFetchSite(referer))
	return h
}

func (c githubProtocolHTTPClient) formHeaders(origin, referer string) http.Header {
	h := c.navigationHeaders(referer)
	h.Set("Content-Type", "application/x-www-form-urlencoded")
	if strings.TrimSpace(origin) != "" {
		h.Set("Origin", strings.TrimSpace(origin))
	}
	return h
}

func githubProtocolFetchSite(referer string) string {
	if strings.TrimSpace(referer) == "" {
		return "none"
	}
	return "same-origin"
}

func githubProtocolCookieHeader(cookies []*http.Cookie) string {
	parts := make([]string, 0, len(cookies))
	for _, cookie := range cookies {
		if cookie == nil || cookie.Name == "" {
			continue
		}
		parts = append(parts, cookie.Name+"="+cookie.Value)
	}
	return strings.Join(parts, "; ")
}

func githubProtocolFindLoginForm(body []byte) (githubProtocolForm, bool) {
	forms, err := githubProtocolExtractForms(body)
	if err != nil {
		return githubProtocolForm{}, false
	}
	for _, form := range forms {
		action := strings.ToLower(form.Action)
		if strings.Contains(action, "/session") && form.Fields.Get("authenticity_token") != "" {
			return form, true
		}
		if _, hasLogin := form.Fields["login"]; hasLogin {
			if _, hasPassword := form.Fields["password"]; hasPassword {
				return form, true
			}
		}
	}
	return githubProtocolForm{}, false
}

func githubProtocolFindTOTPForm(body []byte) (githubProtocolForm, bool) {
	forms, err := githubProtocolExtractForms(body)
	if err != nil {
		return githubProtocolForm{}, false
	}
	for _, form := range forms {
		action := strings.ToLower(form.Action)
		if strings.Contains(action, "/sessions/two-factor") {
			return form, true
		}
		if _, ok := form.Fields["app_otp"]; ok {
			return form, true
		}
	}
	return githubProtocolForm{}, false
}

func githubProtocolExtractForms(body []byte) ([]githubProtocolForm, error) {
	root, err := xhtml.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	var forms []githubProtocolForm
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n == nil {
			return
		}
		if n.Type == xhtml.ElementNode && strings.EqualFold(n.Data, "form") {
			form := githubProtocolForm{
				Action: githubProtocolAttr(n, "action"),
				Method: strings.ToUpper(firstNonEmpty(githubProtocolAttr(n, "method"), http.MethodGet)),
				Fields: url.Values{},
			}
			githubProtocolCollectFormFields(n, form.Fields)
			forms = append(forms, form)
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return forms, nil
}

func githubProtocolCollectFormFields(n *xhtml.Node, values url.Values) {
	if n == nil {
		return
	}
	if n.Type == xhtml.ElementNode {
		switch strings.ToLower(n.Data) {
		case "input", "button":
			name := githubProtocolAttr(n, "name")
			if name != "" {
				if typ := strings.ToLower(githubProtocolAttr(n, "type")); (typ == "checkbox" || typ == "radio") && githubProtocolAttr(n, "checked") == "" {
					break
				}
				values.Add(name, githubProtocolAttr(n, "value"))
			}
		case "textarea":
			name := githubProtocolAttr(n, "name")
			if name != "" {
				values.Add(name, githubProtocolNodeText(n))
			}
		}
	}
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		githubProtocolCollectFormFields(child, values)
	}
}

func githubProtocolAttr(n *xhtml.Node, name string) string {
	for _, attr := range n.Attr {
		if strings.EqualFold(attr.Key, name) {
			return attr.Val
		}
	}
	return ""
}

func githubProtocolNodeText(n *xhtml.Node) string {
	var b strings.Builder
	var walk func(*xhtml.Node)
	walk = func(node *xhtml.Node) {
		if node == nil {
			return
		}
		if node.Type == xhtml.TextNode {
			b.WriteString(node.Data)
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return b.String()
}

func githubProtocolFormAction(pageURL string, form githubProtocolForm) string {
	action := strings.TrimSpace(form.Action)
	if action == "" {
		return pageURL
	}
	u, err := url.Parse(pageURL)
	if err != nil {
		return action
	}
	ref, err := url.Parse(action)
	if err != nil {
		return action
	}
	return u.ResolveReference(ref).String()
}

func githubProtocolCloneValues(in url.Values) url.Values {
	out := url.Values{}
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func githubProtocolSetIfEmpty(values url.Values, key, value string) {
	if strings.TrimSpace(values.Get(key)) == "" {
		values.Set(key, value)
	}
}

func githubProtocolSetCapability(values url.Values, key, value string) {
	current := strings.TrimSpace(strings.ToLower(values.Get(key)))
	if current == "" || current == "unknown" || current == "undefined" {
		values.Set(key, value)
	}
}

func githubProtocolRedirectLocation(resp *githubProtocolResponse) string {
	if resp == nil || resp.Status < 300 || resp.Status >= 400 {
		return ""
	}
	return strings.TrimSpace(resp.Headers.Get("Location"))
}

func githubProtocolResolveLocation(baseURL, location string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	ref, err := url.Parse(strings.TrimSpace(location))
	if err != nil {
		return "", err
	}
	return base.ResolveReference(ref).String(), nil
}

func githubProtocolCallbackURLFromLoginURL(loginURL string) string {
	u, err := url.Parse(loginURL)
	if err != nil {
		return "/onboard"
	}
	authCode := strings.TrimSpace(u.Query().Get("auth_code"))
	if authCode == "" {
		return "/onboard"
	}
	return "/onboard?auth_code=" + url.QueryEscape(authCode)
}

func githubProtocolOAuthCallbackURL(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	root, err := xhtml.Parse(strings.NewReader(string(body)))
	if err != nil {
		return ""
	}
	var found string
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n == nil || found != "" {
			return
		}
		if n.Type == xhtml.ElementNode {
			if strings.EqualFold(n.Data, "meta") && strings.EqualFold(githubProtocolAttr(n, "http-equiv"), "refresh") {
				if target := githubProtocolRefreshURL(githubProtocolAttr(n, "content")); target != "" {
					found = target
					return
				}
			}
			if strings.EqualFold(n.Data, "a") {
				href := githubProtocolAttr(n, "href")
				if strings.Contains(href, "/api/auth/callback/github") {
					found = href
					return
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return stdhtml.UnescapeString(strings.TrimSpace(found))
}

func githubProtocolRefreshURL(content string) string {
	content = strings.TrimSpace(content)
	for _, part := range strings.Split(content, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(strings.ToLower(part), "url=") {
			return strings.Trim(strings.TrimSpace(part[4:]), `"'`)
		}
	}
	return ""
}

func githubProtocolOrigin(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

func githubProtocolURLPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Path == "" {
		return "/"
	}
	return u.Path
}

func githubProtocolClassifyAfterPassword(resp *githubProtocolResponse) *GitHubProtocolLoginResult {
	if resp == nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "missing github password response")
	}
	body := strings.ToLower(string(resp.Body))
	path := githubProtocolURLPath(resp.URL)
	if strings.Contains(path, "/login") || strings.Contains(body, "incorrect username or password") || strings.Contains(body, "incorrect password") {
		return githubProtocolFailure(GitHubProtocolLoginStatusInvalidCredentials, "github rejected username or password")
	}
	return githubProtocolClassifyChallenge(resp, "password")
}

func githubProtocolClassifyAfterTOTP(resp *githubProtocolResponse) *GitHubProtocolLoginResult {
	if resp == nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "missing github totp response")
	}
	body := strings.ToLower(string(resp.Body))
	path := githubProtocolURLPath(resp.URL)
	if strings.Contains(path, "/sessions/two-factor") || strings.Contains(body, "two-factor") && (strings.Contains(body, "invalid") || strings.Contains(body, "incorrect")) {
		return githubProtocolFailure(GitHubProtocolLoginStatusInvalidTOTP, "github rejected two-factor code")
	}
	return githubProtocolClassifyChallenge(resp, "totp")
}

func githubProtocolClassifyChallenge(resp *githubProtocolResponse, stage string) *GitHubProtocolLoginResult {
	if resp == nil {
		return githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "missing github response")
	}
	body := strings.ToLower(string(resp.Body))
	path := githubProtocolURLPath(resp.URL)
	if strings.Contains(body, "captcha") || strings.Contains(path, "captcha") {
		return githubProtocolFailure(GitHubProtocolLoginStatusCaptchaRequired, "github captcha required at "+path)
	}
	if strings.Contains(body, "device verification") || strings.Contains(body, "verify your device") || strings.Contains(body, "verification code") || strings.Contains(path, "verified-device") {
		return githubProtocolFailure(GitHubProtocolLoginStatusAwaitingDeviceVerification, "github device verification required at "+path)
	}
	if strings.Contains(body, "trusted device") {
		return githubProtocolFailure(GitHubProtocolLoginStatusTrustedDeviceRequired, "github trusted-device confirmation required at "+path)
	}
	if githubProtocolPasskeyChallenge(body, path) {
		return githubProtocolFailure(GitHubProtocolLoginStatusPasskeyRequired, "github passkey or security-key challenge required at "+path)
	}
	if resp.Status == http.StatusTooManyRequests || strings.Contains(body, "rate limit") || strings.Contains(body, "too many") {
		return githubProtocolFailure(GitHubProtocolLoginStatusRateLimited, "github rate limited at "+path)
	}
	if strings.Contains(body, "suspicious") || strings.Contains(body, "browser") && strings.Contains(body, "detected") {
		return githubProtocolFailure(GitHubProtocolLoginStatusFingerprintBlocked, "github security check blocked protocol login at "+path)
	}
	if resp.Status >= 400 {
		return githubProtocolFailure(GitHubProtocolLoginStatusUnexpectedRedirect, "github "+stage+" status "+strconv.Itoa(resp.Status)+" at "+path)
	}
	if len(resp.Body) > 0 && !strings.Contains(path, "/onboard") {
		return githubProtocolFailure(GitHubProtocolLoginStatusUnknownChallenge, "github returned unrecognized "+stage+" page at "+path)
	}
	return nil
}

func githubProtocolPasskeyChallenge(body, path string) bool {
	if strings.Contains(path, "passkey") || strings.Contains(path, "webauthn") || strings.Contains(path, "security-key") {
		return true
	}
	for _, marker := range []string{
		"passkey authentication",
		"passkey required",
		"passkey challenge",
		"security key required",
		"security-key challenge",
		"use your security key",
		"verify with a passkey",
		"sign in with a passkey to continue",
	} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	if strings.Contains(body, "passkey") && (strings.Contains(body, "required") || strings.Contains(body, "challenge") || strings.Contains(body, "continue")) {
		return true
	}
	if strings.Contains(body, "security key") && (strings.Contains(body, "required") || strings.Contains(body, "challenge") || strings.Contains(body, "insert")) {
		return true
	}
	return false
}

func githubProtocolFailure(status, reason string) *GitHubProtocolLoginResult {
	return &GitHubProtocolLoginResult{
		Status: strings.TrimSpace(status),
		Reason: githubProtocolSanitizeReason(reason),
	}
}

func githubProtocolSanitizeReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "github protocol login failed"
	}
	if len(reason) > 240 {
		reason = reason[:240]
	}
	return reason
}

func githubProtocolNow(input GitHubProtocolLoginInput) time.Time {
	if input.Now != nil {
		return input.Now()
	}
	return time.Now()
}

func githubProtocolTOTPAt(secret string, now time.Time) (string, error) {
	key, err := githubProtocolDecodeTOTPSecret(secret)
	if err != nil {
		return "", err
	}
	var msg [8]byte
	binary.BigEndian.PutUint64(msg[:], uint64(now.Unix()/30))
	mac := hmac.New(sha1.New, key)
	if _, err := mac.Write(msg[:]); err != nil {
		return "", err
	}
	sum := mac.Sum(nil)
	offset := sum[len(sum)-1] & 0x0f
	code := (int(sum[offset])&0x7f)<<24 |
		(int(sum[offset+1])&0xff)<<16 |
		(int(sum[offset+2])&0xff)<<8 |
		(int(sum[offset+3]) & 0xff)
	return fmt.Sprintf("%06d", code%1_000_000), nil
}

func githubProtocolDecodeTOTPSecret(secret string) ([]byte, error) {
	normalized := strings.ToUpper(strings.NewReplacer(" ", "", "-", "").Replace(strings.TrimSpace(secret)))
	normalized = strings.TrimRight(normalized, "=")
	if normalized == "" {
		return nil, fmt.Errorf("empty totp secret")
	}
	if decoded, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(normalized); err == nil {
		return decoded, nil
	}
	padded := normalized
	if rem := len(padded) % 8; rem != 0 {
		padded += strings.Repeat("=", 8-rem)
	}
	return base32.StdEncoding.DecodeString(padded)
}

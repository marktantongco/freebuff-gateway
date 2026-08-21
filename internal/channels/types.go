package channels

import (
	"context"
	"io"
	"net/http"
	"time"
)

type ResponseClass int

const (
	ClassOk ResponseClass = iota
	ClassRateLimited
	ClassAuthExpired
	ClassRetryable
	ClassFatal
)

func (c ResponseClass) String() string {
	switch c {
	case ClassOk:
		return "ok"
	case ClassRateLimited:
		return "rate_limited"
	case ClassAuthExpired:
		return "auth_expired"
	case ClassRetryable:
		return "retryable"
	case ClassFatal:
		return "fatal"
	}
	return "unknown"
}

type Verdict int

const (
	VerdictHealthy Verdict = iota
	VerdictExpire
	VerdictRefresh
)

type State map[string]any

func (s State) String(key string) string {
	if v, ok := s[key].(string); ok {
		return v
	}
	return ""
}

type Account struct {
	ID         string
	ChannelID  string
	Name       string
	Credential string
	Metadata   map[string]any
}

type InboundRequest struct {
	ChannelID string
	Method    string
	Path      string
	RawQuery  string
	Headers   http.Header
	Body      []byte
}

type OutboundRequest struct {
	Method           string
	URL              string
	Headers          http.Header
	Body             []byte
	Timeout          time.Duration
	TransportProfile TransportProfile
}

type TransportProfile struct {
	TLSClientProfile        string
	ReuseKey                string
	ProxyURL                string
	HeaderOrder             []string
	PseudoHeaderOrder       []string
	ForceHTTP1              bool
	DisableHTTP3            bool
	ProtocolRacing          bool
	RandomTLSExtensionOrder bool
	InsecureSkipVerify      bool
	DisableIPv4             bool
	DisableIPv6             bool
}

type OutboundResponse struct {
	Status          int
	Headers         http.Header
	Body            []byte
	BodyPreview     []byte
	FirstResponseMS int64
}

type OutboundStreamResponse struct {
	Status          int
	Headers         http.Header
	Body            io.ReadCloser
	BodyPreview     []byte
	FirstResponseMS int64
}

type TokenUsage struct {
	InputTokens  int
	OutputTokens int
	Known        bool
}

type Transport interface {
	Do(ctx context.Context, req *OutboundRequest) (*OutboundResponse, error)
}

type RequestScopedTransport interface {
	WithRequestScope(ctx context.Context) (context.Context, func())
}

type StreamTransport interface {
	DoStream(ctx context.Context, req *OutboundRequest) (*OutboundStreamResponse, error)
}

type Lease struct {
	SessionID string
	AccountID string
	ChannelID string
	Key       string
	State     State

	releaseFn func(Verdict)
}

func NewLease(sessionID, accountID, channelID, key string, state State, release func(Verdict)) *Lease {
	return &Lease{
		SessionID: sessionID,
		AccountID: accountID,
		ChannelID: channelID,
		Key:       key,
		State:     state,
		releaseFn: release,
	}
}

func (l *Lease) Release(v Verdict) {
	if l == nil || l.releaseFn == nil {
		return
	}
	fn := l.releaseFn
	l.releaseFn = nil
	fn(v)
}

package channels

import (
	"context"
	"io"
	"net/http"
	"time"
)

type ChannelAdapter interface {
	ID() string
	InboundPathPrefix() string

	SessionPolicy() SessionPolicy
	AuthFlow() AuthFlow

	PrepareOutbound(ctx context.Context, lease *Lease, in *InboundRequest) (*OutboundRequest, error)
	ClassifyResponse(status int, headers http.Header, bodyPreview []byte) ResponseClass
}

type TokenCounter interface {
	TokenUsage(req *OutboundRequest, resp *OutboundResponse) (in, out int, ok bool)
}

type ModelInfo struct {
	ID         string   `json:"id"`
	Aliases    []string `json:"aliases"`
	AgentID    string   `json:"agent_id,omitempty"`
	QuotaGroup string   `json:"quota_group"`
	Enabled    bool     `json:"enabled"`
}

type ModelCatalogProvider interface {
	ModelCatalog() []ModelInfo
}

type AccountStateRefresher interface {
	RefreshAccountState(ctx context.Context, acc Account, tp Transport) (State, error)
}

type SessionRestorer interface {
	RestoreSession(ctx context.Context, acc Account, key string, state State, tp Transport) (State, bool, error)
}

type AccountCandidate struct {
	Account        Account
	Priority       int
	Active         bool
	Eligible       bool
	BlockedReason  string
	SessionCount   int
	MaxSessions    int
	OnCooldown     bool
	QuotaAvailable bool
	LastUsedAtUnix int64
	ProviderFacts  map[string]any
}

type SessionCandidate struct {
	ID             string
	ChannelID      string
	AccountID      string
	Key            string
	State          State
	CreatedAtUnix  int64
	ExpiresAtUnix  int64
	LastUsedAtUnix int64
	InFlight       int
	MaxConcurrency int
	Healthy        bool
	WaitOnFull     bool
}

type SessionScheduleRequest struct {
	ChannelID      string
	SelectionKey   string
	Inbound        *InboundRequest
	Accounts       []AccountCandidate
	Sessions       []SessionCandidate
	PendingCreates []SessionCreateCandidate
	Now            time.Time
}

type SessionCreateCandidate struct {
	ChannelID     string
	AccountID     string
	Key           string
	Model         string
	QuotaGroup    string
	StartedAtUnix int64
}

type SessionCreateLabels struct {
	Model      string
	QuotaGroup string
}

type SessionCreateClassifier interface {
	ClassifySessionCreate(key string, in *InboundRequest) SessionCreateLabels
}

type SessionScheduleAction string

const (
	SessionScheduleCreate SessionScheduleAction = "create"
	SessionScheduleWait   SessionScheduleAction = "wait"
	SessionScheduleReject SessionScheduleAction = "reject"
)

type SessionScheduleDecision struct {
	Action              SessionScheduleAction
	PreferredAccountIDs []string
	WaitTimeout         time.Duration
	Reason              string
	Finish              func()
}

type SessionScheduler interface {
	ScheduleSession(ctx context.Context, req SessionScheduleRequest) (SessionScheduleDecision, error)
}

type SchedulerSnapshotRequest struct {
	Accounts       []AccountCandidate
	Sessions       []SessionCandidate
	PendingCreates []SessionCreateCandidate
	Now            time.Time
}

type SchedulerSnapshotProvider interface {
	SchedulerSnapshot(ctx context.Context, req SchedulerSnapshotRequest) (any, error)
}

type SessionReclaimPolicy interface {
	CanReclaimSessionForRequest(state State, in *InboundRequest) bool
	ReclaimedSessionState(state State) (State, bool)
}

type SessionReclaimExecutor interface {
	ReclaimSessionForRequest(ctx context.Context, acc Account, state State, in *InboundRequest, tp Transport) (State, bool, error)
}

type StreamAdapter interface {
	PrepareStreamOutbound(ctx context.Context, lease *Lease, in *InboundRequest) (*OutboundRequest, error)
	ClassifyStreamResponse(status int, headers http.Header, bodyPreview []byte) ResponseClass
}

type StreamWriter interface {
	io.Writer
	Flush()
}

type StreamRewriter interface {
	RewriteStream(ctx context.Context, lease *Lease, in *InboundRequest, upstream io.Reader, downstream StreamWriter) error
}

type StreamUsageRewriter interface {
	RewriteStreamWithUsage(ctx context.Context, lease *Lease, in *InboundRequest, upstream io.Reader, downstream StreamWriter) (TokenUsage, error)
}

type FinalizeOutcome struct {
	Request  *OutboundRequest
	Response *OutboundResponse
	Status   int
	Class    ResponseClass
	Err      error
}

type Finalizer interface {
	Finalize(ctx context.Context, lease *Lease, outcome FinalizeOutcome) error
}

type SessionFinalizeEvent struct {
	ChannelID      string
	AccountID      string
	LocalSessionID string
	SelectionKey   string
	State          State
	ExpiresAtUnix  int64
	Reason         string
}

// SessionFinalizer is an optional, provider-owned hook for session-scoped
// cleanup. Implementations must return quickly; long remote work should be
// queued by the provider.
type SessionFinalizer interface {
	FinalizeSession(ctx context.Context, event SessionFinalizeEvent)
}

type RetryOutcome struct {
	Request     *OutboundRequest
	Status      int
	BodyPreview []byte
}

// OutboundRetrier is an optional hook for providers that can repair a failed
// prepared request before any response has been sent to the client.
type OutboundRetrier interface {
	RetryOutbound(ctx context.Context, lease *Lease, in *InboundRequest, outcome RetryOutcome) (*OutboundRequest, bool, error)
}

type BackgroundRunner interface {
	Run(ctx context.Context)
}

type SessionPolicy interface {
	SelectionKey(in *InboundRequest) string
	SessionTTL() time.Duration
	MaxSessionsPerAccount() int
	MaxConcurrentPerSession() int

	CreateSession(ctx context.Context, acc Account, key string, tp Transport) (State, error)
	ClassifySessionHealth(s State, c ResponseClass) Verdict

	Heartbeat(ctx context.Context, acc Account, s State, tp Transport) error
}

type SessionExpiryPolicy interface {
	SessionExpiresAt(s State) (time.Time, bool)
}

type AuthFlow interface {
	EnsureAuthenticated(ctx context.Context, acc *Account) error
	CredentialExpired(acc *Account) bool
}

type NoopSessionPolicy struct {
	TTL            time.Duration
	MaxPerAccount  int
	MaxConcurrency int
}

func (p NoopSessionPolicy) SelectionKey(in *InboundRequest) string { return in.ChannelID }
func (p NoopSessionPolicy) SessionTTL() time.Duration {
	if p.TTL == 0 {
		return 24 * time.Hour
	}
	return p.TTL
}
func (p NoopSessionPolicy) MaxSessionsPerAccount() int {
	if p.MaxPerAccount == 0 {
		return 1 << 30
	}
	return p.MaxPerAccount
}
func (p NoopSessionPolicy) MaxConcurrentPerSession() int {
	if p.MaxConcurrency == 0 {
		return 1 << 30
	}
	return p.MaxConcurrency
}
func (p NoopSessionPolicy) CreateSession(_ context.Context, _ Account, _ string, _ Transport) (State, error) {
	return State{}, nil
}
func (p NoopSessionPolicy) ClassifySessionHealth(_ State, c ResponseClass) Verdict {
	switch c {
	case ClassAuthExpired, ClassFatal:
		return VerdictExpire
	default:
		return VerdictHealthy
	}
}
func (p NoopSessionPolicy) Heartbeat(_ context.Context, _ Account, _ State, _ Transport) error {
	return nil
}

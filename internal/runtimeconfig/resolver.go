package runtimeconfig

import (
	"encoding/json"
	"math"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/channelconfig"
	"github.com/marktantongco/freebuff-gateway/internal/session"
)

const (
	metadataSessionKey       = "session"
	metadataMaxConcurrentKey = "max_concurrent_per_session"
)

type Resolver struct {
	channels *channelconfig.Repo
	accounts *accounts.Repo
}

func NewResolver(channels *channelconfig.Repo, accounts *accounts.Repo) *Resolver {
	return &Resolver{channels: channels, accounts: accounts}
}

func (r *Resolver) ResolveSessionPolicy(channelID, accountID string, fallback session.RuntimePolicy) session.RuntimePolicy {
	out := fallback
	if out.MaxConcurrentPerSession < 1 {
		out.MaxConcurrentPerSession = 1
	}
	if r != nil && r.channels != nil && channelID != "" {
		if rec, err := r.channels.Get(channelID); err == nil {
			out.MaxConcurrentPerSession = rec.Config.EffectiveMaxConcurrentPerSession(out.MaxConcurrentPerSession)
			out.WaitOnFull = rec.Config.EffectiveWaitOnFull(out.WaitOnFull)
		}
	}
	if r != nil && r.accounts != nil && accountID != "" {
		if rec, err := r.accounts.Get(accountID); err == nil {
			if max, ok := AccountMaxConcurrentPerSession(rec.Metadata); ok {
				out.MaxConcurrentPerSession = max
			}
		}
	}
	if out.MaxConcurrentPerSession < 1 {
		out.MaxConcurrentPerSession = 1
	}
	return out
}

func AccountMaxConcurrentPerSession(metadata map[string]any) (int, bool) {
	sessionMeta, ok := metadataMap(metadata, metadataSessionKey)
	if !ok {
		return 0, false
	}
	return positiveInt(sessionMeta[metadataMaxConcurrentKey])
}

func SetAccountMaxConcurrentPerSession(metadata map[string]any, max int) map[string]any {
	out := cloneMetadata(metadata)
	sessionMeta, _ := metadataMap(out, metadataSessionKey)
	sessionCopy := cloneMetadata(sessionMeta)
	sessionCopy[metadataMaxConcurrentKey] = max
	out[metadataSessionKey] = sessionCopy
	return out
}

func ClearAccountMaxConcurrentPerSession(metadata map[string]any) map[string]any {
	out := cloneMetadata(metadata)
	sessionMeta, ok := metadataMap(out, metadataSessionKey)
	if !ok {
		return out
	}
	sessionCopy := cloneMetadata(sessionMeta)
	delete(sessionCopy, metadataMaxConcurrentKey)
	if len(sessionCopy) == 0 {
		delete(out, metadataSessionKey)
	} else {
		out[metadataSessionKey] = sessionCopy
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func metadataMap(metadata map[string]any, key string) (map[string]any, bool) {
	if len(metadata) == 0 {
		return nil, false
	}
	raw, ok := metadata[key]
	if !ok || raw == nil {
		return nil, false
	}
	if typed, ok := raw.(map[string]any); ok {
		return typed, true
	}
	return nil, false
}

func positiveInt(v any) (int, bool) {
	switch typed := v.(type) {
	case int:
		return typed, typed > 0
	case int64:
		if typed > 0 && typed <= maxInt() {
			return int(typed), true
		}
	case float64:
		if typed > 0 && math.Trunc(typed) == typed && typed <= float64(maxInt()) {
			return int(typed), true
		}
	case json.Number:
		n, err := typed.Int64()
		if err == nil && n > 0 && n <= maxInt() {
			return int(n), true
		}
	}
	return 0, false
}

func maxInt() int64 {
	return int64(^uint(0) >> 1)
}

func cloneMetadata(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

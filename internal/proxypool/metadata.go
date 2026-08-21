package proxypool

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"freebuff-reverse/internal/channels"
)

const (
	MetadataProxyID  = "proxy_id"
	MetadataProxyURL = "_proxy_url"
)

type Resolver struct {
	repo *Repo
}

func NewResolver(repo *Repo) *Resolver {
	if repo == nil {
		return nil
	}
	return &Resolver{repo: repo}
}

func ProxyIDFromMetadata(metadata map[string]any) (string, bool) {
	if metadata == nil {
		return "", false
	}
	value, ok := metadata[MetadataProxyID].(string)
	if !ok {
		return "", false
	}
	value = strings.TrimSpace(value)
	return value, value != ""
}

func ProxyURLFromMetadata(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	value, _ := metadata[MetadataProxyURL].(string)
	return strings.TrimSpace(value)
}

func SetProxyID(metadata map[string]any, proxyID string) map[string]any {
	out := cloneMetadataWithoutRuntime(metadata)
	proxyID = strings.TrimSpace(proxyID)
	if proxyID == "" {
		delete(out, MetadataProxyID)
		return compactMetadata(out)
	}
	out[MetadataProxyID] = proxyID
	return out
}

func ClearProxyRuntime(metadata map[string]any) map[string]any {
	return compactMetadata(cloneMetadataWithoutRuntime(metadata))
}

func (r *Resolver) ResolveAccountMetadata(_ context.Context, account channels.Account) (map[string]any, error) {
	metadata := cloneMetadataWithoutRuntime(account.Metadata)
	proxyID, ok := ProxyIDFromMetadata(metadata)
	if !ok {
		return compactMetadata(metadata), nil
	}
	rec, err := r.repo.Get(proxyID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return compactMetadata(metadata), channels.AccountUnavailablef("account proxy %q missing", proxyID)
		}
		return compactMetadata(metadata), fmt.Errorf("account proxy %q resolve: %w", proxyID, err)
	}
	if !rec.IsActive {
		return compactMetadata(metadata), channels.AccountUnavailablef("account proxy %q disabled", proxyID)
	}
	metadata[MetadataProxyURL] = rec.ProxyURL
	return metadata, nil
}

func ApplyTransportProfile(metadata map[string]any, profile channels.TransportProfile) channels.TransportProfile {
	if proxyURL := ProxyURLFromMetadata(metadata); proxyURL != "" {
		profile.ProxyURL = proxyURL
	}
	return profile
}

func MergeTransportProfile(base, override channels.TransportProfile) channels.TransportProfile {
	if override.ProxyURL != "" {
		base.ProxyURL = override.ProxyURL
	}
	return base
}

func cloneMetadataWithoutRuntime(metadata map[string]any) map[string]any {
	out := make(map[string]any, len(metadata))
	for k, v := range metadata {
		if k == MetadataProxyURL {
			continue
		}
		out[k] = v
	}
	return out
}

func compactMetadata(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

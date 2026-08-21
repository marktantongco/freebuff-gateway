package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/accounts"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/session"
)

type freeBuffRestoreDeps struct {
	registry         *channels.Registry
	accountRepo      *accounts.Repo
	sessions         *session.Manager
	transport        channels.Transport
	stateRepo        *freebuffstate.Repo
	metadataResolver session.AccountMetadataResolver
}

func restoreFreeBuffSessions(ctx context.Context, deps freeBuffRestoreDeps) {
	if deps.registry == nil || deps.accountRepo == nil || deps.sessions == nil || deps.transport == nil || deps.stateRepo == nil {
		return
	}
	adapter, ok := deps.registry.Get(freebuffstate.ChannelID)
	if !ok {
		return
	}
	restorer, ok := adapter.(channels.SessionRestorer)
	if !ok {
		log.Printf("gateway: freebuff session restore unavailable reason=adapter_missing_restorer")
		return
	}

	candidates, err := deps.stateRepo.ListActiveUpstreamSessions(ctx, time.Now())
	if err != nil {
		log.Printf("gateway: freebuff session restore list failed: %v", err)
		return
	}
	if len(candidates) == 0 {
		return
	}

	restored := 0
	skipped := 0
	failed := 0
	for _, candidate := range candidates {
		result := restoreFreeBuffSession(ctx, deps, restorer, candidate)
		switch result {
		case restoreResultRestored:
			restored++
		case restoreResultSkipped:
			skipped++
		case restoreResultFailed:
			failed++
		}
	}
	log.Printf("gateway: freebuff session restore candidates=%d restored=%d skipped=%d failed=%d", len(candidates), restored, skipped, failed)
}

type restoreResult int

const (
	restoreResultSkipped restoreResult = iota
	restoreResultRestored
	restoreResultFailed
)

func restoreFreeBuffSession(ctx context.Context, deps freeBuffRestoreDeps, restorer channels.SessionRestorer, candidate freebuffstate.UpstreamSession) restoreResult {
	accountID := strings.TrimSpace(candidate.AccountID)
	model := strings.TrimSpace(candidate.Model)
	if accountID == "" || model == "" || strings.TrimSpace(candidate.InstanceID) == "" {
		log.Printf("gateway: freebuff session restore skipped account_id=%s model=%s reason=incomplete_candidate", accountID, model)
		return restoreResultSkipped
	}

	rec, err := deps.accountRepo.Get(accountID)
	if err != nil {
		if errors.Is(err, accounts.ErrNotFound) {
			log.Printf("gateway: freebuff session restore skipped account_id=%s model=%s reason=account_missing", accountID, model)
			return restoreResultSkipped
		}
		log.Printf("gateway: freebuff session restore failed account_id=%s model=%s stage=account_lookup error=%v", accountID, model, err)
		return restoreResultFailed
	}
	if rec.ChannelID != freebuffstate.ChannelID || !rec.IsActive {
		log.Printf("gateway: freebuff session restore skipped account_id=%s model=%s reason=account_ineligible", accountID, model)
		return restoreResultSkipped
	}

	acc := rec.ToChannel()
	if deps.metadataResolver != nil {
		metadata, err := deps.metadataResolver.ResolveAccountMetadata(ctx, acc)
		if err != nil {
			reason := "account_metadata_failed"
			if errors.Is(err, channels.ErrAccountUnavailable) {
				reason = "account_metadata_unavailable"
			}
			log.Printf("gateway: freebuff session restore skipped account_id=%s model=%s reason=%s", accountID, model, reason)
			return restoreResultSkipped
		}
		acc.Metadata = metadata
	}

	key := freebuffstate.ChannelID + "|" + model
	state, valid, err := restorer.RestoreSession(ctx, acc, key, candidate.RestoreState(), deps.transport)
	if err != nil {
		log.Printf("gateway: freebuff session restore failed account_id=%s model=%s stage=validate error=%v", accountID, model, err)
		return restoreResultFailed
	}
	if !valid {
		log.Printf("gateway: freebuff session restore skipped account_id=%s model=%s reason=upstream_inactive_or_mismatch", accountID, model)
		return restoreResultSkipped
	}

	registered, err := deps.sessions.RegisterRestoredSession(ctx, freebuffstate.ChannelID, accountID, key, state)
	if err != nil {
		log.Printf("gateway: freebuff session restore failed account_id=%s model=%s stage=register error=%v", accountID, model, err)
		return restoreResultFailed
	}
	if !registered {
		log.Printf("gateway: freebuff session restore skipped account_id=%s model=%s reason=already_registered", accountID, model)
		return restoreResultSkipped
	}
	return restoreResultRestored
}

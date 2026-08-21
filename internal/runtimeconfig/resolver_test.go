package runtimeconfig

import (
	"path/filepath"
	"testing"

	"freebuff-reverse/internal/accounts"
	"freebuff-reverse/internal/channelconfig"
	"freebuff-reverse/internal/session"
	"freebuff-reverse/internal/storage"
)

func TestAccountMaxConcurrentMetadata(t *testing.T) {
	meta := map[string]any{"auth_method": "credential"}
	meta = SetAccountMaxConcurrentPerSession(meta, 2)
	if meta["auth_method"] != "credential" {
		t.Fatalf("existing metadata was not preserved: %+v", meta)
	}
	max, ok := AccountMaxConcurrentPerSession(meta)
	if !ok || max != 2 {
		t.Fatalf("override = %d/%t, want 2/true", max, ok)
	}
	meta = ClearAccountMaxConcurrentPerSession(meta)
	if _, ok := AccountMaxConcurrentPerSession(meta); ok {
		t.Fatalf("override was not cleared: %+v", meta)
	}
	if meta["auth_method"] != "credential" {
		t.Fatalf("clear removed unrelated metadata: %+v", meta)
	}
}

func TestResolverPrecedence(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "resolver.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	channelRepo := channelconfig.NewRepo(db)
	accountRepo := accounts.NewRepo(db)
	wait := true
	if _, err := channelRepo.Upsert("freebuff", "freebuff", true, channelconfig.Config{
		MaxConcurrentPerSession: 2,
		WaitOnFull:              &wait,
	}); err != nil {
		t.Fatalf("upsert channel config: %v", err)
	}
	rec := &accounts.Record{
		ChannelID:  "freebuff",
		Name:       "Ada",
		Credential: "token",
		IsActive:   true,
		Metadata:   SetAccountMaxConcurrentPerSession(nil, 3),
	}
	if err := accountRepo.Create(rec); err != nil {
		t.Fatalf("create account: %v", err)
	}

	resolver := NewResolver(channelRepo, accountRepo)
	got := resolver.ResolveSessionPolicy("freebuff", rec.ID, session.RuntimePolicy{
		MaxConcurrentPerSession: 1,
		WaitOnFull:              false,
	})
	if got.MaxConcurrentPerSession != 3 || !got.WaitOnFull {
		t.Fatalf("resolved account override = %+v, want max 3 and wait true", got)
	}
	got = resolver.ResolveSessionPolicy("freebuff", "missing", session.RuntimePolicy{
		MaxConcurrentPerSession: 1,
		WaitOnFull:              false,
	})
	if got.MaxConcurrentPerSession != 2 || !got.WaitOnFull {
		t.Fatalf("resolved channel default = %+v, want max 2 and wait true", got)
	}
}

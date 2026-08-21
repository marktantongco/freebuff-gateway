package authkeys

import (
	"errors"
	"path/filepath"
	"testing"

	"freebuff-reverse/internal/storage"
)

func TestCreateListsAndAuthenticatesKeyWithoutPersistingSecret(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "authkeys.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	secret := "sk-test-secret-value"
	created, err := repo.CreateWithKey("client", secret)
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if created.Key != secret {
		t.Fatalf("created key = %q", created.Key)
	}
	if created.KeyPrefix != secret[:14] {
		t.Fatalf("prefix = %q", created.KeyPrefix)
	}

	keys, err := repo.List()
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 1 || keys[0].ID != created.ID {
		t.Fatalf("keys = %+v", keys)
	}
	if keys[0].KeyPrefix == secret {
		t.Fatalf("listed key leaked full secret: %+v", keys[0])
	}

	got, err := repo.Authenticate(secret)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if got.ID != created.ID || got.LastUsedAt == 0 {
		t.Fatalf("authenticated record = %+v", got)
	}

	if _, err := repo.Authenticate("sk-wrong"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong key err = %v, want ErrNotFound", err)
	}
}

func TestDeleteKeyDisablesAuthentication(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "authkeys-delete.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	repo := NewRepo(db)
	created, err := repo.CreateWithKey("client", "sk-delete-me")
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if err := repo.Delete(created.ID); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if _, err := repo.Authenticate("sk-delete-me"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key err = %v, want ErrNotFound", err)
	}
}

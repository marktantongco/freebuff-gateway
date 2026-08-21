package usermgmt

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCreateUser(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, err := repo.Create("secret123", "admin", "alice")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty id")
	}
	u, err := repo.Get(id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if u.Username != "alice" {
		t.Errorf("username = %q, want alice", u.Username)
	}
	if u.Role != "admin" {
		t.Errorf("role = %q, want admin", u.Role)
	}
	if !u.Active {
		t.Error("expected active")
	}
}

func TestCreateDuplicateUsername(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Create("pass1", "admin", "bob")
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Create("pass2", "viewer", "bob")
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestAuthenticate(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Create("mypassword", "admin", "charlie")
	if err != nil {
		t.Fatal(err)
	}
	u, err := repo.Authenticate("charlie", "mypassword")
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if u.Username != "charlie" {
		t.Errorf("username = %q, want charlie", u.Username)
	}
}

func TestAuthenticateWrongPassword(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Create("correct", "admin", "dave")
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Authenticate("dave", "wrong")
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

func TestListUsers(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	repo.Create("pass1", "admin", "user1")
	repo.Create("pass2", "viewer", "user2")
	users, err := repo.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Errorf("got %d users, want 2", len(users))
	}
}

func TestUpdateUserRole(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := repo.Create("pass", "viewer", "eve")
	err = repo.Update(id, "", "admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := repo.Get(id)
	if u.Role != "admin" {
		t.Errorf("role = %q, want admin", u.Role)
	}
}

func TestUpdateUserPassword(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := repo.Create("oldpass", "admin", "frank")
	err = repo.Update(id, "newpass", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Old password should fail
	_, err = repo.Authenticate("frank", "oldpass")
	if err == nil {
		t.Fatal("expected old password to fail")
	}
	// New password should work
	_, err = repo.Authenticate("frank", "newpass")
	if err != nil {
		t.Fatalf("new password auth failed: %v", err)
	}
}

func TestDeactivateUser(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	repo.Create("pass", "admin", "admin1")
	repo.Create("pass", "admin", "admin2")
	// Deactivate admin1
	users, _ := repo.List()
	var admin1ID string
	for _, u := range users {
		if u.Username == "admin1" {
			admin1ID = u.ID
			break
		}
	}
_FALSE := false
	err = repo.Update(admin1ID, "", "", &_FALSE)
	if err != nil {
		t.Fatal(err)
	}
	// Auth should fail
	_, err = repo.Authenticate("admin1", "pass")
	if err == nil {
		t.Fatal("expected auth failure for deactivated user")
	}
}

func TestDeleteUser(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := repo.Create("pass", "viewer", "to_delete")
	err = repo.Delete(id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.Get(id)
	if err == nil {
		t.Fatal("expected not found")
	}
}

func TestDeleteLastAdmin(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := repo.Create("pass", "admin", "solo_admin")
	err = repo.Delete(id)
	if err == nil {
		t.Fatal("expected error deleting last admin")
	}
}

func TestSeed(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	err = repo.Seed("defaultpass")
	if err != nil {
		t.Fatal(err)
	}
	// Should have one user
	users, _ := repo.List()
	if len(users) != 1 {
		t.Errorf("got %d users after seed, want 1", len(users))
	}
	// Seed again should be no-op
	err = repo.Seed("otherpass")
	if err != nil {
		t.Fatal(err)
	}
	users, _ = repo.List()
	if len(users) != 1 {
		t.Errorf("got %d users after second seed, want 1", len(users))
	}
}

func TestChangePassword(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := repo.Create("oldpass", "admin", "pw_user")
	err = repo.ChangePassword(id, "oldpass", "newpass")
	if err != nil {
		t.Fatal(err)
	}
	// Old password fails
	_, err = repo.Authenticate("pw_user", "oldpass")
	if err == nil {
		t.Fatal("expected old password to fail")
	}
	// New password works
	_, err = repo.Authenticate("pw_user", "newpass")
	if err != nil {
		t.Fatalf("new password auth failed: %v", err)
	}
}

func TestChangePasswordWrongOld(t *testing.T) {
	db := setupTestDB(t)
	repo, err := NewRepo(db)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := repo.Create("correct", "admin", "cp_user")
	err = repo.ChangePassword(id, "wrong", "newpass")
	if err == nil {
		t.Fatal("expected error with wrong old password")
	}
}

func TestToResponse(t *testing.T) {
	u := &User{
		ID:       "u_123",
		Username: "test",
		Role:     "admin",
		Active:   true,
	}
	resp := u.ToResponse()
	if resp.ID != "u_123" {
		t.Errorf("id = %q", resp.ID)
	}
	if resp.Username != "test" {
		t.Errorf("username = %q", resp.Username)
	}
	if resp.Role != "admin" {
		t.Errorf("role = %q", resp.Role)
	}
	if !resp.Active {
		t.Error("expected active")
	}
}

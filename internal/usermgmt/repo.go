package usermgmt

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Repo manages users in SQLite.
type Repo struct {
	db *sql.DB
}

// NewRepo creates a new user repository.
func NewRepo(db *sql.DB) (*Repo, error) {
	r := &Repo{db: db}
	if err := r.migrate(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repo) migrate() error {
	const ddl = `
	CREATE TABLE IF NOT EXISTS admin_users (
		id            TEXT PRIMARY KEY,
		username      TEXT NOT NULL UNIQUE,
		password_hash TEXT NOT NULL,
		salt          TEXT NOT NULL,
		role          TEXT NOT NULL DEFAULT 'admin',
		active        INTEGER NOT NULL DEFAULT 1,
		created_at    INTEGER NOT NULL,
		updated_at    INTEGER NOT NULL,
		last_login    INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_admin_users_username ON admin_users(username);
	CREATE INDEX IF NOT EXISTS idx_admin_users_active ON admin_users(active);
	`
	if _, err := r.db.Exec(ddl); err != nil {
		return fmt.Errorf("users migrate: %w", err)
	}
	return nil
}

// Seed creates the default admin user if no users exist.
func (r *Repo) Seed(defaultPassword string) error {
	var count int
	if err := r.db.QueryRow(`SELECT COUNT(*) FROM admin_users`).Scan(&count); err != nil {
		return fmt.Errorf("users count: %w", err)
	}
	if count > 0 {
		return nil
	}
	_, err := r.Create(defaultPassword, "admin", "admin")
	return err
}

// Create adds a new user. Returns the generated ID.
func (r *Repo) Create(password, role, username string) (string, error) {
	if username == "" {
		return "", fmt.Errorf("username is required")
	}
	if password == "" {
		return "", fmt.Errorf("password is required")
	}
	if !ValidRoles[role] {
		role = "viewer"
	}
	id := GenerateID()
	salt := GenerateSalt()
	hash := HashPassword(password, salt)
	now := time.Now().UnixMilli()
	_, err := r.db.Exec(
		`INSERT INTO admin_users (id, username, password_hash, salt, role, active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, 1, ?, ?)`,
		id, strings.ToLower(strings.TrimSpace(username)), hash, salt, role, now, now,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return "", fmt.Errorf("username %q already exists", username)
		}
		return "", fmt.Errorf("create user: %w", err)
	}
	return id, nil
}

// Authenticate checks username+password and returns the user on success.
func (r *Repo) Authenticate(username, password string) (*User, error) {
	u := &User{}
	var createdAt, updatedAt int64
	var lastLogin sql.NullInt64
	err := r.db.QueryRow(
		`SELECT id, username, password_hash, salt, role, active, created_at, updated_at, last_login
		 FROM admin_users WHERE username = ? AND active = 1`,
		strings.ToLower(strings.TrimSpace(username)),
	).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Salt, &u.Role, &u.Active, &createdAt, &updatedAt, &lastLogin)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid credentials")
	}
	if err != nil {
		return nil, fmt.Errorf("authenticate: %w", err)
	}
	u.CreatedAt = time.UnixMilli(createdAt)
	u.UpdatedAt = time.UnixMilli(updatedAt)
	if HashPassword(password, u.Salt) != u.PasswordHash {
		return nil, fmt.Errorf("invalid credentials")
	}
	if lastLogin.Valid {
		u.LastLogin = time.UnixMilli(lastLogin.Int64)
	}
	// Update last_login
	_, _ = r.db.Exec(`UPDATE admin_users SET last_login = ? WHERE id = ?`, time.Now().UnixMilli(), u.ID)
	return u, nil
}

// List returns all users.
func (r *Repo) List() ([]User, error) {
	rows, err := r.db.Query(
		`SELECT id, username, role, active, created_at, updated_at, last_login FROM admin_users ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		var u User
		var createdAt, updatedAt int64
		var lastLogin sql.NullInt64
		if err := rows.Scan(&u.ID, &u.Username, &u.Role, &u.Active, &createdAt, &updatedAt, &lastLogin); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		u.CreatedAt = time.UnixMilli(createdAt)
		u.UpdatedAt = time.UnixMilli(updatedAt)
		if lastLogin.Valid {
			u.LastLogin = time.UnixMilli(lastLogin.Int64)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// Get returns a user by ID.
func (r *Repo) Get(id string) (*User, error) {
	u := &User{}
	var createdAt, updatedAt int64
	var lastLogin sql.NullInt64
	err := r.db.QueryRow(
		`SELECT id, username, password_hash, salt, role, active, created_at, updated_at, last_login FROM admin_users WHERE id = ?`, id,
	).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Salt, &u.Role, &u.Active, &createdAt, &updatedAt, &lastLogin)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found")
	}
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	u.CreatedAt = time.UnixMilli(createdAt)
	u.UpdatedAt = time.UnixMilli(updatedAt)
	if lastLogin.Valid {
		u.LastLogin = time.UnixMilli(lastLogin.Int64)
	}
	return u, nil
}

// Update modifies user fields. Pass empty values to skip.
func (r *Repo) Update(id, password, role string, active *bool) error {
	u, err := r.Get(id)
	if err != nil {
		return err
	}
	sets := []string{}
	args := []interface{}{}

	if password != "" {
		salt := GenerateSalt()
		hash := HashPassword(password, salt)
		sets = append(sets, "password_hash = ?, salt = ?")
		args = append(args, hash, salt)
	}
	if role != "" && ValidRoles[role] {
		sets = append(sets, "role = ?")
		args = append(args, role)
	}
	if active != nil {
		sets = append(sets, "active = ?")
		if *active {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}
	if len(sets) == 0 {
		return nil
	}
	sets = append(sets, "updated_at = ?")
	args = append(args, time.Now().UnixMilli())
	args = append(args, id)
	query := fmt.Sprintf("UPDATE admin_users SET %s WHERE id = ?", strings.Join(sets, ", "))
	_, err = r.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	_ = u // used for validation
	return nil
}

// Delete removes a user by ID. Prevents deleting the last admin.
func (r *Repo) Delete(id string) error {
	// Check if this is the last admin
	var adminCount int
	if err := r.db.QueryRow(`SELECT COUNT(*) FROM admin_users WHERE role = 'admin' AND active = 1`).Scan(&adminCount); err != nil {
		return fmt.Errorf("count admins: %w", err)
	}
	var userRole string
	if err := r.db.QueryRow(`SELECT role FROM admin_users WHERE id = ?`, id).Scan(&userRole); err == sql.ErrNoRows {
		return fmt.Errorf("user not found")
	}
	if userRole == "admin" && adminCount <= 1 {
		return fmt.Errorf("cannot delete the last admin user")
	}
	_, err := r.db.Exec(`DELETE FROM admin_users WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	return nil
}

// ChangePassword changes a user's password.
func (r *Repo) ChangePassword(id, oldPassword, newPassword string) error {
	u, err := r.Get(id)
	if err != nil {
		return err
	}
	if HashPassword(oldPassword, u.Salt) != u.PasswordHash {
		return fmt.Errorf("current password is incorrect")
	}
	salt := GenerateSalt()
	hash := HashPassword(newPassword, salt)
	_, err = r.db.Exec(`UPDATE admin_users SET password_hash = ?, salt = ?, updated_at = ? WHERE id = ?`,
		hash, salt, time.Now().UnixMilli(), id)
	return err
}

// GenerateSalt creates a random salt.
func genSalt() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

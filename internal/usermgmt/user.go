package usermgmt

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// User represents an admin user.
type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	Salt         string    `json:"-"`
	Role         string    `json:"role"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	LastLogin    time.Time `json:"last_login,omitempty"`
	Active       bool      `json:"active"`
}

// UserCreateRequest is the request to create a user.
type UserCreateRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role,omitempty"`
}

// UserResponse is the response for user operations.
type UserResponse struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	Active   bool   `json:"active"`
}

// HashPassword hashes a password with a salt.
func HashPassword(password, salt string) string {
	h := sha256.New()
	h.Write([]byte(salt + password))
	return hex.EncodeToString(h.Sum(nil))
}

// GenerateSalt creates a random salt.
func GenerateSalt() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// GenerateID creates a random user ID.
func GenerateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return "u_" + hex.EncodeToString(b)
}

// ToResponse converts a User to a UserResponse.
func (u *User) ToResponse() UserResponse {
	return UserResponse{
		ID:       u.ID,
		Username: u.Username,
		Role:     u.Role,
		Active:   u.Active,
	}
}

// ValidRoles are the valid user roles.
var ValidRoles = map[string]bool{
	"admin":  true,
	"viewer": true,
}

package credential

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"
)

// DecryptedToken represents a decrypted token with metadata
type DecryptedToken struct {
	ID            string       `json:"id"`
	Provider      ProviderType `json:"provider"`
	Value         string       `json:"-"` // Never serialized
	ValueHash     string       `json:"value_hash"`
	DecryptedAt   time.Time    `json:"decrypted_at"`
	ExpiresAt     *time.Time   `json:"expires_at,omitempty"`
	IsValid       bool         `json:"is_valid"`
	LastValidated *time.Time   `json:"last_validated,omitempty"`
}

// CredentialManager handles decryption and caching of tokens
type CredentialManager struct {
	discovery *TokenDiscovery
	cache     map[string]*DecryptedToken
	mu        sync.RWMutex
	config    ManagerConfig
}

// ManagerConfig configures the credential manager
type ManagerConfig struct {
	// CacheDuration is how long to cache decrypted tokens
	CacheDuration time.Duration `json:"cache_duration"`
	// MaxCacheSize is the maximum number of cached tokens
	MaxCacheSize int `json:"max_cache_size"`
	// AutoRefresh enables automatic token refresh before expiry
	AutoRefresh bool `json:"auto_refresh"`
}

// DefaultManagerConfig returns the default configuration
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		CacheDuration: 15 * time.Minute,
		MaxCacheSize:  100,
		AutoRefresh:   true,
	}
}

// NewCredentialManager creates a new credential manager
func NewCredentialManager(discovery *TokenDiscovery, config ManagerConfig) *CredentialManager {
	return &CredentialManager{
		discovery: discovery,
		cache:     make(map[string]*DecryptedToken),
		config:    config,
	}
}

// GetToken retrieves and decrypts a token by ID
func (cm *CredentialManager) GetToken(id string) (*DecryptedToken, error) {
	cm.mu.RLock()
	if cached, ok := cm.cache[id]; ok && cm.isCacheValid(cached) {
		cm.mu.RUnlock()
		return cached, nil
	}
	cm.mu.RUnlock()

	// Decrypt token
	token, err := cm.decryptToken(id)
	if err != nil {
		return nil, fmt.Errorf("decrypt token %s: %w", id, err)
	}

	// Cache the decrypted token
	cm.mu.Lock()
	cm.cache[id] = token
	cm.mu.Unlock()

	return token, nil
}

// GetTokenByProvider retrieves a random valid token for a specific provider
func (cm *CredentialManager) GetTokenByProvider(provider ProviderType) (*DecryptedToken, error) {
	tokens := cm.discovery.GetByProvider(provider)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("no tokens found for provider %s", provider)
	}

	// Try each token until we find a valid one
	for _, tokenInfo := range tokens {
		token, err := cm.GetToken(tokenInfo.ID)
		if err == nil && token.IsValid {
			return token, nil
		}
	}

	return nil, fmt.Errorf("no valid tokens for provider %s", provider)
}

// GetAllTokens returns all decrypted tokens (cached or freshly decrypted)
func (cm *CredentialManager) GetAllTokens() ([]*DecryptedToken, error) {
	allTokens := cm.discovery.GetTokens()
	var result []*DecryptedToken

	for id := range allTokens {
		token, err := cm.GetToken(id)
		if err == nil {
			result = append(result, token)
		}
	}

	return result, nil
}

// GetTokensByProvider returns all valid tokens for a provider
func (cm *CredentialManager) GetTokensByProvider(provider ProviderType) ([]*DecryptedToken, error) {
	tokens := cm.discovery.GetByProvider(provider)
	var result []*DecryptedToken

	for _, tokenInfo := range tokens {
		token, err := cm.GetToken(tokenInfo.ID)
		if err == nil && token.IsValid {
			result = append(result, token)
		}
	}

	return result, nil
}

// RefreshToken forces a refresh of a cached token
func (cm *CredentialManager) RefreshToken(id string) (*DecryptedToken, error) {
	cm.mu.Lock()
	delete(cm.cache, id)
	cm.mu.Unlock()

	return cm.GetToken(id)
}

// ValidateToken checks if a token is valid without caching
func (cm *CredentialManager) ValidateToken(id string) (bool, error) {
	token, err := cm.decryptToken(id)
	if err != nil {
		return false, err
	}
	return token.IsValid, nil
}

// ClearCache removes all cached tokens
func (cm *CredentialManager) ClearCache() {
	cm.mu.Lock()
	cm.cache = make(map[string]*DecryptedToken)
	cm.mu.Unlock()
}

// GetCacheStats returns cache statistics
func (cm *CredentialManager) GetCacheStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	valid := 0
	expired := 0
	for _, token := range cm.cache {
		if cm.isCacheValid(token) {
			valid++
		} else {
			expired++
		}
	}

	return map[string]interface{}{
		"total_cached":  len(cm.cache),
		"valid_cached":  valid,
		"expired_cached": expired,
		"max_size":      cm.config.MaxCacheSize,
	}
}

// decryptToken decrypts a token from the discovery info
func (cm *CredentialManager) decryptToken(id string) (*DecryptedToken, error) {
	tokens := cm.discovery.GetTokens()
	tokenInfo, ok := tokens[id]
	if !ok {
		return nil, fmt.Errorf("token %s not found", id)
	}

	// Read the encryption key
	keyData, err := os.ReadFile(tokenInfo.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}

	// Read the encrypted token
	encData, err := os.ReadFile(tokenInfo.EncryptedPath)
	if err != nil {
		return nil, fmt.Errorf("read encrypted token: %w", err)
	}

	// Decrypt using Fernet
	value, err := FernetDecrypt(string(keyData), string(encData))
	if err != nil {
		return nil, fmt.Errorf("fernet decrypt: %w", err)
	}

	// Generate hash of the value (for logging without exposing)
	hash := sha256.Sum256([]byte(value))
	valueHash := hex.EncodeToString(hash[:8])

	now := time.Now()
	return &DecryptedToken{
		ID:          id,
		Provider:    tokenInfo.Provider,
		Value:       value,
		ValueHash:   valueHash,
		DecryptedAt: now,
		IsValid:     len(value) > 0 && value != "PLACEHOLDER",
	}, nil
}

// isCacheValid checks if a cached token is still valid
func (cm *CredentialManager) isCacheValid(token *DecryptedToken) bool {
	if token == nil {
		return false
	}

	// Check cache duration
	if time.Since(token.DecryptedAt) > cm.config.CacheDuration {
		return false
	}

	// Check expiry if set
	if token.ExpiresAt != nil && time.Now().After(*token.ExpiresAt) {
		return false
	}

	return true
}

// ProviderCredentials returns credentials grouped by provider
type ProviderCredentials struct {
	OpenAI   []*DecryptedToken `json:"openai,omitempty"`
	Gemini   []*DecryptedToken `json:"gemini,omitempty"`
	Nvidia   []*DecryptedToken `json:"nvidia,omitempty"`
	Tool     []*DecryptedToken `json:"tool,omitempty"`
	GitHub   []*DecryptedToken `json:"github,omitempty"`
	Vercel   []*DecryptedToken `json:"vercel,omitempty"`
	Database []*DecryptedToken `json:"database,omitempty"`
}

// GetCredentialsByProvider returns all credentials grouped by provider
func (cm *CredentialManager) GetCredentialsByProvider() (*ProviderCredentials, error) {
	creds := &ProviderCredentials{}

	providers := []ProviderType{
		ProviderOpenAI, ProviderGemini, ProviderNvidia,
		ProviderTool, ProviderGitHub, ProviderVercel, ProviderDatabase,
	}

	for _, provider := range providers {
		tokens, err := cm.GetTokensByProvider(provider)
		if err != nil {
			continue // Skip providers with no tokens
		}

		switch provider {
		case ProviderOpenAI:
			creds.OpenAI = tokens
		case ProviderGemini:
			creds.Gemini = tokens
		case ProviderNvidia:
			creds.Nvidia = tokens
		case ProviderTool:
			creds.Tool = tokens
		case ProviderGitHub:
			creds.GitHub = tokens
		case ProviderVercel:
			creds.Vercel = tokens
		case ProviderDatabase:
			creds.Database = tokens
		}
	}

	return creds, nil
}

// TokenSummary provides a summary of available tokens
type TokenSummary struct {
	TotalDiscovered int                    `json:"total_discovered"`
	TotalDecrypted  int                    `json:"total_decrypted"`
	TotalValid      int                    `json:"total_valid"`
	ByProvider      map[ProviderType]int   `json:"by_provider"`
	LastScan        time.Time              `json:"last_scan"`
}

// GetSummary returns a summary of all tokens
func (cm *CredentialManager) GetSummary() (*TokenSummary, error) {
	allTokens := cm.discovery.GetTokens()
	summary := &TokenSummary{
		TotalDiscovered: len(allTokens),
		ByProvider:      make(map[ProviderType]int),
		LastScan:        time.Now(),
	}

	for id := range allTokens {
		token, err := cm.GetToken(id)
		if err == nil {
			summary.TotalDecrypted++
			if token.IsValid {
				summary.TotalValid++
			}
			summary.ByProvider[token.Provider]++
		}
	}

	return summary, nil
}

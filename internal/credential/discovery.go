// Package credential provides secure token discovery, decryption, and management
// for the Freebuff Gateway. It scans the ~/secure-tokens/ directory structure
// and integrates encrypted tokens into the gateway's provider registry.
package credential

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ProviderType represents the type of AI provider
type ProviderType string

const (
	ProviderOpenAI   ProviderType = "openai"
	ProviderGemini   ProviderType = "gemini"
	ProviderNvidia   ProviderType = "nvidia"
	ProviderTool     ProviderType = "tool"
	ProviderGitHub   ProviderType = "github"
	ProviderVercel   ProviderType = "vercel"
	ProviderDatabase ProviderType = "database"
	ProviderUnknown  ProviderType = "unknown"
)

// TokenInfo represents a discovered encrypted token
type TokenInfo struct {
	ID            string       `json:"id"`
	Provider      ProviderType `json:"provider"`
	EncryptedPath string       `json:"encrypted_path"`
	KeyPath       string       `json:"key_path"`
	Fingerprint   string       `json:"fingerprint"`
	DiscoveredAt  time.Time    `json:"discovered_at"`
}

// DiscoveryConfig configures the token discovery process
type DiscoveryConfig struct {
	// BasePath is the root directory to scan (default: ~/secure-tokens/)
	BasePath string `json:"base_path"`
	// ScanInterval is how often to rescan for new tokens
	ScanInterval time.Duration `json:"scan_interval"`
	// IncludeProviders filters discovery to specific providers (empty = all)
	IncludeProviders []ProviderType `json:"include_providers,omitempty"`
	// ExcludeProviders excludes specific providers from discovery
	ExcludeProviders []ProviderType `json:"exclude_providers,omitempty"`
}

// DefaultDiscoveryConfig returns the default configuration
func DefaultDiscoveryConfig() DiscoveryConfig {
	home, _ := os.UserHomeDir()
	return DiscoveryConfig{
		BasePath:     filepath.Join(home, "secure-tokens"),
		ScanInterval: 5 * time.Minute,
	}
}

// TokenDiscovery handles scanning and discovering encrypted tokens
type TokenDiscovery struct {
	config    DiscoveryConfig
	mu        sync.RWMutex
	tokens    map[string]*TokenInfo
	providers map[ProviderType][]*TokenInfo
	onChange  func(map[string]*TokenInfo)
}

// NewTokenDiscovery creates a new token discovery instance
func NewTokenDiscovery(config DiscoveryConfig) *TokenDiscovery {
	return &TokenDiscovery{
		config:    config,
		tokens:    make(map[string]*TokenInfo),
		providers: make(map[ProviderType][]*TokenInfo),
	}
}

// SetOnChange registers a callback for when tokens are discovered/removed
func (td *TokenDiscovery) SetOnChange(fn func(map[string]*TokenInfo)) {
	td.onChange = fn
}

// Scan performs a one-time scan of the secure-tokens directory
func (td *TokenDiscovery) Scan() error {
	td.mu.Lock()
	defer td.mu.Unlock()

	newTokens := make(map[string]*TokenInfo)
	newProviders := make(map[ProviderType][]*TokenInfo)

	// Scan all subdirectories
	err := filepath.Walk(td.config.BasePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Skip .git and hidden directories
		if info.IsDir() && (strings.HasPrefix(info.Name(), ".") || info.Name() == ".git") {
			return filepath.SkipDir
		}

		// Look for .fernet.enc files
		if !info.IsDir() && strings.HasSuffix(path, ".fernet.enc") {
			token := td.parseTokenFile(path)
			if token != nil {
				// Check provider filter
				if td.isProviderAllowed(token.Provider) {
					newTokens[token.ID] = token
					newProviders[token.Provider] = append(newProviders[token.Provider], token)
				}
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("walk secure-tokens: %w", err)
	}

	// Detect changes
	if td.onChange != nil && len(newTokens) != len(td.tokens) {
		td.onChange(newTokens)
	}

	td.tokens = newTokens
	td.providers = newProviders

	return nil
}

// parseTokenFile extracts token info from an encrypted file path
func (td *TokenDiscovery) parseTokenFile(encPath string) *TokenInfo {
	// Find corresponding .key file
	keyPath := strings.TrimSuffix(encPath, ".fernet.enc") + ".key"
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		return nil // No key file, skip
	}

	// Extract provider from directory structure
	provider := td.extractProvider(encPath)
	if provider == ProviderUnknown {
		return nil
	}

	// Generate unique ID from path
	id := td.generateID(encPath)

	// Generate fingerprint from encrypted content
	fingerprint, _ := td.generateFingerprint(encPath)

	return &TokenInfo{
		ID:            id,
		Provider:      provider,
		EncryptedPath: encPath,
		KeyPath:       keyPath,
		Fingerprint:   fingerprint,
		DiscoveredAt:  time.Now(),
	}
}

// extractProvider determines the provider type from the file path
func (td *TokenDiscovery) extractProvider(path string) ProviderType {
	pathLower := strings.ToLower(path)

	// Check ai_agents subdirectories first
	if strings.Contains(pathLower, "ai_agents/openai") || strings.Contains(pathLower, "openai/") {
		return ProviderOpenAI
	}
	if strings.Contains(pathLower, "ai_agents/gemini") || strings.Contains(pathLower, "gemini/") {
		return ProviderGemini
	}
	if strings.Contains(pathLower, "ai_agents/nvidia") || strings.Contains(pathLower, "nvidia/") {
		return ProviderNvidia
	}
	if strings.Contains(pathLower, "ai_agents/tool") || strings.Contains(pathLower, "tool/") {
		return ProviderTool
	}

	// Check top-level directories
	if strings.Contains(pathLower, "gh/") || strings.Contains(pathLower, "github") {
		return ProviderGitHub
	}
	if strings.Contains(pathLower, "vercel/") {
		return ProviderVercel
	}
	if strings.Contains(pathLower, "db/") || strings.Contains(pathLower, "database") {
		return ProviderDatabase
	}

	return ProviderUnknown
}

// generateID creates a unique ID for a token based on its path
func (td *TokenDiscovery) generateID(path string) string {
	// Use relative path from base path
	rel, err := filepath.Rel(td.config.BasePath, path)
	if err != nil {
		rel = path
	}
	// Clean and normalize
	rel = strings.ReplaceAll(rel, "/", ":")
	rel = strings.TrimSuffix(rel, ".fernet.enc")
	return rel
}

// generateFingerprint creates a SHA-256 fingerprint of the encrypted file
func (td *TokenDiscovery) generateFingerprint(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:8]), nil // First 8 bytes for brevity
}

// isProviderAllowed checks if a provider is allowed by the filter
func (td *TokenDiscovery) isProviderAllowed(provider ProviderType) bool {
	// Check exclusion list
	for _, p := range td.config.ExcludeProviders {
		if p == provider {
			return false
		}
	}

	// Check inclusion list (empty = all allowed)
	if len(td.config.IncludeProviders) == 0 {
		return true
	}

	for _, p := range td.config.IncludeProviders {
		if p == provider {
			return true
		}
	}

	return false
}

// GetTokens returns all discovered tokens
func (td *TokenDiscovery) GetTokens() map[string]*TokenInfo {
	td.mu.RLock()
	defer td.mu.RUnlock()
	return td.tokens
}

// GetByProvider returns tokens for a specific provider
func (td *TokenDiscovery) GetByProvider(provider ProviderType) []*TokenInfo {
	td.mu.RLock()
	defer td.mu.RUnlock()
	return td.providers[provider]
}

// GetStats returns discovery statistics
func (td *TokenDiscovery) GetStats() map[string]interface{} {
	td.mu.RLock()
	defer td.mu.RUnlock()

	stats := map[string]interface{}{
		"total_tokens": len(td.tokens),
		"providers":    make(map[string]int),
	}

	providerCounts := make(map[string]int)
	for provider, tokens := range td.providers {
		providerCounts[string(provider)] = len(tokens)
	}
	stats["providers"] = providerCounts

	return stats
}

// StartPeriodicScan starts a background goroutine that periodically scans for tokens
func (td *TokenDiscovery) StartPeriodicScan(stop <-chan struct{}) {
	go func() {
		// Initial scan
		td.Scan()

		ticker := time.NewTicker(td.config.ScanInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				td.Scan()
			}
		}
	}()
}

// SortTokensByProvider returns tokens sorted by provider then ID
func (td *TokenDiscovery) SortTokensByProvider() []*TokenInfo {
	td.mu.RLock()
	defer td.mu.RUnlock()

	var sorted []*TokenInfo
	for _, token := range td.tokens {
		sorted = append(sorted, token)
	}

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Provider != sorted[j].Provider {
			return sorted[i].Provider < sorted[j].Provider
		}
		return sorted[i].ID < sorted[j].ID
	})

	return sorted
}

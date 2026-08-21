package credential

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// GatewayAuthIntegration integrates token discovery with the gateway's auth system
type GatewayAuthIntegration struct {
	discovery *TokenDiscovery
	manager   *CredentialManager
	registry  *ProviderRegistry
	mu        sync.RWMutex
	started   bool
	stopCh    chan struct{}
}

// NewGatewayAuthIntegration creates a new integration instance
func NewGatewayAuthIntegration() *GatewayAuthIntegration {
	config := DefaultDiscoveryConfig()
	discovery := NewTokenDiscovery(config)

	managerConfig := DefaultManagerConfig()
	manager := NewCredentialManager(discovery, managerConfig)

	registry := NewProviderRegistry("data")
	registry.LoadProviders()

	return &GatewayAuthIntegration{
		discovery: discovery,
		manager:   manager,
		registry:  registry,
		stopCh:    make(chan struct{}),
	}
}

// Initialize initializes the token discovery and loads all tokens
func (g *GatewayAuthIntegration) Initialize() error {
	fmt.Println("🔐 Initializing Freebuff Gateway Token System...")
	fmt.Println()

	// Scan for tokens
	if err := g.discovery.Scan(); err != nil {
		return fmt.Errorf("token discovery: %w", err)
	}

	tokens := g.discovery.GetTokens()
	fmt.Printf("Found %d encrypted tokens\n", len(tokens))

	// Group by provider
	providerCounts := make(map[ProviderType]int)
	for _, token := range tokens {
		providerCounts[token.Provider]++
	}

	fmt.Println("\nTokens by provider:")
	for provider, count := range providerCounts {
		fmt.Printf("  %s: %d tokens\n", provider, count)
	}

	// Load tokens into registry
	for _, token := range tokens {
		decrypted, err := g.manager.GetToken(token.ID)
		if err != nil {
			fmt.Printf("  ⚠️  Failed to decrypt %s: %v\n", token.ID, err)
			continue
		}

		if decrypted.IsValid {
			keys := g.registry.GetAPIKeys(token.Provider)
			keys = append(keys, decrypted)
			g.registry.SetAPIKeys(token.Provider, keys)
		}
	}

	// Print provider status
	fmt.Println("\nProvider status:")
	statuses := g.registry.GetStatus()
	for _, status := range statuses {
		if status.TotalKeys > 0 {
			enabled := "✓"
			if !status.Enabled {
				enabled = "✗"
			}
			fmt.Printf("  %s %s: %d/%d valid keys\n", enabled, status.Name, status.ValidKeys, status.TotalKeys)
		}
	}

	g.started = true
	return nil
}

// Start starts the periodic token refresh
func (g *GatewayAuthIntegration) Start() {
	if g.started {
		return
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-g.stopCh:
				return
			case <-ticker.C:
				g.discovery.Scan()
			}
		}
	}()

	g.started = true
}

// Stop stops the periodic token refresh
func (g *GatewayAuthIntegration) Stop() {
	close(g.stopCh)
	g.started = false
}

// GetAPIKeyForProvider returns a valid API key for the specified provider
func (g *GatewayAuthIntegration) GetAPIKeyForProvider(provider ProviderType) (string, error) {
	token, err := g.registry.GetNextAPIKey(provider)
	if err != nil {
		return "", err
	}

	if !token.IsValid {
		return "", fmt.Errorf("no valid API key for provider %s", provider)
	}

	return token.Value, nil
}

// GetProviderConfig returns configuration for a provider
func (g *GatewayAuthIntegration) GetProviderConfig(provider ProviderType) (*ProviderConfig, bool) {
	return g.registry.GetProvider(provider)
}

// SetProviderEnabled enables or disables a provider
func (g *GatewayAuthIntegration) SetProviderEnabled(provider ProviderType, enabled bool) {
	config, ok := g.registry.GetProvider(provider)
	if ok {
		config.Enabled = enabled
	}
}

// GetStatus returns the current status of all providers
func (g *GatewayAuthIntegration) GetStatus() []ProviderStatus {
	return g.registry.GetStatus()
}

// InjectEnvironmentVariables injects API keys as environment variables
func (g *GatewayAuthIntegration) InjectEnvironmentVariables() error {
	providers := []ProviderType{
		ProviderOpenAI, ProviderGemini, ProviderNvidia,
		ProviderGitHub, ProviderVercel,
	}

	for _, provider := range providers {
		token, err := g.registry.GetNextAPIKey(provider)
		if err != nil {
			continue
		}

		if !token.IsValid {
			continue
		}

		config, ok := g.registry.GetProvider(provider)
		if !ok || config.APIKeyEnvVar == "" {
			continue
		}

		os.Setenv(config.APIKeyEnvVar, token.Value)
	}

	return nil
}

// AuthMiddleware returns an HTTP middleware that validates API keys
func (g *GatewayAuthIntegration) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for API key in Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, `{"error": {"message": "Missing API key", "type": "auth_error"}}`, http.StatusUnauthorized)
			return
		}

		// Extract Bearer token
		if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
			http.Error(w, `{"error": {"message": "Invalid authorization format", "type": "auth_error"}}`, http.StatusUnauthorized)
			return
		}

		apiKey := authHeader[7:]

		// Validate against known API keys
		valid := g.validateAPIKey(apiKey)
		if !valid {
			http.Error(w, `{"error": {"message": "Invalid API key", "type": "auth_error"}}`, http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// validateAPIKey checks if an API key is valid
func (g *GatewayAuthIntegration) validateAPIKey(apiKey string) bool {
	// Check against all provider keys
	providers := []ProviderType{
		ProviderOpenAI, ProviderGemini, ProviderNvidia,
		ProviderGitHub, ProviderVercel,
	}

	for _, provider := range providers {
		keys := g.registry.GetAPIKeys(provider)
		for _, key := range keys {
			if key.IsValid && key.Value == apiKey {
				return true
			}
		}
	}

	return false
}

// GetModelsForProvider returns available models for a provider
func (g *GatewayAuthIntegration) GetModelsForProvider(provider ProviderType) []string {
	config, ok := g.registry.GetProvider(provider)
	if !ok {
		return nil
	}
	return config.Models
}

// GetAllModels returns all available models across all providers
func (g *GatewayAuthIntegration) GetAllModels() map[ProviderType][]string {
	result := make(map[ProviderType][]string)
	for providerType, config := range g.registry.GetAllProviders() {
		if config.Enabled && len(config.Models) > 0 {
			result[providerType] = config.Models
		}
	}
	return result
}

package credential

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ProviderConfig represents configuration for an AI provider
type ProviderConfig struct {
	Name            string       `json:"name"`
	Type            ProviderType `json:"type"`
	BaseURL         string       `json:"base_url"`
	APIKeyEnvVar    string       `json:"api_key_env_var,omitempty"`
	Models          []string     `json:"models,omitempty"`
	MaxTokens       int          `json:"max_tokens,omitempty"`
	RateLimitRPM    int          `json:"rate_limit_rpm,omitempty"`
	Priority        int          `json:"priority,omitempty"`
	Enabled         bool         `json:"enabled"`
}

// ProviderRegistry manages provider configurations and API keys
type ProviderRegistry struct {
	configDir string
	mu        sync.RWMutex
	providers map[ProviderType]*ProviderConfig
	keys      map[ProviderType][]*DecryptedToken
}

// NewProviderRegistry creates a new provider registry
func NewProviderRegistry(configDir string) *ProviderRegistry {
	return &ProviderRegistry{
		configDir: configDir,
		providers: make(map[ProviderType]*ProviderConfig),
		keys:      make(map[ProviderType][]*DecryptedToken),
	}
}

// DefaultProviders returns the default provider configurations
func DefaultProviders() map[ProviderType]*ProviderConfig {
	return map[ProviderType]*ProviderConfig{
		ProviderOpenAI: {
			Name:         "OpenAI",
			Type:         ProviderOpenAI,
			BaseURL:      "https://api.openai.com/v1",
			APIKeyEnvVar: "OPENAI_API_KEY",
			Models:       []string{"gpt-4", "gpt-4-turbo", "gpt-3.5-turbo", "o1", "o1-mini"},
			MaxTokens:    128000,
			RateLimitRPM: 500,
			Priority:     1,
			Enabled:      true,
		},
		ProviderGemini: {
			Name:         "Google Gemini",
			Type:         ProviderGemini,
			BaseURL:      "https://generativelanguage.googleapis.com/v1beta",
			APIKeyEnvVar: "GEMINI_API_KEY",
			Models:       []string{"gemini-2.0-flash", "gemini-2.5-pro", "gemini-1.5-pro"},
			MaxTokens:    1000000,
			RateLimitRPM: 60,
			Priority:     2,
			Enabled:      true,
		},
		ProviderNvidia: {
			Name:         "NVIDIA Build",
			Type:         ProviderNvidia,
			BaseURL:      "https://integrate.api.nvidia.com/v1",
			APIKeyEnvVar: "NVIDIA_API_KEY",
			Models:       []string{"llama-3.1-405b-instruct", "mixtral-8x22b-instruct-v0.1"},
			MaxTokens:    32768,
			RateLimitRPM: 60,
			Priority:     3,
			Enabled:      true,
		},
		ProviderGitHub: {
			Name:         "GitHub",
			Type:         ProviderGitHub,
			BaseURL:      "https://api.github.com",
			APIKeyEnvVar: "GITHUB_TOKEN",
			Models:       []string{},
			MaxTokens:    0,
			RateLimitRPM: 60,
			Priority:     4,
			Enabled:      true,
		},
		ProviderVercel: {
			Name:         "Vercel",
			Type:         ProviderVercel,
			BaseURL:      "https://api.vercel.com",
			APIKeyEnvVar: "VERCEL_TOKEN",
			Models:       []string{},
			MaxTokens:    0,
			RateLimitRPM: 60,
			Priority:     5,
			Enabled:      true,
		},
		ProviderTool: {
			Name:         "AI Tools",
			Type:         ProviderTool,
			BaseURL:      "",
			APIKeyEnvVar: "",
			Models:       []string{"browser-use", "deepseek", "kimi"},
			MaxTokens:    0,
			RateLimitRPM: 0,
			Priority:     6,
			Enabled:      true,
		},
		ProviderDatabase: {
			Name:         "Database",
			Type:         ProviderDatabase,
			BaseURL:      "",
			APIKeyEnvVar: "",
			Models:       []string{},
			MaxTokens:    0,
			RateLimitRPM: 0,
			Priority:     7,
			Enabled:      true,
		},
	}
}

// LoadProviders loads provider configurations from config directory
func (pr *ProviderRegistry) LoadProviders() error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Load default providers
	for providerType, config := range DefaultProviders() {
		pr.providers[providerType] = config
	}

	// Try to load custom config if exists
	configFile := filepath.Join(pr.configDir, "providers.json")
	if _, err := os.Stat(configFile); err == nil {
		data, err := os.ReadFile(configFile)
		if err != nil {
			return fmt.Errorf("read providers config: %w", err)
		}

		var customProviders map[ProviderType]*ProviderConfig
		if err := json.Unmarshal(data, &customProviders); err != nil {
			return fmt.Errorf("parse providers config: %w", err)
		}

		// Merge custom configs
		for providerType, config := range customProviders {
			pr.providers[providerType] = config
		}
	}

	return nil
}

// SaveProviders saves provider configurations to config directory
func (pr *ProviderRegistry) SaveProviders() error {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	configFile := filepath.Join(pr.configDir, "providers.json")
	data, err := json.MarshalIndent(pr.providers, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal providers: %w", err)
	}

	if err := os.MkdirAll(pr.configDir, 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	if err := os.WriteFile(configFile, data, 0600); err != nil {
		return fmt.Errorf("write providers config: %w", err)
	}

	return nil
}

// GetProvider returns configuration for a provider
func (pr *ProviderRegistry) GetProvider(providerType ProviderType) (*ProviderConfig, bool) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	config, ok := pr.providers[providerType]
	return config, ok
}

// GetAllProviders returns all provider configurations
func (pr *ProviderRegistry) GetAllProviders() map[ProviderType]*ProviderConfig {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return pr.providers
}

// SetAPIKeys sets decrypted API keys for a provider
func (pr *ProviderRegistry) SetAPIKeys(providerType ProviderType, keys []*DecryptedToken) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.keys[providerType] = keys
}

// GetAPIKeys returns API keys for a provider
func (pr *ProviderRegistry) GetAPIKeys(providerType ProviderType) []*DecryptedToken {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	return pr.keys[providerType]
}

// GetNextAPIKey returns the next API key for a provider (round-robin)
func (pr *ProviderRegistry) GetNextAPIKey(providerType ProviderType) (*DecryptedToken, error) {
	keys := pr.GetAPIKeys(providerType)
	if len(keys) == 0 {
		return nil, fmt.Errorf("no API keys for provider %s", providerType)
	}

	// Simple round-robin (could be improved with weighted selection)
	now := time.Now().UnixNano()
	index := now % int64(len(keys))
	return keys[index], nil
}

// ValidateProvider checks if a provider has valid credentials
func (pr *ProviderRegistry) ValidateProvider(providerType ProviderType) (bool, string) {
	config, ok := pr.GetProvider(providerType)
	if !ok {
		return false, "provider not configured"
	}

	if !config.Enabled {
		return false, "provider disabled"
	}

	keys := pr.GetAPIKeys(providerType)
	if len(keys) == 0 {
		return false, "no API keys"
	}

	// Check if at least one key is valid
	validCount := 0
	for _, key := range keys {
		if key.IsValid {
			validCount++
		}
	}

	if validCount == 0 {
		return false, "no valid API keys"
	}

	return true, fmt.Sprintf("%d valid keys", validCount)
}

// ProviderStatus represents the status of a provider
type ProviderStatus struct {
	Provider    ProviderType `json:"provider"`
	Name        string       `json:"name"`
	Enabled     bool         `json:"enabled"`
	Configured  bool         `json:"configured"`
	ValidKeys   int          `json:"valid_keys"`
	TotalKeys   int          `json:"total_keys"`
	Status      string       `json:"status"`
	LastChecked time.Time    `json:"last_checked"`
}

// GetStatus returns status for all providers
func (pr *ProviderRegistry) GetStatus() []ProviderStatus {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	var statuses []ProviderStatus

	for providerType, config := range pr.providers {
		keys := pr.keys[providerType]
		validKeys := 0
		for _, key := range keys {
			if key.IsValid {
				validKeys++
			}
		}

		status := "ready"
		if !config.Enabled {
			status = "disabled"
		} else if len(keys) == 0 {
			status = "no keys"
		} else if validKeys == 0 {
			status = "no valid keys"
		}

		statuses = append(statuses, ProviderStatus{
			Provider:    providerType,
			Name:        config.Name,
			Enabled:     config.Enabled,
			Configured:  len(config.BaseURL) > 0,
			ValidKeys:   validKeys,
			TotalKeys:   len(keys),
			Status:      status,
			LastChecked: time.Now(),
		})
	}

	return statuses
}

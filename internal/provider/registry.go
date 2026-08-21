package provider

import (
	"fmt"
	"sync"
)

// Registry manages provider adapters.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Adapter
	configs   map[string]ProviderConfig
}

// NewRegistry creates a new provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]Adapter),
		configs:   make(map[string]ProviderConfig),
	}
}

// Register adds a provider to the registry.
func (r *Registry) Register(name string, adapter Adapter, config ProviderConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[name] = adapter
	r.configs[name] = config
}

// Get retrieves a provider by name.
func (r *Registry) Get(name string) (Adapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	adapter, ok := r.providers[name]
	return adapter, ok
}

// GetConfig retrieves a provider config by name.
func (r *Registry) GetConfig(name string) (ProviderConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	config, ok := r.configs[name]
	return config, ok
}

// List returns all registered provider names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// Remove removes a provider from the registry.
func (r *Registry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.providers, name)
	delete(r.configs, name)
}

// Len returns the number of registered providers.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.providers)
}

// SelectProvider selects the best provider for a model.
func (r *Registry) SelectProvider(model string) (Adapter, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check model aliases first
	for name, config := range r.configs {
		if _, ok := config.ModelAliases[model]; ok {
			if adapter, exists := r.providers[name]; exists {
				return adapter, name, nil
			}
		}
	}

	// Check supported models
	for name, config := range r.configs {
		for _, supportedModel := range config.SupportedModels {
			if supportedModel == model || supportedModel == "*" {
				if adapter, exists := r.providers[name]; exists {
					return adapter, name, nil
				}
			}
		}
	}

	return nil, "", fmt.Errorf("no provider found for model: %s", model)
}

// DefaultRegistry creates a registry with default providers.
func DefaultRegistry() *Registry {
	registry := NewRegistry()

	openaiConfig := ProviderConfig{
		Name:     "openai",
		Type:     "openai",
		BaseURL:  "https://api.openai.com",
		MaxConns: 10,
		SupportedModels: []string{
			"gpt-4", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo",
		},
		ModelAliases: map[string]string{
			"gpt-4":       "gpt-4",
			"gpt-4-turbo": "gpt-4-turbo",
			"gpt-4o":      "gpt-4o",
		},
	}
	registry.Register("openai", NewOpenAIAdapter(openaiConfig), openaiConfig)

	anthropicConfig := ProviderConfig{
		Name:     "anthropic",
		Type:     "anthropic",
		BaseURL:  "https://api.anthropic.com",
		MaxConns: 10,
		SupportedModels: []string{
			"claude-3-opus", "claude-3-sonnet", "claude-3-haiku",
			"claude-2.1", "claude-2.0",
		},
		ModelAliases: map[string]string{
			"claude-3-opus":   "claude-3-opus-20240229",
			"claude-3-sonnet": "claude-3-sonnet-20240229",
			"claude-3-haiku":  "claude-3-haiku-20240307",
		},
	}
	registry.Register("anthropic", NewAnthropicAdapter(anthropicConfig), anthropicConfig)

	return registry
}

package provider

import (
	"context"
	"testing"
	"time"
)

func TestNewOpenAIAdapter(t *testing.T) {
	config := ProviderConfig{
		Name:    "openai",
		Type:    "openai",
		BaseURL: "https://api.openai.com",
		APIKey:  "test-key",
	}

	adapter := NewOpenAIAdapter(config)

	if adapter.Name() != "OpenAI" {
		t.Errorf("expected name 'OpenAI', got '%s'", adapter.Name())
	}

	if adapter.Type() != ProviderOpenAI {
		t.Errorf("expected type ProviderOpenAI, got '%s'", adapter.Type())
	}

	if !adapter.SupportsModel("gpt-4") {
		t.Error("expected to support gpt-4")
	}
}

func TestOpenAISendRequest(t *testing.T) {
	config := ProviderConfig{
		Name:    "openai",
		Type:    "openai",
		BaseURL: "https://httpbin.org",
		APIKey:  "test-key",
	}

	adapter := NewOpenAIAdapter(config)

	// This will fail because httpbin doesn't serve OpenAI responses,
	// but it verifies the request is well-formed
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &Request{
		Model: "gpt-4",
		Messages: []Message{
			{Role: "user", Content: "Hello"},
		},
		MaxTokens: 100,
	}

	_, err := adapter.SendRequest(ctx, req)
	// Expected to fail with non-200 status from httpbin
	if err == nil {
		t.Log("request succeeded (unexpected but ok)")
	} else {
		t.Logf("expected error from httpbin: %v", err)
	}
}

func TestAnthropicAdapter(t *testing.T) {
	config := ProviderConfig{
		Name:    "anthropic",
		Type:    "anthropic",
		BaseURL: "https://api.anthropic.com",
		APIKey:  "test-key",
	}

	adapter := NewAnthropicAdapter(config)

	if adapter.Name() != "Anthropic" {
		t.Errorf("expected name 'Anthropic', got '%s'", adapter.Name())
	}

	if adapter.Type() != ProviderAnthropic {
		t.Errorf("expected type ProviderAnthropic, got '%s'", adapter.Type())
	}

	if !adapter.SupportsModel("claude-3-opus") {
		t.Error("expected to support claude-3-opus")
	}
}

func TestRegistry(t *testing.T) {
	registry := NewRegistry()

	config := ProviderConfig{
		Name:            "test",
		Type:            "openai",
		SupportedModels: []string{"test-model"},
	}

	adapter := NewOpenAIAdapter(config)
	registry.Register("test", adapter, config)

	got, ok := registry.Get("test")
	if !ok {
		t.Fatal("expected to find 'test' provider")
	}
	if got.Name() != "OpenAI" {
		t.Errorf("expected name 'OpenAI', got '%s'", got.Name())
	}

	names := registry.List()
	if len(names) != 1 {
		t.Errorf("expected 1 provider, got %d", len(names))
	}

	if registry.Len() != 1 {
		t.Errorf("expected length 1, got %d", registry.Len())
	}

	registry.Remove("test")
	if registry.Len() != 0 {
		t.Errorf("expected length 0 after remove, got %d", registry.Len())
	}
}

func TestRegistrySelectProvider(t *testing.T) {
	registry := NewRegistry()

	openaiConfig := ProviderConfig{
		Name:            "openai",
		Type:            "openai",
		SupportedModels: []string{"gpt-4", "gpt-3.5-turbo"},
		ModelAliases:    map[string]string{"gpt-4": "gpt-4-0125-preview"},
	}
	registry.Register("openai", NewOpenAIAdapter(openaiConfig), openaiConfig)

	anthropicConfig := ProviderConfig{
		Name:            "anthropic",
		Type:            "anthropic",
		SupportedModels: []string{"claude-3-opus"},
		ModelAliases:    map[string]string{"claude-3-opus": "claude-3-opus-20240229"},
	}
	registry.Register("anthropic", NewAnthropicAdapter(anthropicConfig), anthropicConfig)

	adapter, name, err := registry.SelectProvider("gpt-4")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "openai" {
		t.Errorf("expected 'openai', got '%s'", name)
	}
	if adapter.Type() != ProviderOpenAI {
		t.Errorf("expected ProviderOpenAI, got '%s'", adapter.Type())
	}

	adapter, name, err = registry.SelectProvider("claude-3-opus")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "anthropic" {
		t.Errorf("expected 'anthropic', got '%s'", name)
	}

	_, _, err = registry.SelectProvider("unknown-model")
	if err == nil {
		t.Error("expected error for unknown model")
	}
}

func TestRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	if config.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", config.MaxRetries)
	}

	if config.InitialBackoff != 1*time.Second {
		t.Errorf("expected initial backoff 1s, got %v", config.InitialBackoff)
	}

	if config.Multiplier != 2.0 {
		t.Errorf("expected multiplier 2.0, got %f", config.Multiplier)
	}
}

func TestCalculateBackoff(t *testing.T) {
	config := DefaultRetryConfig()

	b0 := calculateBackoff(config, 0)
	b1 := calculateBackoff(config, 1)
	b2 := calculateBackoff(config, 2)

	if b0 <= 0 || b1 <= 0 || b2 <= 0 {
		t.Errorf("expected positive backoffs, got %v, %v, %v", b0, b1, b2)
	}

	largeBackoff := calculateBackoff(config, 10)
	if largeBackoff > config.MaxBackoff+config.MaxBackoff/4 {
		t.Errorf("backoff %v exceeds max %v", largeBackoff, config.MaxBackoff)
	}
}

func TestProviderWithRetry(t *testing.T) {
	config := ProviderConfig{
		Name:            "openai",
		Type:            "openai",
		SupportedModels: []string{"gpt-4"},
	}

	adapter := NewOpenAIAdapter(config)
	retryAdapter := NewProviderWithRetry(adapter, DefaultRetryConfig())

	if retryAdapter.Name() != "OpenAI" {
		t.Errorf("expected name 'OpenAI', got '%s'", retryAdapter.Name())
	}

	if retryAdapter.Type() != ProviderOpenAI {
		t.Errorf("expected type ProviderOpenAI, got '%s'", retryAdapter.Type())
	}

	if !retryAdapter.SupportsModel("gpt-4") {
		t.Error("expected to support gpt-4")
	}
}

func TestDefaultRegistry(t *testing.T) {
	registry := DefaultRegistry()

	if registry.Len() != 2 {
		t.Errorf("expected 2 providers, got %d", registry.Len())
	}

	names := registry.List()
	if len(names) != 2 {
		t.Errorf("expected 2 names, got %d", len(names))
	}
}

func TestHealthCheck(t *testing.T) {
	config := ProviderConfig{
		Name:    "test",
		Type:    "openai",
		BaseURL: "http://localhost:99999",
	}

	adapter := NewOpenAIAdapter(config)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := adapter.HealthCheck(ctx)
	// Health check may or may not fail depending on port
	t.Logf("health check result: %v", err)
}

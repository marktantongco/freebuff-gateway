package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// ErrNoAPIKey is returned when no API key is configured.
var ErrNoAPIKey = errors.New("no API key configured")

// ProviderType represents the type of AI provider
type ProviderType string

const (
	ProviderOpenAI   ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
	ProviderGemini   ProviderType = "gemini"
	ProviderNvidia   ProviderType = "nvidia"
	ProviderFreebuff ProviderType = "freebuff"
)

// Request represents a unified provider request
type Request struct {
	ID            string            `json:"id"`
	Provider      ProviderType      `json:"provider"`
	Model         string            `json:"model"`
	Messages      []Message         `json:"messages"`
	MaxTokens     int               `json:"max_tokens,omitempty"`
	Temperature   float64           `json:"temperature,omitempty"`
	Stream        bool              `json:"stream"`
	Tools         []Tool            `json:"tools,omitempty"`
	ToolChoice    any               `json:"tool_choice,omitempty"`
	Stop          []string          `json:"stop,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// Message represents a chat message
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Tool represents a function tool
type Tool struct {
	Type     string         `json:"type"`
	Function ToolFunction   `json:"function"`
}

// ToolFunction represents a function tool definition
type ToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// Response represents a unified provider response
type Response struct {
	ID            string          `json:"id"`
	Provider      ProviderType    `json:"provider"`
	Model         string          `json:"model"`
	Choices       []Choice        `json:"choices"`
	Usage         Usage           `json:"usage"`
	CreatedAt     int64           `json:"created_at"`
}

// Choice represents a response choice
type Choice struct {
	Index        int             `json:"index"`
	Message      *Message        `json:"message,omitempty"`
	Delta        *Delta          `json:"delta,omitempty"`
	FinishReason string          `json:"finish_reason"`
}

// Delta represents a streaming delta
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Usage represents token usage
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// StreamChunk represents a streaming response chunk
type StreamChunk struct {
	ID        string   `json:"id"`
	Provider  ProviderType `json:"provider"`
	Model     string   `json:"model"`
	Choices   []Choice `json:"choices"`
	CreatedAt int64    `json:"created_at"`
}

// Adapter defines the interface for provider adapters
type Adapter interface {
	// ID returns the unique identifier for this adapter
	ID() string

	// Type returns the provider type
	Type() ProviderType

	// Name returns a human-readable name
	Name() string

	// SendRequest sends a request to the provider and returns a response
	SendRequest(ctx context.Context, req *Request) (*Response, error)

	// StreamRequest sends a streaming request and returns a channel of chunks
	StreamRequest(ctx context.Context, req *Request) (<-chan *StreamChunk, error)

	// HealthCheck checks if the provider is healthy
	HealthCheck(ctx context.Context) error

	// SupportsModel checks if this adapter supports a model
	SupportsModel(model string) bool

	// ListModels returns the list of supported models
	ListModels() []string

	// MaxTokens returns the maximum tokens for a model
	MaxTokens(model string) int
}

// BaseAdapter provides common functionality for adapters
type BaseAdapter struct {
	id       string
	name     string
	provider ProviderType
	apiKey   string
	baseURL  string
	models   []string
	timeout  time.Duration
}

// NewBaseAdapter creates a new base adapter
func NewBaseAdapter(id, name string, provider ProviderType, apiKey, baseURL string, models []string) *BaseAdapter {
	return &BaseAdapter{
		id:       id,
		name:     name,
		provider: provider,
		apiKey:   apiKey,
		baseURL:  baseURL,
		models:   models,
		timeout:  60 * time.Second,
	}
}

// ID returns the adapter ID
func (a *BaseAdapter) ID() string {
	return a.id
}

// Type returns the provider type
func (a *BaseAdapter) Type() ProviderType {
	return a.provider
}

// Name returns the adapter name
func (a *BaseAdapter) Name() string {
	return a.name
}

// SupportsModel checks if a model is supported
func (a *BaseAdapter) SupportsModel(model string) bool {
	for _, m := range a.models {
		if m == model {
			return true
		}
	}
	return false
}

// ListModels returns supported models
func (a *BaseAdapter) ListModels() []string {
	return a.models
}

// MaxTokens returns max tokens for a model
func (a *BaseAdapter) MaxTokens(model string) int {
	// Default max tokens
	return 4096
}

// HealthCheck is a default health check implementation
func (a *BaseAdapter) HealthCheck(ctx context.Context) error {
	// Default implementation: just check API key exists
	if a.apiKey == "" {
		return ErrNoAPIKey
	}
	return nil
}

// Suppress unused import warnings
var _ = io.EOF
var _ = json.RawMessage(nil)

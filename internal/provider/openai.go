package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAIAdapter implements the Adapter interface for OpenAI.
type OpenAIAdapter struct {
	*BaseAdapter
	config     ProviderConfig
	httpClient *http.Client
}

// ProviderConfig holds configuration for a provider adapter.
type ProviderConfig struct {
	Name            string            `json:"name"`
	Type            string            `json:"type"`
	BaseURL         string            `json:"base_url"`
	APIKey          string            `json:"-"`
	MaxConns        int               `json:"max_conns"`
	Timeout         time.Duration     `json:"timeout"`
	SupportedModels []string          `json:"supported_models"`
	ModelAliases    map[string]string `json:"model_aliases"`
}

// NewOpenAIAdapter creates a new OpenAI adapter.
func NewOpenAIAdapter(config ProviderConfig) *OpenAIAdapter {
	models := config.SupportedModels
	if len(models) == 0 {
		models = []string{
			"gpt-4", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo",
		}
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	return &OpenAIAdapter{
		BaseAdapter: NewBaseAdapter(
			"openai", "OpenAI", ProviderOpenAI,
			config.APIKey, config.BaseURL, models,
		),
		config: config,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// SendRequest sends a request to OpenAI and returns a response.
func (a *OpenAIAdapter) SendRequest(ctx context.Context, req *Request) (*Response, error) {
	body, path, err := a.translateRequest(req)
	if err != nil {
		return nil, err
	}

	url := a.baseURL + path
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openai API error %d: %s", resp.StatusCode, string(respBody))
	}

	return a.translateResponse(respBody, req.Model)
}

// StreamRequest sends a streaming request and returns a channel of chunks.
func (a *OpenAIAdapter) StreamRequest(ctx context.Context, req *Request) (<-chan *StreamChunk, error) {
	body, path, err := a.translateRequest(req)
	if err != nil {
		return nil, err
	}

	// Add stream parameter
	var openAIReq map[string]interface{}
	if err := json.Unmarshal(body, &openAIReq); err != nil {
		return nil, fmt.Errorf("unmarshal for stream: %w", err)
	}
	openAIReq["stream"] = true
	body, _ = json.Marshal(openAIReq)

	url := a.baseURL + path
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("openai API error %d: %s", resp.StatusCode, string(respBody))
	}

	chunks := make(chan *StreamChunk, 100)

	go func() {
		defer close(chunks)
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}

			data := strings.TrimPrefix(line, "data: ")
			if data == "[DONE]" {
				return
			}

			var chunk StreamChunk
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue
			}
			chunk.Provider = ProviderOpenAI
			chunk.Model = req.Model

			select {
			case chunks <- &chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	return chunks, nil
}

// translateRequest converts a Request to OpenAI format.
func (a *OpenAIAdapter) translateRequest(req *Request) ([]byte, string, error) {
	model := req.Model
	if alias, ok := a.config.ModelAliases[model]; ok {
		model = alias
	}

	var messages []map[string]interface{}
	for _, msg := range req.Messages {
		messages = append(messages, map[string]interface{}{
			"role":    msg.Role,
			"content": msg.Content,
		})
	}

	openAIReq := map[string]interface{}{
		"model":    model,
		"messages": messages,
	}

	if req.MaxTokens > 0 {
		openAIReq["max_tokens"] = req.MaxTokens
	}

	if req.Temperature > 0 {
		openAIReq["temperature"] = req.Temperature
	}

	if req.Stop != nil {
		openAIReq["stop"] = req.Stop
	}

	body, err := json.Marshal(openAIReq)
	if err != nil {
		return nil, "", fmt.Errorf("marshal request: %w", err)
	}

	return body, "/v1/chat/completions", nil
}

// translateResponse converts an OpenAI response to internal format.
func (a *OpenAIAdapter) translateResponse(data []byte, model string) (*Response, error) {
	var openAIResp struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			Index        int      `json:"index"`
			Message      *Message `json:"message,omitempty"`
			Delta        *Delta   `json:"delta,omitempty"`
			FinishReason string   `json:"finish_reason"`
		} `json:"choices"`
		Usage Usage `json:"usage"`
	}

	if err := json.Unmarshal(data, &openAIResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	resp := &Response{
		ID:        openAIResp.ID,
		Provider:  ProviderOpenAI,
		Model:     openAIResp.Model,
		CreatedAt: openAIResp.Created,
		Usage:     openAIResp.Usage,
	}

	for _, c := range openAIResp.Choices {
		choice := Choice{
			Index:        c.Index,
			FinishReason: c.FinishReason,
		}
		if c.Message != nil {
			choice.Message = &Message{
				Role:    c.Message.Role,
				Content: c.Message.Content,
			}
		}
		if c.Delta != nil {
			choice.Delta = &Delta{
				Role:    c.Delta.Role,
				Content: c.Delta.Content,
			}
		}
		resp.Choices = append(resp.Choices, choice)
	}

	return resp, nil
}

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

// AnthropicAdapter implements the Adapter interface for Anthropic.
type AnthropicAdapter struct {
	*BaseAdapter
	config     ProviderConfig
	httpClient *http.Client
}

// NewAnthropicAdapter creates a new Anthropic adapter.
func NewAnthropicAdapter(config ProviderConfig) *AnthropicAdapter {
	models := config.SupportedModels
	if len(models) == 0 {
		models = []string{
			"claude-3-opus", "claude-3-sonnet", "claude-3-haiku",
			"claude-2.1", "claude-2.0",
		}
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}

	return &AnthropicAdapter{
		BaseAdapter: NewBaseAdapter(
			"anthropic", "Anthropic", ProviderAnthropic,
			config.APIKey, config.BaseURL, models,
		),
		config: config,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// SendRequest sends a request to Anthropic and returns a response.
func (a *AnthropicAdapter) SendRequest(ctx context.Context, req *Request) (*Response, error) {
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
	httpReq.Header.Set("anthropic-version", "2023-06-01")
	if a.apiKey != "" {
		httpReq.Header.Set("x-api-key", a.apiKey)
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
		return nil, fmt.Errorf("anthropic API error %d: %s", resp.StatusCode, string(respBody))
	}

	return a.translateResponse(respBody, req.Model)
}

// StreamRequest sends a streaming request and returns a channel of chunks.
func (a *AnthropicAdapter) StreamRequest(ctx context.Context, req *Request) (<-chan *StreamChunk, error) {
	body, path, err := a.translateRequest(req)
	if err != nil {
		return nil, err
	}

	// Add stream parameter
	var anthropicReq map[string]interface{}
	if err := json.Unmarshal(body, &anthropicReq); err != nil {
		return nil, fmt.Errorf("unmarshal for stream: %w", err)
	}
	anthropicReq["stream"] = true
	body, _ = json.Marshal(anthropicReq)

	url := a.baseURL + path
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("anthropic-version", "2023-06-01")
	if a.apiKey != "" {
		httpReq.Header.Set("x-api-key", a.apiKey)
	}

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("anthropic API error %d: %s", resp.StatusCode, string(respBody))
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

			var event struct {
				Type  string `json:"type"`
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			}

			if err := json.Unmarshal([]byte(data), &event); err != nil {
				continue
			}

			if event.Type == "content_block_delta" && event.Delta.Text != "" {
				chunk := &StreamChunk{
					ID:       fmt.Sprintf("msg_%d", time.Now().UnixNano()),
					Provider: ProviderAnthropic,
					Model:    req.Model,
					Choices: []Choice{
						{
							Index: 0,
							Delta: &Delta{
								Role:    "assistant",
								Content: event.Delta.Text,
							},
						},
					},
					CreatedAt: time.Now().Unix(),
				}

				select {
				case chunks <- chunk:
				case <-ctx.Done():
					return
				}
			}

			if event.Type == "message_stop" {
				return
			}
		}
	}()

	return chunks, nil
}

// translateRequest converts a Request to Anthropic format.
func (a *AnthropicAdapter) translateRequest(req *Request) ([]byte, string, error) {
	model := req.Model
	if alias, ok := a.config.ModelAliases[model]; ok {
		model = alias
	}

	var systemPrompt string
	var messages []map[string]interface{}

	for _, msg := range req.Messages {
		if msg.Role == "system" {
			systemPrompt = msg.Content
			continue
		}
		messages = append(messages, map[string]interface{}{
			"role":    msg.Role,
			"content": msg.Content,
		})
	}

	anthropicReq := map[string]interface{}{
		"model":      model,
		"max_tokens": req.MaxTokens,
		"messages":   messages,
	}

	if systemPrompt != "" {
		anthropicReq["system"] = systemPrompt
	}

	if req.Temperature > 0 {
		anthropicReq["temperature"] = req.Temperature
	}

	if req.Stop != nil {
		anthropicReq["stop_sequences"] = req.Stop
	}

	body, err := json.Marshal(anthropicReq)
	if err != nil {
		return nil, "", fmt.Errorf("marshal request: %w", err)
	}

	return body, "/v1/messages", nil
}

// translateResponse converts an Anthropic response to internal format.
func (a *AnthropicAdapter) translateResponse(data []byte, model string) (*Response, error) {
	var anthropicResp struct {
		ID   string `json:"id"`
		Type string `json:"type"`
		Role string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(data, &anthropicResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	var text string
	for _, block := range anthropicResp.Content {
		if block.Type == "text" {
			text += block.Text
		}
	}

	var finishReason string
	switch anthropicResp.StopReason {
	case "end_turn":
		finishReason = "stop"
	case "max_tokens":
		finishReason = "length"
	case "stop_sequence":
		finishReason = "stop"
	default:
		finishReason = anthropicResp.StopReason
	}

	return &Response{
		ID:       anthropicResp.ID,
		Provider: ProviderAnthropic,
		Model:    anthropicResp.Model,
		Choices: []Choice{
			{
				Index: 0,
				Message: &Message{
					Role:    "assistant",
					Content: text,
				},
				FinishReason: finishReason,
			},
		},
		Usage: Usage{
			PromptTokens:     anthropicResp.Usage.InputTokens,
			CompletionTokens: anthropicResp.Usage.OutputTokens,
			TotalTokens:      anthropicResp.Usage.InputTokens + anthropicResp.Usage.OutputTokens,
		},
		CreatedAt: time.Now().Unix(),
	}, nil
}

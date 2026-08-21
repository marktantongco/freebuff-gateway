package freebuff

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/phasetiming"
)

type testStreamSink struct {
	bytes.Buffer
}

func (s *testStreamSink) Flush() {}

func TestAnthropicToOpenAIToolMessages(t *testing.T) {
	body := decodeJSONMap(t, `{
		"model": "minimax-m2.7",
		"stream": true,
		"messages": [
			{
				"role": "assistant",
				"content": [
					{"type": "text", "text": "I will inspect it."},
					{"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {"path": "a.txt"}}
				]
			},
			{
				"role": "user",
				"content": [
					{"type": "tool_result", "tool_use_id": "toolu_1", "content": "done"}
				]
			}
		],
		"tools": [
			{
				"name": "read_file",
				"description": "read a file",
				"input_schema": {
					"type": "object",
					"properties": {"path": {"type": "string", "default": "x"}},
					"additionalProperties": false
				}
			}
		]
	}`)

	out := anthropicToOpenAI(body, "minimax/minimax-m2.7")
	messages := out["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("messages len = %d, want 2: %+v", len(messages), messages)
	}
	assistant := messages[0].(map[string]any)
	if assistant["role"] != "assistant" || assistant["content"] != "I will inspect it." {
		t.Fatalf("unexpected assistant message: %+v", assistant)
	}
	toolCalls := assistant["tool_calls"].([]any)
	if len(toolCalls) != 1 {
		t.Fatalf("tool calls len = %d, want 1", len(toolCalls))
	}
	toolCall := toolCalls[0].(map[string]any)
	if toolCall["id"] != "toolu_1" || toolCall["type"] != "function" {
		t.Fatalf("unexpected tool call envelope: %+v", toolCall)
	}
	fn := toolCall["function"].(map[string]any)
	if fn["name"] != "read_file" || !strings.Contains(fn["arguments"].(string), `"path":"a.txt"`) {
		t.Fatalf("unexpected tool call function: %+v", fn)
	}

	toolResult := messages[1].(map[string]any)
	if toolResult["role"] != "tool" || toolResult["tool_call_id"] != "toolu_1" || toolResult["content"] != "done" {
		t.Fatalf("unexpected tool result message: %+v", toolResult)
	}

	tools := out["tools"].([]any)
	params := tools[0].(map[string]any)["function"].(map[string]any)["parameters"].(map[string]any)
	if _, ok := params["additionalProperties"]; ok {
		t.Fatalf("additionalProperties was not stripped: %+v", params)
	}
	pathSchema := params["properties"].(map[string]any)["path"].(map[string]any)
	if _, ok := pathSchema["default"]; ok {
		t.Fatalf("nested default was not stripped: %+v", pathSchema)
	}
}

func TestAnthropicToOpenAIPreservesThinkingAsReasoningContent(t *testing.T) {
	body := decodeJSONMap(t, `{
		"model": "deepseek/deepseek-v4-flash",
		"stream": true,
		"messages": [
			{
				"role": "assistant",
				"content": [
					{"type": "thinking", "thinking": "private chain", "signature": "sig"},
					{"type": "redacted_thinking", "data": "opaque"},
					{"type": "text", "text": "visible answer"}
				]
			}
		]
	}`)

	out := anthropicToOpenAI(body, "deepseek/deepseek-v4-flash")
	messages := out["messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("messages len = %d, want 1: %+v", len(messages), messages)
	}
	assistant := messages[0].(map[string]any)
	if assistant["content"] != "visible answer" {
		t.Fatalf("assistant content = %+v", assistant)
	}
	if assistant["reasoning_content"] != "private chain" {
		t.Fatalf("assistant reasoning_content = %+v", assistant)
	}
}

func TestAnthropicToOpenAIPreservesOptionsAndFlattensContextBlocks(t *testing.T) {
	body := decodeJSONMap(t, `{
		"model": "deepseek/deepseek-v4-pro",
		"stream": true,
		"max_tokens": 2048,
		"top_p": 0.7,
		"top_k": 40,
		"metadata": {"user_id": "u-1"},
		"stop_sequences": ["</done>"],
		"thinking": {"type": "enabled", "budget_tokens": 4096},
		"system": [
			{"type": "text", "text": "system context", "cache_control": {"type": "ephemeral"}}
		],
		"messages": [
			{
				"role": "user",
				"content": [
					{"type": "text", "text": "cached context", "cache_control": {"type": "ephemeral"}},
					{"type": "text", "text": "question"}
				]
			}
		],
		"tools": [
			{"name": "search", "description": "search", "input_schema": {"type": "object"}}
		],
		"tool_choice": {"type": "tool", "name": "search", "disable_parallel_tool_use": true}
	}`)

	out := anthropicToOpenAI(body, "deepseek/deepseek-v4-pro")
	if out["top_p"].(json.Number).String() != "0.7" || out["top_k"].(json.Number).String() != "40" {
		t.Fatalf("sampling params not preserved: %+v", out)
	}
	if out["reasoning_effort"] != "medium" {
		t.Fatalf("reasoning_effort = %+v", out["reasoning_effort"])
	}
	if _, ok := out["thinking"].(map[string]any); !ok {
		t.Fatalf("thinking not preserved: %+v", out["thinking"])
	}
	stops := out["stop"].([]string)
	if len(stops) != 1 || stops[0] != "</done>" {
		t.Fatalf("stop = %+v", stops)
	}
	if out["parallel_tool_calls"] != false {
		t.Fatalf("parallel_tool_calls = %+v", out["parallel_tool_calls"])
	}
	toolChoice := out["tool_choice"].(map[string]any)
	fn := toolChoice["function"].(map[string]any)
	if toolChoice["type"] != "function" || fn["name"] != "search" {
		t.Fatalf("tool_choice = %+v", toolChoice)
	}

	messages := out["messages"].([]any)
	system := messages[0].(map[string]any)
	if system["content"] != "system context" {
		t.Fatalf("system content = %+v", system["content"])
	}
	user := messages[1].(map[string]any)
	if user["content"] != "cached context\nquestion" {
		t.Fatalf("user content = %+v", user["content"])
	}
}

func TestAnthropicToOpenAIDropsAdaptiveThinkingAndFlattensContextBlocks(t *testing.T) {
	body := decodeJSONMap(t, `{
		"model": "deepseek/deepseek-v4-pro",
		"stream": true,
		"thinking": {"type": "adaptive"},
		"system": [
			{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.143", "cache_control": {"type": "ephemeral"}},
			{"type": "text", "text": "system prompt", "cache_control": {"type": "ephemeral"}}
		],
		"messages": [
			{
				"role": "user",
				"content": [
					{"type": "text", "text": "cached user context", "cache_control": {"type": "ephemeral"}},
					{"type": "text", "text": "actual question"}
				]
			}
		]
	}`)

	out := anthropicToOpenAI(body, "deepseek/deepseek-v4-pro")
	if _, ok := out["thinking"]; ok {
		t.Fatalf("adaptive thinking should not be forwarded: %+v", out["thinking"])
	}
	if out["reasoning_effort"] != "medium" {
		t.Fatalf("reasoning_effort = %+v, want medium", out["reasoning_effort"])
	}
	messages := out["messages"].([]any)
	system := messages[0].(map[string]any)
	if system["content"] != "x-anthropic-billing-header: cc_version=2.1.143\nsystem prompt" {
		t.Fatalf("system content = %+v", system["content"])
	}
	user := messages[1].(map[string]any)
	if user["content"] != "cached user context\nactual question" {
		t.Fatalf("user content = %+v", user["content"])
	}
}

func TestAnthropicStreamRewriterConvertsReasoningContent(t *testing.T) {
	requestBody := []byte(`{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"role":"assistant","content":null,"reasoning_content":""}}]}`,
		``,
		`data: {"choices":[{"delta":{"content":null,"reasoning_content":"think-1"}}]}`,
		``,
		`data: {"choices":[{"delta":{"content":null,"reasoning_content":"think-2"}}]}`,
		``,
		`data: {"choices":[{"delta":{"content":"answer"}}]}`,
		``,
		`data: {"choices":[{"finish_reason":"stop"}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	if _, err := translateOpenAIStreamToAnthropic(context.Background(), requestBody, upstream, sink); err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	got := sink.String()
	required := []string{
		`"content_block":{"signature":"","thinking":"","type":"thinking"},"index":0`,
		`"type":"thinking_delta"`,
		`"thinking":"think-1"`,
		`"thinking":"think-2"`,
		`"type":"signature_delta"`,
		`"content_block":{"text":"","type":"text"},"index":1`,
		`"type":"text_delta"`,
		`"text":"answer"`,
		`"stop_reason":"end_turn"`,
	}
	for _, want := range required {
		if !strings.Contains(got, want) {
			t.Fatalf("stream output missing %q:\n%s", want, got)
		}
	}
}

func TestAnthropicStreamRewriterMapsUsage(t *testing.T) {
	requestBody := []byte(`{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"answer"},"finish_reason":null}],"usage":null}`,
		``,
		`data: {"choices":[{"delta":{"content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":30}}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	tokens, err := translateOpenAIStreamToAnthropic(context.Background(), requestBody, upstream, sink)
	if err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	if !tokens.Known || tokens.InputTokens != 100 || tokens.OutputTokens != 20 {
		t.Fatalf("tokens = %+v, want 100/20 known", tokens)
	}
	got := sink.String()
	required := []string{
		`"input_tokens":70`,
		`"cache_read_input_tokens":30`,
		`"output_tokens":20`,
	}
	for _, want := range required {
		if !strings.Contains(got, want) {
			t.Fatalf("stream output missing %q:\n%s", want, got)
		}
	}
}

func TestOpenAIStreamCopyReturnsUsage(t *testing.T) {
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"answer"},"finish_reason":null}],"usage":null}`,
		``,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":13}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	tokens, err := copyOpenAIStreamWithUsage(context.Background(), upstream, sink)
	if err != nil {
		t.Fatalf("copy stream: %v", err)
	}
	if !tokens.Known || tokens.InputTokens != 11 || tokens.OutputTokens != 13 {
		t.Fatalf("tokens = %+v, want 11/13 known", tokens)
	}
	got := sink.String()
	if !strings.Contains(got, `"prompt_tokens":11`) || !strings.Contains(got, "data: [DONE]") {
		t.Fatalf("stream output not copied:\n%s", got)
	}
}

func TestOpenAIStreamCopyMarksFirstContent(t *testing.T) {
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{},"finish_reason":null}],"usage":null}`,
		``,
		`data: {"choices":[{"delta":{"content":"answer"},"finish_reason":null}],"usage":null}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))

	if _, err := copyOpenAIStreamWithUsage(ctx, upstream, &testStreamSink{}); err != nil {
		t.Fatalf("copy stream: %v", err)
	}
	if _, ok := trace.Snapshot()["first_content_ms"]; !ok {
		t.Fatalf("first content timing was not recorded: %+v", trace.Snapshot())
	}
}

func TestAnthropicStreamFirstContentIgnoresSyntheticMessageStart(t *testing.T) {
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	requestBody := []byte(`{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":0}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	if _, err := translateOpenAIStreamToAnthropic(ctx, requestBody, upstream, sink); err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	if strings.Contains(sink.String(), "event: message_start") && trace.Snapshot()["first_content_ms"] != nil {
		t.Fatalf("synthetic message_start recorded first content: %+v", trace.Snapshot())
	}
}

func TestAnthropicStreamMarksFirstContentOnTextDelta(t *testing.T) {
	trace := phasetiming.New(time.Now())
	ctx := phasetiming.ContextWithTrace(context.Background(), trace)
	requestBody := []byte(`{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"answer"},"finish_reason":null}],"usage":null}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))

	if _, err := translateOpenAIStreamToAnthropic(ctx, requestBody, upstream, &testStreamSink{}); err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	if _, ok := trace.Snapshot()["first_content_ms"]; !ok {
		t.Fatalf("first content timing was not recorded: %+v", trace.Snapshot())
	}
}

func TestAnthropicStreamRewriterRejectsEarlyEOFWithoutTerminalEvent(t *testing.T) {
	requestBody := []byte(`{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"partial answer"}}]}`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	_, err := translateOpenAIStreamToAnthropic(context.Background(), requestBody, upstream, sink)
	if err == nil {
		t.Fatal("expected terminal event error")
	}
	if !strings.Contains(err.Error(), "upstream stream ended before terminal event") {
		t.Fatalf("error = %v", err)
	}
	got := sink.String()
	if strings.Contains(got, "event: message_stop") {
		t.Fatalf("early EOF emitted message_stop:\n%s", got)
	}
	if !strings.Contains(got, `"partial answer"`) {
		t.Fatalf("partial body was not forwarded before error:\n%s", got)
	}
}

func TestAnthropicStreamRewriterAcceptsDoneWithoutFinishReason(t *testing.T) {
	requestBody := []byte(`{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"answer"}}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	if _, err := translateOpenAIStreamToAnthropic(context.Background(), requestBody, upstream, sink); err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	got := sink.String()
	if !strings.Contains(got, "event: message_stop") {
		t.Fatalf("stream output missing message_stop:\n%s", got)
	}
}

func TestAnthropicStreamRewriterConvertsToolCalls(t *testing.T) {
	requestBody := []byte(`{"model":"minimax-m2.7","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	upstream := strings.NewReader(strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read_file","arguments":"{\"path\""}}]}}]}`,
		``,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"a.txt\"}"}}]}}]}`,
		``,
		`data: {"choices":[{"finish_reason":"tool_calls"}]}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n"))
	sink := &testStreamSink{}

	if _, err := translateOpenAIStreamToAnthropic(context.Background(), requestBody, upstream, sink); err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	got := sink.String()
	required := []string{
		"event: message_start",
		`"type":"tool_use"`,
		`"name":"read_file"`,
		"event: content_block_delta",
		`"type":"input_json_delta"`,
		`"partial_json":"{\"path\""`,
		`"partial_json":":\"a.txt\"}"`,
		"event: message_delta",
		`"stop_reason":"tool_use"`,
		"event: message_stop",
	}
	for _, want := range required {
		if !strings.Contains(got, want) {
			t.Fatalf("stream output missing %q:\n%s", want, got)
		}
	}
}

func TestAddCodebuffStopPreservesExistingStops(t *testing.T) {
	body := map[string]any{"stop": []string{"</done>"}}
	addCodebuffStop(body)
	stops := body["stop"].([]string)
	if len(stops) != 2 || stops[0] != "</done>" || stops[1] != "\"cb_easp\"" {
		t.Fatalf("stops = %+v", stops)
	}

	body = map[string]any{"stop": "</done>"}
	addCodebuffStop(body)
	stops = body["stop"].([]string)
	if len(stops) != 2 || stops[0] != "</done>" || stops[1] != "\"cb_easp\"" {
		t.Fatalf("string stop merged as %+v", stops)
	}
}

func TestPrepareStreamOutboundSupportsAnthropicMessages(t *testing.T) {
	a := New(WithBaseURL("https://codebuff.test"))
	tp := &sequenceTransport{t: t}
	var runStarted bool
	tp.respond = func(req *channels.OutboundRequest, idx int) (*channels.OutboundResponse, error) {
		path := mustPath(t, req.URL)
		switch {
		case req.Method == http.MethodGet && path == "/api/v1/freebuff/session":
			return &channels.OutboundResponse{Status: http.StatusNoContent, Headers: http.Header{}}, nil
		case req.Method == http.MethodPost && path == "/api/v1/freebuff/session":
			return jsonResponse(200, map[string]any{
				"status":     "active",
				"instanceId": "inst-1",
				"model":      "minimax/minimax-m2.7",
			}), nil
		case req.Method == http.MethodPost && path == "/api/v1/agent-runs" && strings.Contains(string(req.Body), agentID) && strings.Contains(string(req.Body), "START"):
			runStarted = true
			return jsonResponse(200, map[string]any{"runId": "run-anthropic"}), nil
		}
		return nil, errors.New("unexpected request")
	}

	state, err := a.CreateSession(context.Background(), account(), "freebuff|minimax/minimax-m2.7", tp)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	lease := channels.NewLease("sess-1", "acc-1", ID, "freebuff|minimax/minimax-m2.7", state, func(channels.Verdict) {})
	out, err := a.PrepareStreamOutbound(context.Background(), lease, &channels.InboundRequest{
		ChannelID: ID,
		Method:    http.MethodPost,
		Path:      "/v1/messages",
		Headers:   http.Header{},
		Body: []byte(`{
			"model":"minimax-m2.7",
			"stream":true,
			"messages":[
				{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"read_file","input":{"path":"a.txt"}}]},
				{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"done"}]}
			],
			"tools":[{"name":"read_file","description":"read","input_schema":{"type":"object","properties":{"path":{"type":"string"}},"additionalProperties":false}}]
		}`),
	})
	if err != nil {
		t.Fatalf("prepare stream outbound: %v", err)
	}
	if !runStarted {
		t.Fatal("run start was not called")
	}
	if got, want := mustPath(t, out.URL), "/api/v1/chat/completions"; got != want {
		t.Fatalf("out path = %s, want %s", got, want)
	}
	if out.Headers.Get("Accept") != "text/event-stream" || out.Headers.Get("Accept-Encoding") != "identity" {
		t.Fatalf("stream headers not set: %+v", out.Headers)
	}

	var outbound map[string]any
	if err := json.Unmarshal(out.Body, &outbound); err != nil {
		t.Fatalf("decode outbound: %v", err)
	}
	if outbound["model"] != "minimax/minimax-m2.7" || outbound["stream"] != true {
		t.Fatalf("unexpected outbound model/stream: %+v", outbound)
	}
	messages := outbound["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("messages len = %d, want 2: %+v", len(messages), messages)
	}
	assistant := messages[0].(map[string]any)
	if _, ok := assistant["tool_calls"]; !ok {
		t.Fatalf("assistant tool_calls missing: %+v", assistant)
	}
	toolResult := messages[1].(map[string]any)
	if toolResult["role"] != "tool" || toolResult["tool_call_id"] != "toolu_1" {
		t.Fatalf("tool result not converted: %+v", toolResult)
	}
	meta := outbound["codebuff_metadata"].(map[string]any)
	if meta["freebuff_instance_id"] != "inst-1" || meta["run_id"] != "run-anthropic" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
}

func decodeJSONMap(t *testing.T, raw string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	return out
}

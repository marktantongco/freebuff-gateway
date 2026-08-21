package freebuff

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"freebuff-reverse/internal/channels"
	"freebuff-reverse/internal/phasetiming"
)

func decodeAnthropicBody(raw []byte) (map[string]any, string, bool, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return nil, "", false, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	if body == nil {
		body = map[string]any{}
	}
	model, _ := body["model"].(string)
	return body, CanonicalModel(model), boolValue(body["stream"]), nil
}

func anthropicToOpenAI(body map[string]any, model string) map[string]any {
	messages := make([]any, 0)
	if system, ok := body["system"]; ok {
		messages = append(messages, map[string]any{
			"role":    "system",
			"content": anthropicSystemToOpenAIContent(system),
		})
	}
	if rawMessages, ok := body["messages"].([]any); ok {
		for _, raw := range rawMessages {
			msg, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			openaiMsg := anthropicMessageToOpenAI(msg)
			if toolResults, ok := openaiMsg["_tool_results"].([]map[string]any); ok && len(toolResults) > 0 {
				delete(openaiMsg, "_tool_results")
				for _, tr := range toolResults {
					messages = append(messages, tr)
				}
				if messageHasContent(openaiMsg) {
					messages = append(messages, openaiMsg)
				}
				continue
			}
			delete(openaiMsg, "_tool_results")
			messages = append(messages, openaiMsg)
		}
	}
	out := map[string]any{
		"model":    model,
		"messages": normalizeOpenAIToolMessages(messages),
		"stream":   boolValue(body["stream"]),
	}
	if v, ok := body["max_tokens"]; ok {
		out["max_tokens"] = v
	}
	if v, ok := body["temperature"]; ok {
		out["temperature"] = v
	}
	copyIfPresent(out, body, "top_p", "top_k", "metadata", "service_tier", "reasoning_effort")
	if stop, ok := anthropicStopSequencesToOpenAI(body["stop_sequences"]); ok {
		out["stop"] = stop
	}
	if thinking, ok := body["thinking"]; ok {
		if upstreamThinking, keep := anthropicThinkingToUpstream(thinking); keep {
			out["thinking"] = upstreamThinking
		}
		if _, exists := out["reasoning_effort"]; !exists {
			if effort := reasoningEffortFromThinking(thinking); effort != "" {
				out["reasoning_effort"] = effort
			}
		}
	}
	if tools, ok := body["tools"].([]any); ok && len(tools) > 0 {
		openaiTools := make([]any, 0, len(tools))
		for _, raw := range tools {
			tool, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			fn := map[string]any{
				"name":        stringValue(tool["name"]),
				"description": stringValue(tool["description"]),
			}
			if schema, ok := tool["input_schema"]; ok {
				fn["parameters"] = sanitizeSchema(schema)
			}
			openaiTools = append(openaiTools, map[string]any{
				"type":     "function",
				"function": fn,
			})
		}
		if len(openaiTools) > 0 {
			out["tools"] = openaiTools
		}
	}
	if toolChoice, ok := anthropicToolChoiceToOpenAI(body["tool_choice"]); ok {
		out["tool_choice"] = toolChoice
	}
	if toolChoice, ok := body["tool_choice"].(map[string]any); ok && boolValue(toolChoice["disable_parallel_tool_use"]) {
		out["parallel_tool_calls"] = false
	}
	return out
}

func anthropicMessageToOpenAI(msg map[string]any) map[string]any {
	role := stringValue(msg["role"])
	content := msg["content"]
	if _, ok := content.(string); ok {
		return map[string]any{"role": role, "content": content}
	}
	blocks, ok := content.([]any)
	if !ok {
		return map[string]any{"role": role, "content": valueToString(content)}
	}
	switch role {
	case "assistant":
		textParts := make([]string, 0)
		thinkingParts := make([]string, 0)
		toolCalls := make([]any, 0)
		for _, raw := range blocks {
			block, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			switch stringValue(block["type"]) {
			case "text":
				if text := stringValue(block["text"]); text != "" {
					textParts = append(textParts, text)
				}
			case "thinking":
				if thinking := stringValue(block["thinking"]); thinking != "" {
					thinkingParts = append(thinkingParts, thinking)
				}
			case "redacted_thinking":
				continue
			case "tool_use":
				input := block["input"]
				if input == nil {
					input = map[string]any{}
				}
				args, _ := json.Marshal(input)
				toolCalls = append(toolCalls, map[string]any{
					"id":   stringValue(block["id"]),
					"type": "function",
					"function": map[string]any{
						"name":      stringValue(block["name"]),
						"arguments": string(args),
					},
				})
			default:
				textParts = append(textParts, valueToString(block))
			}
		}
		out := map[string]any{"role": role}
		if len(thinkingParts) > 0 {
			out["reasoning_content"] = strings.Join(thinkingParts, "\n")
		}
		if len(toolCalls) > 0 {
			out["tool_calls"] = toolCalls
			if len(textParts) > 0 {
				out["content"] = strings.Join(textParts, "\n")
			} else {
				out["content"] = nil
			}
			return out
		}
		out["content"] = strings.Join(textParts, "\n")
		return out
	case "user":
		textParts := make([]string, 0)
		toolResults := make([]map[string]any, 0)
		for _, raw := range blocks {
			block, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			switch stringValue(block["type"]) {
			case "text":
				if text := stringValue(block["text"]); text != "" {
					textParts = append(textParts, text)
				}
			case "tool_result":
				content := block["content"]
				resultText, ok := content.(string)
				if !ok {
					resultText = valueToString(content)
				}
				toolResults = append(toolResults, map[string]any{
					"role":         "tool",
					"tool_call_id": stringValue(block["tool_use_id"]),
					"content":      resultText,
				})
			default:
				textParts = append(textParts, valueToString(block))
			}
		}
		content := any(strings.Join(textParts, "\n"))
		out := map[string]any{
			"role":    role,
			"content": content,
		}
		if len(toolResults) > 0 {
			out["_tool_results"] = toolResults
		}
		return out
	default:
		return map[string]any{"role": role, "content": anthropicBlocksToText(blocks)}
	}
}

func messageHasContent(msg map[string]any) bool {
	switch content := msg["content"].(type) {
	case string:
		return content != ""
	case []any:
		return len(content) > 0
	default:
		return content != nil
	}
}

func anthropicSystemToOpenAIContent(system any) any {
	switch v := system.(type) {
	case string:
		return v
	case []any:
		textParts := make([]string, 0, len(v))
		for _, raw := range v {
			block, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if stringValue(block["type"]) != "text" {
				textParts = append(textParts, valueToString(block))
				continue
			}
			text := stringValue(block["text"])
			if text == "" {
				continue
			}
			textParts = append(textParts, text)
		}
		return strings.Join(textParts, "\n")
	default:
		return valueToString(system)
	}
}

func copyIfPresent(dst, src map[string]any, keys ...string) {
	for _, key := range keys {
		if value, ok := src[key]; ok {
			dst[key] = value
		}
	}
}

func anthropicStopSequencesToOpenAI(value any) (any, bool) {
	switch typed := value.(type) {
	case string:
		return typed, typed != ""
	case []any:
		out := make([]string, 0, len(typed))
		for _, raw := range typed {
			if stop := stringValue(raw); stop != "" {
				out = append(out, stop)
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	case []string:
		out := make([]string, 0, len(typed))
		for _, stop := range typed {
			if stop != "" {
				out = append(out, stop)
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	default:
		return nil, false
	}
}

func anthropicToolChoiceToOpenAI(value any) (any, bool) {
	switch typed := value.(type) {
	case string:
		if typed == "" {
			return nil, false
		}
		return typed, true
	case map[string]any:
		switch stringValue(typed["type"]) {
		case "auto":
			return "auto", true
		case "any":
			return "required", true
		case "none":
			return "none", true
		case "tool":
			name := stringValue(typed["name"])
			if name == "" {
				return nil, false
			}
			return map[string]any{
				"type": "function",
				"function": map[string]any{
					"name": name,
				},
			}, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func reasoningEffortFromThinking(value any) string {
	thinking, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	switch stringValue(thinking["type"]) {
	case "adaptive":
		return "medium"
	case "enabled":
	default:
		return ""
	}
	budget := intValue(thinking["budget_tokens"])
	switch {
	case budget <= 0:
		return "medium"
	case budget < 4096:
		return "low"
	case budget < 12000:
		return "medium"
	default:
		return "high"
	}
}

func anthropicThinkingToUpstream(value any) (any, bool) {
	thinking, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	switch stringValue(thinking["type"]) {
	case "enabled", "disabled":
		return thinking, true
	default:
		return nil, false
	}
}

func normalizeOpenAIToolMessages(messages []any) []any {
	skip := make(map[int]bool)
	out := make([]any, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		if skip[i] {
			continue
		}
		msg, ok := messages[i].(map[string]any)
		if !ok {
			out = append(out, messages[i])
			continue
		}
		if stringValue(msg["role"]) == "assistant" {
			toolCalls := anySlice(msg["tool_calls"])
			if len(toolCalls) > 0 {
				ids := toolCallIDs(toolCalls)
				found := make(map[string]bool)
				toolMessages := make([]any, 0, len(ids))
				for j := i + 1; j < len(messages) && len(found) < len(ids); j++ {
					next, ok := messages[j].(map[string]any)
					if !ok {
						continue
					}
					role := stringValue(next["role"])
					if role == "assistant" {
						break
					}
					if role != "tool" {
						continue
					}
					id := stringValue(next["tool_call_id"])
					if ids[id] && !found[id] {
						found[id] = true
						toolMessages = append(toolMessages, next)
						skip[j] = true
					}
				}
				if len(ids) > 0 && len(found) == len(ids) {
					out = append(out, msg)
					out = append(out, toolMessages...)
				} else {
					out = append(out, assistantWithoutToolCalls(msg))
				}
				continue
			}
		}
		if stringValue(msg["role"]) == "tool" {
			out = append(out, orphanToolMessageToUser(msg))
			continue
		}
		out = append(out, msg)
	}
	return out
}

func (a *Adapter) RewriteStream(ctx context.Context, lease *channels.Lease, in *channels.InboundRequest, upstream io.Reader, downstream channels.StreamWriter) error {
	_, err := a.RewriteStreamWithUsage(ctx, lease, in, upstream, downstream)
	return err
}

func (a *Adapter) RewriteStreamWithUsage(ctx context.Context, lease *channels.Lease, in *channels.InboundRequest, upstream io.Reader, downstream channels.StreamWriter) (channels.TokenUsage, error) {
	if in == nil || in.Path != "/v1/messages" {
		return copyOpenAIStreamWithUsage(ctx, upstream, downstream)
	}
	return translateOpenAIStreamToAnthropic(ctx, in.Body, upstream, downstream)
}

type anthropicStreamState struct {
	nextBlockIndex  int
	thinkingIndex   int
	thinkingStarted bool
	thinkingClosed  bool
	textIndex       int
	textStarted     bool
	textClosed      bool
	toolCalls       map[int]*toolCallState
	finalStopReason string
	usage           map[string]any
	tokenUsage      channels.TokenUsage
	upstreamDone    bool
	finishSeen      bool
	trace           *phasetiming.Trace
}

type toolCallState struct {
	index       int
	id          string
	name        string
	nameStarted bool
	blockClosed bool
}

func translateOpenAIStreamToAnthropic(ctx context.Context, rawRequest []byte, upstream io.Reader, downstream channels.StreamWriter) (channels.TokenUsage, error) {
	_, requestedModel, _, _ := decodeAnthropicBody(rawRequest)
	state := &anthropicStreamState{
		toolCalls:       make(map[int]*toolCallState),
		finalStopReason: "end_turn",
		trace:           phasetiming.FromContext(ctx),
	}
	messageID := fmt.Sprintf("msg_%d", time.Now().UnixMilli())
	if err := writeSSEEvent(downstream, "message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            messageID,
			"type":          "message",
			"role":          "assistant",
			"content":       []any{},
			"model":         requestedModel,
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens":  0,
				"output_tokens": 0,
			},
		},
	}); err != nil {
		return channels.TokenUsage{}, err
	}
	reader := bufio.NewReaderSize(upstream, 32*1024)
	for {
		if ctx.Err() != nil {
			return channels.TokenUsage{}, ctx.Err()
		}
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			if handleErr := handleOpenAISSELine(downstream, line, state); handleErr != nil {
				return state.tokenUsage, handleErr
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return state.tokenUsage, err
		}
	}
	if !state.terminalSeen() {
		return state.tokenUsage, fmt.Errorf("freebuff: upstream stream ended before terminal event")
	}
	if err := state.closeAll(downstream); err != nil {
		return state.tokenUsage, err
	}
	if err := writeSSEEvent(downstream, "message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   state.finalStopReason,
			"stop_sequence": nil,
		},
		"usage": state.messageDeltaUsage(),
	}); err != nil {
		return state.tokenUsage, err
	}
	if err := writeSSEEvent(downstream, "message_stop", map[string]any{"type": "message_stop"}); err != nil {
		return state.tokenUsage, err
	}
	return state.tokenUsage, nil
}

func handleOpenAISSELine(downstream channels.StreamWriter, line string, state *anthropicStreamState) error {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || !strings.HasPrefix(trimmed, "data:") {
		return nil
	}
	data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
	if data == "" {
		return nil
	}
	if data == "[DONE]" {
		state.upstreamDone = true
		return nil
	}
	var parsed map[string]any
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return nil
	}
	choice := firstChoice(parsed)
	delta, _ := choice["delta"].(map[string]any)
	if finishReason := stringValue(choice["finish_reason"]); finishReason != "" {
		state.setStopReason(finishReason)
		state.finishSeen = true
	}
	if usage, ok := parsed["usage"].(map[string]any); ok && len(usage) > 0 {
		state.usage = openAIUsageToAnthropic(usage)
		state.tokenUsage = openAITokenUsage(usage)
	}
	if delta == nil {
		return nil
	}
	if reasoning := firstString(delta, "reasoning_content", "reasoning", "reasoning_text", "thinking"); reasoning != "" {
		if err := state.ensureThinking(downstream); err != nil {
			return err
		}
		state.markFirstContent()
		if err := writeSSEEvent(downstream, "content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": state.thinkingIndex,
			"delta": map[string]any{
				"type":     "thinking_delta",
				"thinking": reasoning,
			},
		}); err != nil {
			return err
		}
	}
	if content := stringValue(delta["content"]); content != "" {
		if err := state.ensureText(downstream); err != nil {
			return err
		}
		state.markFirstContent()
		return writeSSEEvent(downstream, "content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": state.textIndex,
			"delta": map[string]any{
				"type": "text_delta",
				"text": content,
			},
		})
	}
	if rawToolCalls, ok := delta["tool_calls"].([]any); ok {
		if err := state.closeText(downstream); err != nil {
			return err
		}
		for _, raw := range rawToolCalls {
			tcDelta, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			index := int(numberValue(tcDelta["index"]))
			tc := state.toolState(index)
			if id := stringValue(tcDelta["id"]); id != "" {
				tc.id = id
			}
			fn, _ := tcDelta["function"].(map[string]any)
			if fn == nil {
				continue
			}
			if name := stringValue(fn["name"]); name != "" {
				tc.name = name
				if err := tc.ensureStarted(downstream); err != nil {
					return err
				}
			}
			if args := stringValue(fn["arguments"]); args != "" {
				if err := tc.ensureStarted(downstream); err != nil {
					return err
				}
				state.markFirstContent()
				if err := writeSSEEvent(downstream, "content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": tc.index,
					"delta": map[string]any{
						"type":         "input_json_delta",
						"partial_json": args,
					},
				}); err != nil {
					return err
				}
			}
		}
		state.finalStopReason = "tool_use"
	}
	return nil
}

func (s *anthropicStreamState) ensureThinking(downstream channels.StreamWriter) error {
	if s.thinkingStarted {
		return nil
	}
	s.thinkingIndex = s.nextBlockIndex
	s.nextBlockIndex++
	if err := writeSSEEvent(downstream, "content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": s.thinkingIndex,
		"content_block": map[string]any{
			"type":      "thinking",
			"thinking":  "",
			"signature": "",
		},
	}); err != nil {
		return err
	}
	s.thinkingStarted = true
	return nil
}

func (s *anthropicStreamState) closeThinking(downstream channels.StreamWriter) error {
	if !s.thinkingStarted || s.thinkingClosed {
		return nil
	}
	if err := writeSSEEvent(downstream, "content_block_delta", map[string]any{
		"type":  "content_block_delta",
		"index": s.thinkingIndex,
		"delta": map[string]any{
			"type":      "signature_delta",
			"signature": "",
		},
	}); err != nil {
		return err
	}
	if err := writeSSEEvent(downstream, "content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": s.thinkingIndex,
	}); err != nil {
		return err
	}
	s.thinkingClosed = true
	return nil
}

func (s *anthropicStreamState) ensureText(downstream channels.StreamWriter) error {
	if s.textStarted {
		return nil
	}
	if err := s.closeThinking(downstream); err != nil {
		return err
	}
	s.textIndex = s.nextBlockIndex
	s.nextBlockIndex++
	if err := writeSSEEvent(downstream, "content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": s.textIndex,
		"content_block": map[string]any{
			"type": "text",
			"text": "",
		},
	}); err != nil {
		return err
	}
	s.textStarted = true
	return nil
}

func (s *anthropicStreamState) closeText(downstream channels.StreamWriter) error {
	if !s.textStarted || s.textClosed {
		return nil
	}
	if err := writeSSEEvent(downstream, "content_block_stop", map[string]any{
		"type":  "content_block_stop",
		"index": s.textIndex,
	}); err != nil {
		return err
	}
	s.textClosed = true
	return nil
}

func (s *anthropicStreamState) closeAll(downstream channels.StreamWriter) error {
	if err := s.closeThinking(downstream); err != nil {
		return err
	}
	if err := s.closeText(downstream); err != nil {
		return err
	}
	for _, tc := range s.toolCalls {
		if tc.blockClosed {
			continue
		}
		if err := writeSSEEvent(downstream, "content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": tc.index,
		}); err != nil {
			return err
		}
		tc.blockClosed = true
	}
	return nil
}

func (s *anthropicStreamState) toolState(openAIIndex int) *toolCallState {
	if tc, ok := s.toolCalls[openAIIndex]; ok {
		return tc
	}
	tc := &toolCallState{index: s.nextBlockIndex}
	s.nextBlockIndex++
	s.toolCalls[openAIIndex] = tc
	return tc
}

func (s *anthropicStreamState) setStopReason(reason string) {
	switch reason {
	case "tool_calls", "function_call":
		s.finalStopReason = "tool_use"
	case "stop", "":
		s.finalStopReason = "end_turn"
	default:
		s.finalStopReason = reason
	}
}

func (s *anthropicStreamState) terminalSeen() bool {
	return s.upstreamDone || s.finishSeen
}

func (s *anthropicStreamState) messageDeltaUsage() map[string]any {
	if len(s.usage) == 0 {
		return map[string]any{"output_tokens": 0}
	}
	return s.usage
}

func (s *anthropicStreamState) markFirstContent() {
	if s == nil || s.trace == nil {
		return
	}
	s.trace.MarkFirst("first_content_ms")
}

func (tc *toolCallState) ensureStarted(downstream channels.StreamWriter) error {
	if tc.nameStarted {
		return nil
	}
	if tc.id == "" {
		tc.id = fmt.Sprintf("toolu_%d_%d", time.Now().UnixMilli(), tc.index)
	}
	if tc.name == "" {
		tc.name = "unknown"
	}
	if err := writeSSEEvent(downstream, "content_block_start", map[string]any{
		"type":  "content_block_start",
		"index": tc.index,
		"content_block": map[string]any{
			"type":  "tool_use",
			"id":    tc.id,
			"name":  tc.name,
			"input": map[string]any{},
		},
	}); err != nil {
		return err
	}
	tc.nameStarted = true
	return nil
}

func writeSSEEvent(w channels.StreamWriter, event string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
		return err
	}
	w.Flush()
	return nil
}

func copyOpenAIStreamWithUsage(ctx context.Context, r io.Reader, w channels.StreamWriter) (channels.TokenUsage, error) {
	reader := bufio.NewReaderSize(r, 32*1024)
	var tokens channels.TokenUsage
	trace := phasetiming.FromContext(ctx)
	for {
		if ctx.Err() != nil {
			return tokens, ctx.Err()
		}
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			if _, writeErr := w.Write([]byte(line)); writeErr != nil {
				return tokens, writeErr
			}
			w.Flush()
			if trace != nil && openAISSELineHasMeaningfulDelta(line) {
				trace.MarkFirst("first_content_ms")
			}
			if parsed := openAITokenUsageFromSSELine(line); parsed.Known {
				tokens = parsed
			}
		}
		if err != nil {
			if err == io.EOF {
				return tokens, nil
			}
			return tokens, err
		}
	}
}

func openAISSELineHasMeaningfulDelta(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || !strings.HasPrefix(trimmed, "data:") {
		return false
	}
	data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
	if data == "" || data == "[DONE]" {
		return false
	}
	var parsed map[string]any
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return false
	}
	choice := firstChoice(parsed)
	delta, _ := choice["delta"].(map[string]any)
	if len(delta) == 0 {
		return false
	}
	if firstString(delta, "reasoning_content", "reasoning", "reasoning_text", "thinking") != "" {
		return true
	}
	if stringValue(delta["content"]) != "" {
		return true
	}
	if rawToolCalls, ok := delta["tool_calls"].([]any); ok {
		for _, raw := range rawToolCalls {
			tcDelta, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			fn, _ := tcDelta["function"].(map[string]any)
			if fn != nil && stringValue(fn["arguments"]) != "" {
				return true
			}
		}
	}
	return false
}

func openAITokenUsageFromSSELine(line string) channels.TokenUsage {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || !strings.HasPrefix(trimmed, "data:") {
		return channels.TokenUsage{}
	}
	data := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
	if data == "" || data == "[DONE]" {
		return channels.TokenUsage{}
	}
	var parsed map[string]any
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return channels.TokenUsage{}
	}
	usage, ok := parsed["usage"].(map[string]any)
	if !ok || len(usage) == 0 {
		return channels.TokenUsage{}
	}
	return openAITokenUsage(usage)
}

func openAITokenUsage(usage map[string]any) channels.TokenUsage {
	inputTokens, hasInputTokens := intValueOK(usage["prompt_tokens"])
	if !hasInputTokens {
		inputTokens, hasInputTokens = intValueOK(usage["input_tokens"])
	}
	outputTokens, hasOutputTokens := intValueOK(usage["completion_tokens"])
	if !hasOutputTokens {
		outputTokens, hasOutputTokens = intValueOK(usage["output_tokens"])
	}
	if !hasInputTokens && !hasOutputTokens {
		return channels.TokenUsage{}
	}
	return channels.TokenUsage{
		InputTokens:  int(inputTokens),
		OutputTokens: int(outputTokens),
		Known:        true,
	}
}

func openAIUsageToAnthropic(usage map[string]any) map[string]any {
	out := make(map[string]any)
	promptTokens, hasPromptTokens := intValueOK(usage["prompt_tokens"])
	completionTokens, hasCompletionTokens := intValueOK(usage["completion_tokens"])

	cacheReadTokens, hasCacheReadTokens := intValueOK(usage["prompt_cache_hit_tokens"])
	if !hasCacheReadTokens {
		if details, ok := usage["prompt_tokens_details"].(map[string]any); ok {
			cacheReadTokens, hasCacheReadTokens = intValueOK(details["cached_tokens"])
		}
	}

	if hasPromptTokens {
		inputTokens := promptTokens
		if hasCacheReadTokens && cacheReadTokens > 0 && promptTokens >= cacheReadTokens {
			inputTokens = promptTokens - cacheReadTokens
		}
		out["input_tokens"] = inputTokens
	}
	if hasCacheReadTokens && cacheReadTokens > 0 {
		out["cache_read_input_tokens"] = cacheReadTokens
	}
	if cacheCreationTokens, ok := intValueOK(usage["cache_creation_input_tokens"]); ok && cacheCreationTokens > 0 {
		out["cache_creation_input_tokens"] = cacheCreationTokens
	}
	if hasCompletionTokens {
		out["output_tokens"] = completionTokens
	}
	if len(out) == 0 {
		out["output_tokens"] = 0
	}
	return out
}

func firstChoice(m map[string]any) map[string]any {
	choices, _ := m["choices"].([]any)
	if len(choices) == 0 {
		return nil
	}
	choice, _ := choices[0].(map[string]any)
	return choice
}

func anySlice(v any) []any {
	switch typed := v.(type) {
	case []any:
		return typed
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return nil
	}
}

func toolCallIDs(toolCalls []any) map[string]bool {
	ids := make(map[string]bool)
	for _, raw := range toolCalls {
		tc, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id := stringValue(tc["id"]); id != "" {
			ids[id] = true
		}
	}
	return ids
}

func assistantWithoutToolCalls(msg map[string]any) map[string]any {
	out := make(map[string]any, len(msg))
	for k, v := range msg {
		if k != "tool_calls" {
			out[k] = v
		}
	}
	if stringValue(out["content"]) == "" {
		out["content"] = summarizeToolCalls(anySlice(msg["tool_calls"]))
	}
	return out
}

func orphanToolMessageToUser(msg map[string]any) map[string]any {
	content := stringValue(msg["content"])
	if content == "" {
		content = valueToString(msg["content"])
	}
	id := stringValue(msg["tool_call_id"])
	if id != "" {
		content = fmt.Sprintf("[tool_result:%s] %s", id, content)
	}
	return map[string]any{"role": "user", "content": content}
}

func summarizeToolCalls(toolCalls []any) string {
	parts := make([]string, 0, len(toolCalls))
	for _, raw := range toolCalls {
		tc, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fn, _ := tc["function"].(map[string]any)
		name := stringValue(fn["name"])
		args := stringValue(fn["arguments"])
		if name == "" {
			name = stringValue(tc["id"])
		}
		if name != "" {
			parts = append(parts, fmt.Sprintf("[tool_call:%s] %s", name, args))
		}
	}
	return strings.Join(parts, "\n")
}

func sanitizeSchema(schema any) any {
	switch v := schema.(type) {
	case map[string]any:
		unsupported := map[string]bool{
			"not": true, "anyOf": true, "oneOf": true, "allOf": true,
			"if": true, "then": true, "else": true, "$ref": true,
			"$defs": true, "definitions": true, "patternProperties": true,
			"additionalProperties": true, "default": true, "examples": true,
		}
		out := make(map[string]any, len(v))
		for key, val := range v {
			if unsupported[key] {
				continue
			}
			out[key] = sanitizeSchema(val)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = sanitizeSchema(item)
		}
		return out
	default:
		return schema
	}
}

func anthropicBlocksToText(blocks []any) string {
	parts := make([]string, 0, len(blocks))
	for _, raw := range blocks {
		block, ok := raw.(map[string]any)
		if !ok {
			parts = append(parts, valueToString(raw))
			continue
		}
		switch stringValue(block["type"]) {
		case "text":
			parts = append(parts, stringValue(block["text"]))
		case "tool_use":
			input, _ := json.Marshal(block["input"])
			parts = append(parts, fmt.Sprintf("[tool_use:%s] %s", stringValue(block["name"]), input))
		case "tool_result":
			parts = append(parts, "[tool_result] "+valueToString(block["content"]))
		default:
			parts = append(parts, valueToString(block))
		}
	}
	return strings.Join(parts, "\n")
}

func stringValue(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringValue(m[key]); value != "" {
			return value
		}
	}
	return ""
}

func numberValue(v any) float64 {
	switch typed := v.(type) {
	case json.Number:
		f, _ := typed.Float64()
		return f
	case float64:
		return typed
	case int:
		return float64(typed)
	default:
		return 0
	}
}

func intValue(v any) int64 {
	n, _ := intValueOK(v)
	return n
}

func intValueOK(v any) (int64, bool) {
	switch typed := v.(type) {
	case json.Number:
		n, err := typed.Int64()
		if err == nil {
			return n, true
		}
		f, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		return int64(f), true
	case float64:
		return int64(typed), true
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	default:
		return 0, false
	}
}

func valueToString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(raw)
	}
}

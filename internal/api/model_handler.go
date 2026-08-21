package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

const freeBuffChannelID = "freebuff"

type ModelHandler struct {
	registry *channels.Registry
}

type anthropicModelsPage struct {
	Object  string               `json:"object,omitempty"`
	Data    []anthropicModelInfo `json:"data"`
	HasMore bool                 `json:"has_more"`
	FirstID *string              `json:"first_id"`
	LastID  *string              `json:"last_id"`
}

type anthropicModelInfo struct {
	ID             string         `json:"id"`
	Type           string         `json:"type"`
	Object         string         `json:"object,omitempty"`
	DisplayName    string         `json:"display_name"`
	CreatedAt      string         `json:"created_at"`
	MaxInputTokens *int           `json:"max_input_tokens"`
	MaxTokens      *int           `json:"max_tokens"`
	Capabilities   map[string]any `json:"capabilities"`
}

func NewModelHandler(registry *channels.Registry) *ModelHandler {
	return &ModelHandler{registry: registry}
}

func (h *ModelHandler) ListModels(w http.ResponseWriter, r *http.Request) {
	models := h.enabledFreeBuffModels()
	resp := anthropicModelsPage{
		Object:  "list",
		Data:    make([]anthropicModelInfo, 0, len(models)),
		HasMore: false,
	}
	for _, model := range models {
		resp.Data = append(resp.Data, anthropicModelFromCatalog(model))
	}
	if len(resp.Data) > 0 {
		first := resp.Data[0].ID
		last := resp.Data[len(resp.Data)-1].ID
		resp.FirstID = &first
		resp.LastID = &last
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *ModelHandler) GetModel(w http.ResponseWriter, r *http.Request) {
	requested := strings.TrimSpace(r.PathValue("modelID"))
	if requested == "" {
		writeJSONError(w, http.StatusBadRequest, "model id required")
		return
	}
	for _, model := range h.enabledFreeBuffModels() {
		if modelMatches(model, requested) {
			writeJSON(w, http.StatusOK, anthropicModelFromCatalog(model))
			return
		}
	}
	writeJSONError(w, http.StatusNotFound, "model not found")
}

func (h *ModelHandler) enabledFreeBuffModels() []channels.ModelInfo {
	if h == nil || h.registry == nil {
		return []channels.ModelInfo{}
	}
	adapter, ok := h.registry.Get(freeBuffChannelID)
	if !ok {
		return []channels.ModelInfo{}
	}
	provider, ok := adapter.(channels.ModelCatalogProvider)
	if !ok {
		return []channels.ModelInfo{}
	}
	out := []channels.ModelInfo{}
	for _, model := range provider.ModelCatalog() {
		if model.Enabled {
			out = append(out, model)
		}
	}
	return out
}

func anthropicModelFromCatalog(model channels.ModelInfo) anthropicModelInfo {
	return anthropicModelInfo{
		ID:             model.ID,
		Type:           "model",
		Object:         "model",
		DisplayName:    displayNameForModel(model.ID),
		CreatedAt:      time.Unix(0, 0).UTC().Format(time.RFC3339),
		MaxInputTokens: nil,
		MaxTokens:      nil,
		Capabilities:   nil,
	}
}

func modelMatches(model channels.ModelInfo, requested string) bool {
	key := strings.ToLower(strings.TrimSpace(requested))
	if key == strings.ToLower(model.ID) {
		return true
	}
	for _, alias := range model.Aliases {
		if key == strings.ToLower(strings.TrimSpace(alias)) {
			return true
		}
	}
	return false
}

func displayNameForModel(id string) string {
	switch id {
	case "minimax/minimax-m2.7":
		return "MiniMax M2.7"
	case "deepseek/deepseek-v4-flash":
		return "DeepSeek V4 Flash"
	case "moonshotai/kimi-k2.6":
		return "Kimi K2.6"
	case "deepseek/deepseek-v4-pro":
		return "DeepSeek V4 Pro"
	case "z-ai/glm-5.1":
		return "GLM 5.1"
	default:
		parts := strings.Split(id, "/")
		return parts[len(parts)-1]
	}
}

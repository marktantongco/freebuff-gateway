package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
)

func TestModelHandlerReturnsAnthropicModelPageForEnabledFreeBuffModels(t *testing.T) {
	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: freebuffstate.ChannelID}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewModelHandler(registry)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()

	handler.ListModels(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body anthropicModelsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Object != "list" || body.HasMore {
		t.Fatalf("page metadata = %+v", body)
	}
	if len(body.Data) != 1 {
		t.Fatalf("models = %+v", body.Data)
	}
	if body.Data[0].ID != "demo/model" || body.Data[0].Type != "model" || body.Data[0].DisplayName == "" {
		t.Fatalf("model = %+v", body.Data[0])
	}
	if body.FirstID == nil || *body.FirstID != "demo/model" || body.LastID == nil || *body.LastID != "demo/model" {
		t.Fatalf("cursors = first:%v last:%v", body.FirstID, body.LastID)
	}
}

func TestModelHandlerCanRetrieveModelBySlashID(t *testing.T) {
	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: freebuffstate.ChannelID}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	handler := NewModelHandler(registry)
	req := httptest.NewRequest(http.MethodGet, "/v1/models/demo/model", nil)
	req.SetPathValue("modelID", "demo/model")
	rec := httptest.NewRecorder()

	handler.GetModel(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body anthropicModelInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.ID != "demo/model" || body.Type != "model" {
		t.Fatalf("model = %+v", body)
	}
}

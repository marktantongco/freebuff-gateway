package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
	"github.com/marktantongco/freebuff-gateway/internal/channels"
	"github.com/marktantongco/freebuff-gateway/internal/freebuffstate"
	"github.com/marktantongco/freebuff-gateway/internal/storage"
)

func TestBuildRouterRegistersAdminAndModelAuthRoutes(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "router-auth.db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()

	keyRepo := authkeys.NewRepo(db)
	if _, err := keyRepo.CreateWithKey("client", "sk-router-models"); err != nil {
		t.Fatalf("create key: %v", err)
	}
	registry := channels.NewRegistry()
	if err := registry.Register(&fakeCredentialImportAdapter{id: freebuffstate.ChannelID}); err != nil {
		t.Fatalf("register adapter: %v", err)
	}
	admin := NewAdminHandler(registry, nil, nil, nil, nil, nil)
	proxy := NewProxyHandler(registry, nil)
	mux := BuildRouter(
		admin,
		proxy,
		nil,
		NewAdminAuthenticator("admin-pass", 0),
		NewAPIKeyAuthenticator(keyRepo),
	)

	meReq := httptest.NewRequest(http.MethodGet, "/api/admin/me", nil)
	meRec := httptest.NewRecorder()
	mux.ServeHTTP(meRec, meReq)
	if meRec.Code != http.StatusOK {
		t.Fatalf("me status = %d body=%s", meRec.Code, meRec.Body.String())
	}

	modelsReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	modelsReq.Header.Set("x-api-key", "sk-router-models")
	modelsRec := httptest.NewRecorder()
	mux.ServeHTTP(modelsRec, modelsReq)
	if modelsRec.Code != http.StatusOK {
		t.Fatalf("models status = %d body=%s", modelsRec.Code, modelsRec.Body.String())
	}
	var body anthropicModelsPage
	if err := json.Unmarshal(modelsRec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode models: %v", err)
	}
	if len(body.Data) != 1 || body.Data[0].ID != "demo/model" {
		t.Fatalf("models = %+v", body.Data)
	}
}

func TestWebAppHandlerServesIndexForClientRoutes(t *testing.T) {
	web := fstest.MapFS{
		"index.html": {Data: []byte("<main>app</main>")},
	}
	handler := webAppHandler(web)

	for _, route := range []string{"/", "/accounts", "/monitoring?range=all"} {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d body=%s", route, rec.Code, rec.Body.String())
		}
		if rec.Body.String() != "<main>app</main>" {
			t.Fatalf("%s body = %q, want index html", route, rec.Body.String())
		}
	}
}

func TestWebAppHandlerDoesNotFallbackForMissingAssets(t *testing.T) {
	web := fstest.MapFS{
		"index.html": {Data: []byte("<main>app</main>")},
	}
	req := httptest.NewRequest(http.MethodGet, "/missing.js", nil)
	rec := httptest.NewRecorder()

	webAppHandler(web).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s, want 404", rec.Code, rec.Body.String())
	}
}

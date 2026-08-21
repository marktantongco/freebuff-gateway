package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

func TestWriteProxyExecutionErrorMapsCapacityLimitedTo429(t *testing.T) {
	rec := httptest.NewRecorder()
	writeProxyExecutionError(rec, errors.Join(errors.New("wrapped"), channels.CapacityLimitedf("premium_capacity_limited")))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "capacity limited") {
		t.Fatalf("body = %s, want capacity limited", rec.Body.String())
	}
}

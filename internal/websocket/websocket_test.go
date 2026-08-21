package websocket

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
)

func TestHubCreate(t *testing.T) {
	hub := NewHub()
	if hub == nil {
		t.Fatal("expected non-nil hub")
	}
	if hub.ClientCount() != 0 {
		t.Fatalf("expected 0 clients, got %d", hub.ClientCount())
	}
}

func TestHubRunStop(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	time.Sleep(10 * time.Millisecond)
	hub.Stop()
}

func TestHubBroadcast(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	// Broadcast without clients should not panic
	hub.Broadcast(MsgTypeHealth, map[string]string{"status": "ok"})
}

func TestHubBroadcastToTopic(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	// Broadcast to topic without clients
	hub.BroadcastToTopic(MsgTypeAlert, map[string]string{"alert": "test"})
}

func TestHubClientCount(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	if hub.ClientCount() != 0 {
		t.Fatalf("expected 0, got %d", hub.ClientCount())
	}
}

func TestHandlerStatus(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	handler := NewHandler(hub)
	req := httptest.NewRequest("GET", "/ws/status", nil)
	rec := httptest.NewRecorder()
	handler.HandleWSStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var status map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &status)
	if _, ok := status["connected_clients"]; !ok {
		t.Fatal("expected connected_clients in response")
	}
}

func TestWebSocketUpgrade(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	handler := NewHandler(hub)

	server := httptest.NewServer(http.HandlerFunc(handler.HandleWebSocket))
	defer server.Close()

	// Connect with WebSocket client
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close()

	// Wait for connection to register
	time.Sleep(50 * time.Millisecond)

	if hub.ClientCount() != 1 {
		t.Fatalf("expected 1 client, got %d", hub.ClientCount())
	}
}

func TestWebSocketBroadcast(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	handler := NewHandler(hub)

	server := httptest.NewServer(http.HandlerFunc(handler.HandleWebSocket))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close()

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Wait for registration
	time.Sleep(50 * time.Millisecond)

	// Broadcast a message
	hub.Broadcast(MsgTypeHealth, map[string]string{"status": "healthy"})

	// Read message
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}

	var received Message
	if err := json.Unmarshal(msg, &received); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if received.Type != MsgTypeHealth {
		t.Fatalf("expected health message, got %s", received.Type)
	}
}

func TestWebSocketDisconnect(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	handler := NewHandler(hub)

	server := httptest.NewServer(http.HandlerFunc(handler.HandleWebSocket))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}

	// Wait for registration
	time.Sleep(50 * time.Millisecond)
	if hub.ClientCount() != 1 {
		t.Fatalf("expected 1 client, got %d", hub.ClientCount())
	}

	// Close connection
	conn.Close()

	// Wait for unregister
	time.Sleep(100 * time.Millisecond)
	if hub.ClientCount() != 0 {
		t.Fatalf("expected 0 clients after disconnect, got %d", hub.ClientCount())
	}
}

func TestWebSocketSubscribe(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	handler := NewHandler(hub)

	server := httptest.NewServer(http.HandlerFunc(handler.HandleWebSocket))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Wait for registration
	time.Sleep(50 * time.Millisecond)

	// Subscribe to alerts topic
	subMsg := Message{
		Type: MsgTypeSubscribe,
		Data: json.RawMessage(`["alert","health"]`),
	}
	subBytes, _ := json.Marshal(subMsg)
	conn.WriteMessage(ws.TextMessage, subBytes)

	time.Sleep(50 * time.Millisecond)

	// Broadcast to alert topic
	hub.BroadcastToTopic(MsgTypeAlert, map[string]string{"alert": "test"})

	// Should receive the message
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}

	var received Message
	json.Unmarshal(msg, &received)
	if received.Type != MsgTypeAlert {
		t.Fatalf("expected alert message, got %s", received.Type)
	}
}

func TestWebSocketMultipleClients(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	handler := NewHandler(hub)

	server := httptest.NewServer(http.HandlerFunc(handler.HandleWebSocket))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")

	// Connect 3 clients
	for i := 0; i < 3; i++ {
		conn, _, err := ws.DefaultDialer.Dial(url, nil)
		if err != nil {
			t.Fatalf("client %d dial: %v", i, err)
		}
		defer conn.Close()
	}

	time.Sleep(100 * time.Millisecond)

	if hub.ClientCount() != 3 {
		t.Fatalf("expected 3 clients, got %d", hub.ClientCount())
	}
}

func TestMessageTypes(t *testing.T) {
	types := []MessageType{
		MsgTypeHealth, MsgTypeAlert, MsgTypeMetrics,
		MsgTypeSession, MsgTypeConfig, MsgTypePing,
		MsgTypePong, MsgTypeSubscribe, MsgTypeUnsubscribe,
	}

	for _, mt := range types {
		if mt == "" {
			t.Fatal("empty message type")
		}
	}
}

func TestIntToString(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{123, "123"},
		{999, "999"},
	}

	for _, tt := range tests {
		result := intToString(tt.input)
		if result != tt.expected {
			t.Fatalf("intToString(%d) = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

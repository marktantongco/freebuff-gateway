package websocket

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"

	ws "github.com/gorilla/websocket"
)

var upgrader = ws.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins in development
		// In production, check the Origin header
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		// Allow localhost origins
		for _, allowed := range []string{"http://localhost", "https://localhost", "http://127.0.0.1"} {
			if len(origin) >= len(allowed) && origin[:len(allowed)] == allowed {
				return true
			}
		}
		return true // Allow all for now
	},
}

// Handler provides WebSocket endpoint handlers.
type Handler struct {
	hub *Hub
}

// NewHandler creates a new WebSocket handler.
func NewHandler(hub *Hub) *Handler {
	return &Handler{hub: hub}
}

// HandleWebSocket handles WebSocket upgrade requests.
func (h *Handler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Optional: verify session cookie for auth
	// For now, allow all connections

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws: upgrade error: %v", err)
		return
	}

	client := &Client{
		hub:    h.hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		id:     generateClientID(),
		topics: make(map[MessageType]bool),
	}

	h.hub.RegisterClient(client)

	// Start pumps
	go client.WritePump()
	go client.ReadPump()
}

// HandleWSStatus returns current WebSocket connection status.
func (h *Handler) HandleWSStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"connected_clients":` + intToString(h.hub.ClientCount()) + `}`))
}

func generateClientID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		result = string(rune('0'+n%10)) + result
		n /= 10
	}
	return result
}

package websocket

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	ws "github.com/gorilla/websocket"
)

// MessageType identifies the type of WebSocket message.
type MessageType string

const (
	MsgTypeHealth     MessageType = "health"
	MsgTypeAlert      MessageType = "alert"
	MsgTypeMetrics    MessageType = "metrics"
	MsgTypeSession    MessageType = "session"
	MsgTypeConfig      MessageType = "config"
	MsgTypeRateLimits  MessageType = "ratelimits"
	MsgTypeAnalytics   MessageType = "analytics"
	MsgTypePing        MessageType = "ping"
	MsgTypePong        MessageType = "pong"
	MsgTypeSubscribe   MessageType = "subscribe"
	MsgTypeUnsubscribe MessageType = "unsubscribe"
)

// Message is a WebSocket message envelope.
type Message struct {
	Type      MessageType     `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

// Client represents a single WebSocket connection.
type Client struct {
	hub        *Hub
	conn       *ws.Conn
	send       chan []byte
	id         string
	topics     map[MessageType]bool
	mu         sync.Mutex
}

// Hub manages all WebSocket clients and broadcasts.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewHub creates a new WebSocket hub.
func NewHub() *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Run starts the hub's event loop.
func (h *Hub) Run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("ws: client connected (total: %d)", h.ClientCount())

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("ws: client disconnected (total: %d)", h.ClientCount())

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Client buffer full, disconnect
					h.mu.RUnlock()
					h.mu.Lock()
					delete(h.clients, client)
					close(client.send)
					h.mu.Unlock()
					h.mu.RLock()
				}
			}
			h.mu.RUnlock()

		case <-ticker.C:
			// Send periodic ping
			ping, _ := json.Marshal(Message{
				Type:      MsgTypePing,
				Timestamp: time.Now().UnixMilli(),
			})
			h.broadcast <- ping

		case <-h.ctx.Done():
			return
		}
	}
}

// Stop shuts down the hub.
func (h *Hub) Stop() {
	h.cancel()
}

// Broadcast sends a message to all subscribed clients.
func (h *Hub) Broadcast(msgType MessageType, data interface{}) {
	rawData, err := json.Marshal(data)
	if err != nil {
		log.Printf("ws: marshal error: %v", err)
		return
	}

	msg := Message{
		Type:      msgType,
		Data:      rawData,
		Timestamp: time.Now().UnixMilli(),
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("ws: marshal message: %v", err)
		return
	}

	h.broadcast <- msgBytes
}

// BroadcastToTopic sends a message to clients subscribed to a specific topic.
func (h *Hub) BroadcastToTopic(topic MessageType, data interface{}) {
	rawData, err := json.Marshal(data)
	if err != nil {
		log.Printf("ws: marshal error: %v", err)
		return
	}

	msg := Message{
		Type:      topic,
		Data:      rawData,
		Timestamp: time.Now().UnixMilli(),
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		client.mu.Lock()
		subscribed := client.topics[topic] || len(client.topics) == 0
		client.mu.Unlock()

		if subscribed {
			select {
			case client.send <- msgBytes:
			default:
			}
		}
	}
}

// ClientCount returns the number of connected clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// RegisterClient adds a client to the hub.
func (h *Hub) RegisterClient(client *Client) {
	h.register <- client
}

// UnregisterClient removes a client from the hub.
func (h *Hub) UnregisterClient(client *Client) {
	h.unregister <- client
}

// WritePump pumps messages from the hub to the WebSocket connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(ws.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(ws.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Drain queued messages into current write
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte("\n"))
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(ws.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump pumps messages from the WebSocket connection to the hub.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.UnregisterClient(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if ws.IsUnexpectedCloseError(err, ws.CloseGoingAway, ws.CloseAbnormalClosure) {
				log.Printf("ws: read error: %v", err)
			}
			return
		}

		// Parse incoming message
		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case MsgTypePong:
			// Response to our ping
		case MsgTypeSubscribe:
			var topics []MessageType
			if err := json.Unmarshal(msg.Data, &topics); err == nil {
				c.mu.Lock()
				for _, t := range topics {
					c.topics[t] = true
				}
				c.mu.Unlock()
			}
		case MsgTypeUnsubscribe:
			var topics []MessageType
			if err := json.Unmarshal(msg.Data, &topics); err == nil {
				c.mu.Lock()
				for _, t := range topics {
					delete(c.topics, t)
				}
				c.mu.Unlock()
			}
		}
	}
}

// SetTopics sets the topics this client subscribes to.
func (c *Client) SetTopics(topics []MessageType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topics = make(map[MessageType]bool)
	for _, t := range topics {
		c.topics[t] = true
	}
}

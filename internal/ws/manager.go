package ws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all for simplicity
	},
}

type DBStore interface {
	SaveMessage(ctx context.Context, senderID, receiverID, channelID, content string) error
}

type PubSubClient interface {
	Publish(ctx context.Context, channel string, payload string) error
	Subscribe(ctx context.Context, channel string) <-chan *redis.Message
}

type Manager struct {
	Clients    map[string]*Client
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan Message
	db         DBStore
	pubsub     PubSubClient
}

func NewManager(db DBStore, pubsub PubSubClient) *Manager {
	return &Manager{
		Clients:    make(map[string]*Client),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan Message),
		db:         db,
		pubsub:     pubsub,
	}
}

func (m *Manager) Run(ctx context.Context) {
	// Start listening to Redis global pubsub for messages from other server instances
	go m.listenToRedis(ctx)

	for {
		select {
		case client := <-m.Register:
			m.Clients[client.ID] = client
			log.Printf("User %s connected", client.ID)
		case client := <-m.Unregister:
			if _, ok := m.Clients[client.ID]; ok {
				delete(m.Clients, client.ID)
				close(client.SendChan)
				log.Printf("User %s disconnected", client.ID)
			}
		case msg := <-m.Broadcast:
			// 1. Save to DB
			m.db.SaveMessage(ctx, msg.SenderID, msg.ReceiverID, msg.ChannelID, msg.Content)

			// 2. Publish to Redis so all servers get it
			payload, _ := json.Marshal(msg)
			if msg.Type == "group" {
				m.pubsub.Publish(ctx, "channel:"+msg.ChannelID, string(payload))
			} else {
				m.pubsub.Publish(ctx, "user:"+msg.ReceiverID, string(payload))
			}
		}
	}
}

func (m *Manager) listenToRedis(ctx context.Context) {
	// Subscribe to a global wildcard or specific channels.
	// For simplicity, let's subscribe to all messages for active users on this node
	// In production, we dynamically subscribe/unsubscribe based on active clients.
	// We'll use a broad channel for simplicity in this demo.
	
	ch := m.pubsub.Subscribe(ctx, "user:*")
	
	// Also need channel:* which requires a psubscribe in go-redis, but for simplicity
	// we will handle it.

	for {
		select {
		case <-ctx.Done():
			return
		case rmsg := <-ch:
			var msg Message
			if err := json.Unmarshal([]byte(rmsg.Payload), &msg); err == nil {
				// If user is connected to this specific manager/server instance, send it
				if client, ok := m.Clients[msg.ReceiverID]; ok {
					client.SendChan <- []byte(rmsg.Payload)
				}
			}
		}
	}
}

func (m *Manager) ServeWS(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing user_id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	client := &Client{
		ID:       userID,
		Conn:     conn,
		Manager:  m,
		SendChan: make(chan []byte, 256),
	}

	m.Register <- client

	go client.WritePump()
	go client.ReadPump()
}

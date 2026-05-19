package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type MockDB struct {
	saveFunc func(ctx context.Context, senderID, receiverID, channelID, content string) error
}

func (m *MockDB) SaveMessage(ctx context.Context, senderID, receiverID, channelID, content string) error {
	if m.saveFunc != nil {
		return m.saveFunc(ctx, senderID, receiverID, channelID, content)
	}
	return nil
}

type MockPubSub struct {
	publishFunc   func(ctx context.Context, channel string, payload string) error
	subscribeFunc func(ctx context.Context, channel string) <-chan *redis.Message
}

func (m *MockPubSub) Publish(ctx context.Context, channel string, payload string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, channel, payload)
	}
	return nil
}

func (m *MockPubSub) Subscribe(ctx context.Context, channel string) <-chan *redis.Message {
	if m.subscribeFunc != nil {
		return m.subscribeFunc(ctx, channel)
	}
	return nil
}

func TestManager_RegisterUnregister(t *testing.T) {
	db := &MockDB{}
	pubsub := &MockPubSub{
		subscribeFunc: func(ctx context.Context, channel string) <-chan *redis.Message {
			return make(chan *redis.Message) // block forever
		},
	}
	m := NewManager(db, pubsub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	client := &Client{
		ID:       "user1",
		SendChan: make(chan []byte, 10),
	}

	m.Register <- client
	time.Sleep(50 * time.Millisecond) // Give run loop time to process

	if _, ok := m.Clients["user1"]; !ok {
		t.Errorf("Client was not registered")
	}

	m.Unregister <- client
	time.Sleep(50 * time.Millisecond) // Give run loop time to process

	if _, ok := m.Clients["user1"]; ok {
		t.Errorf("Client was not unregistered")
	}
}

func TestManager_Broadcast(t *testing.T) {
	savedChan := make(chan bool, 1)
	publishedChan := make(chan bool, 1)

	db := &MockDB{
		saveFunc: func(ctx context.Context, senderID, receiverID, channelID, content string) error {
			if senderID == "user1" && receiverID == "user2" && content == "hello" {
				savedChan <- true
			}
			return nil
		},
	}

	pubsub := &MockPubSub{
		subscribeFunc: func(ctx context.Context, channel string) <-chan *redis.Message {
			return make(chan *redis.Message) // block forever
		},
		publishFunc: func(ctx context.Context, channel string, payload string) error {
			if channel == "user:user2" {
				publishedChan <- true
			}
			return nil
		},
	}

	m := NewManager(db, pubsub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	msg := Message{
		Type:       "direct",
		SenderID:   "user1",
		ReceiverID: "user2",
		Content:    "hello",
	}

	m.Broadcast <- msg

	select {
	case <-savedChan:
	case <-time.After(time.Second):
		t.Errorf("Message was not saved to DB")
	}

	select {
	case <-publishedChan:
	case <-time.After(time.Second):
		t.Errorf("Message was not published to Redis")
	}
}

func TestManager_ListenToRedis(t *testing.T) {
	db := &MockDB{}
	msgChan := make(chan *redis.Message, 1)
	pubsub := &MockPubSub{
		subscribeFunc: func(ctx context.Context, channel string) <-chan *redis.Message {
			return msgChan
		},
	}

	m := NewManager(db, pubsub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	client := &Client{
		ID:       "user1",
		SendChan: make(chan []byte, 10),
	}
	m.Register <- client
	time.Sleep(50 * time.Millisecond)

	payload := `{"type":"direct","sender_id":"user2","receiver_id":"user1","content":"ping"}`
	msgChan <- &redis.Message{
		Channel: "user:user1",
		Payload: payload,
	}

	select {
	case msg := <-client.SendChan:
		if string(msg) != payload {
			t.Errorf("Expected payload %s, got %s", payload, string(msg))
		}
	case <-time.After(time.Second):
		t.Errorf("Message was not sent to client")
	}
}

func TestManager_ServeWS(t *testing.T) {
	db := &MockDB{}
	pubsub := &MockPubSub{
		subscribeFunc: func(ctx context.Context, channel string) <-chan *redis.Message {
			return make(chan *redis.Message)
		},
	}
	m := NewManager(db, pubsub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Run(ctx)

	s := httptest.NewServer(http.HandlerFunc(m.ServeWS))
	defer s.Close()

	// Missing user_id should fail
	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("could not make get request: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status Bad Request when missing user_id, got %v", resp.StatusCode)
	}

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "?user_id=test_user"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("could not connect to websocket: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	if _, ok := m.Clients["test_user"]; !ok {
		t.Errorf("Client test_user was not registered")
	}

	// Wait for ReadPump and WritePump to be active
	time.Sleep(50 * time.Millisecond)
}

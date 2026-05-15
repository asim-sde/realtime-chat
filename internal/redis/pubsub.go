package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type PubSub struct {
	client *redis.Client
}

func NewPubSub(ctx context.Context, redisURL string) (*PubSub, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		opts = &redis.Options{Addr: redisURL}
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &PubSub{client: client}, nil
}

func (p *PubSub) Close() {
	p.client.Close()
}

// Publish sends a message payload to a specific Redis channel
func (p *PubSub) Publish(ctx context.Context, channel string, payload string) error {
	return p.client.Publish(ctx, channel, payload).Err()
}

// Subscribe returns a Go channel that receives messages from the given Redis channel
func (p *PubSub) Subscribe(ctx context.Context, channel string) <-chan *redis.Message {
	pubsub := p.client.Subscribe(ctx, channel)
	return pubsub.Channel()
}

// UpdatePresence updates the heartbeat for a user
func (p *PubSub) UpdatePresence(ctx context.Context, userID string) error {
	now := float64(time.Now().Unix())
	return p.client.ZAdd(ctx, "presence", redis.Z{Score: now, Member: userID}).Err()
}

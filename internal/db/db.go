package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	pool *pgxpool.Pool
}

func NewDB(ctx context.Context, connectionString string) (*DB, error) {
	poolConfig, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &DB{pool: pool}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) SaveMessage(ctx context.Context, senderID, receiverID, channelID, content string) error {
	var rID, cID *string
	if receiverID != "" {
		rID = &receiverID
	}
	if channelID != "" {
		cID = &channelID
	}

	_, err := db.pool.Exec(ctx,
		"INSERT INTO messages (sender_id, receiver_id, channel_id, content) VALUES ($1, $2, $3, $4)",
		senderID, rID, cID, content,
	)
	if err != nil {
		return fmt.Errorf("failed to save message: %w", err)
	}
	return nil
}

package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/maang-prep/realtime-chat/internal/db"
	"github.com/maang-prep/realtime-chat/internal/redis"
	"github.com/maang-prep/realtime-chat/internal/ws"
)

func main() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://user:password@localhost:5432/chat_db?sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx := context.Background()

	database, err := db.NewDB(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to db: %v", err)
	}
	defer database.Close()

	pubsub, err := redis.NewPubSub(ctx, redisURL)
	if err != nil {
		log.Fatalf("Failed to connect to redis: %v", err)
	}
	defer pubsub.Close()

	manager := ws.NewManager(database, pubsub)
	go manager.Run(ctx)

	r := chi.NewRouter()
	r.Get("/ws", manager.ServeWS)

	log.Printf("Chat server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("listen: %s\n", err)
	}
}

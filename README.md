# Realtime Chat

Go + WebSockets + Redis Pub/Sub. Maps to HLD Chapter 12 (Design a Chat System).

## Problem

Design a realtime chat service supporting direct messages and group channels. Messages must be delivered instantly to online users and persisted for offline retrieval.

## Architecture

```
Client ↔ WebSocket Server (multiple instances)
                ↕
          Redis Pub/Sub (message routing)
                ↕
         PostgreSQL (message persistence)
```

<!-- Add actual architecture diagram here -->

## Deployment

```bash
docker-compose up -d
# Connect via WebSocket client to ws://localhost:8080/ws
```

## Scale to 1M Users

- WebSocket servers are stateful — use Redis Pub/Sub to route messages across server instances
- Connection mapping: user_id → server_id stored in Redis
- Message persistence: batch writes to PostgreSQL (append-only table, partitioned by date)
- Presence service: heartbeat every 30s, Redis sorted set with last-seen timestamps
- Group channels: fan-out via Redis Pub/Sub channels (one channel per group)
- Read receipts: separate lightweight table, eventual consistency acceptable
- Capacity: 1M concurrent connections across ~10 WebSocket servers, ~50K msgs/sec

## Status

- [ ] Project setup (Go module, Docker Compose)
- [ ] WebSocket connection handler
- [ ] Redis Pub/Sub message routing
- [ ] Direct message support
- [ ] Group channel support
- [ ] Message persistence (PostgreSQL)
- [ ] Online presence tracking
- [ ] Docker Compose deployment
- [ ] README with HLD diagram

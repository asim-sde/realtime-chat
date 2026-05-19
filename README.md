# Realtime Chat Service

A highly scalable, low-latency Realtime Chat architecture designed to support millions of concurrent TCP WebSocket connections spread across multiple application servers.

## Architecture & Tech Stack

- **Go (Golang) & gorilla/websocket:** Efficient management of thousands of concurrent WebSocket client connections per node with minimal memory footprint.
- **Redis (Pub/Sub & Sorted Sets):** 
  - **Message Broker:** Routes messages between different chat server instances. When User A (Node 1) messages User B (Node 2), Node 1 publishes the message to Redis, and Node 2 consumes it to deliver to User B.
  - **Presence:** Tracks online/offline status using Redis Sorted Sets as a distributed heartbeat registry.
- **PostgreSQL:** Durable, persistent storage for chat message history.

## Features

- **Fan-Out Architecture:** Perfect decoupling of client nodes. A message sent to a group channel is pushed to Redis Pub/Sub, and every server node listens and pushes down the wire only to its locally connected participants.
- **Connection Management:** Features independent Read/Write Goroutine pumps per client.
- **Direct & Group Messaging:** Extensible payload routing allows both 1-on-1 direct messages and multi-user group channels.
- **Testable Design:** Core manager abstractions decouple WebSocket business logic from infrastructure, allowing fully mocked automated test suites.

## Setup Instructions

### 1. Prerequisites
- Docker & Docker Compose
- Go 1.23+

### 2. Local Infrastructure setup
Launch PostgreSQL and Redis:
```bash
docker-compose up -d
```
*Note: The included `init.sql` will provision the `messages` table and all necessary indices automatically.*

### 3. Running the Server
Start the WebSocket manager application:
```bash
go run cmd/server/main.go
```

## Connecting as a Client

You can connect to the websocket server using any standard WebSocket client (e.g. wscat or a browser console).

**Connect:**
```
ws://localhost:8080/ws?user_id=alice
```

**Send a Message (JSON Payload):**
```json
{
  "type": "direct",
  "receiver_id": "bob",
  "content": "Hello Bob!"
}
```

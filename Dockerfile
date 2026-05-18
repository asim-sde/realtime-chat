FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o chat-server ./cmd/server

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/chat-server .
EXPOSE 8080
CMD ["./chat-server"]

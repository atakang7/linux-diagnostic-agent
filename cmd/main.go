package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"diagnostic-agent/internal/agent"
)

func main() {
	hostAddr := os.Getenv("HOST_ADDR")
	if hostAddr == "" {
		hostAddr = "localhost:8081"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a, err := agent.New(hostAddr, redisAddr)
	if err != nil {
		log.Fatalf("Failed to initialize agent: %v", err)
	}
	if err := a.Run(ctx); err != nil {
		log.Fatalf("Agent error: %v", err)
	}
}

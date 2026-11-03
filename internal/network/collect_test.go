package network

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestIsNetworkInterface(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"eth0", true},
		{"enp1s0", true},
		{"wlan0", true},
		{"lo", false},
		{"docker0", false},
		{"veth123", false},
		{"br-123", false},
		{"vmnet1", false},
		{"unknown0", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNetworkInterface(tc.name); got != tc.want {
				t.Fatalf("isNetworkInterface(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestProcessNextBatchReportsRedisFailure(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	collector := &Collector{redisClient: client}
	if err := collector.processNextBatch(context.Background()); err == nil || errors.Is(err, redis.Nil) {
		t.Fatalf("expected Redis error, got %v", err)
	}
}

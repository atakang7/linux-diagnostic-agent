//go:build integration

package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

func TestPacketToRedisToMetrics(t *testing.T) {
	collector, err := New("127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	defer collector.redisClient.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := collector.redisClient.Del(ctx, packetQueue).Err(); err != nil {
		t.Fatal(err)
	}

	ip := &layers.IPv4{
		Version: 4, IHL: 5, TTL: 64, Protocol: layers.IPProtocolTCP,
		SrcIP: net.ParseIP("10.1.1.1").To4(),
		DstIP: net.ParseIP("10.1.1.2").To4(),
	}
	tcp := &layers.TCP{SrcPort: 40300, DstPort: 443, SYN: true}
	if err := tcp.SetNetworkLayerForChecksum(ip); err != nil {
		t.Fatal(err)
	}
	buffer := gopacket.NewSerializeBuffer()
	if err := gopacket.SerializeLayers(buffer, gopacket.SerializeOptions{
		FixLengths: true, ComputeChecksums: true,
	}, ip, tcp, gopacket.Payload([]byte("test"))); err != nil {
		t.Fatal(err)
	}
	packet := gopacket.NewPacket(buffer.Bytes(), layers.LayerTypeIPv4, gopacket.Default)
	if err := collector.processPacket(ctx, packet); err != nil {
		t.Fatal(err)
	}
	if err := collector.processNextBatch(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case metrics := <-collector.Metrics():
		if len(metrics.Packets) != 1 {
			t.Fatalf("packet count = %d", len(metrics.Packets))
		}
		p := metrics.Packets[0]
		if p.SrcIP != "10.1.1.1" || p.DstIP != "10.1.1.2" || p.DstPort != 443 {
			t.Fatalf("invalid decoded packet: %+v", p)
		}
	case <-ctx.Done():
		t.Fatal("processed packet was not forwarded")
	}
	left, err := collector.redisClient.LLen(ctx, packetQueue).Result()
	if err != nil || left != 0 {
		t.Fatalf("packets left in input queue: %d; error: %v", left, err)
	}
}

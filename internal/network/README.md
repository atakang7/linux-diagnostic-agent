Part of [linux-diagnostic-agent](../../README.md).

# network

Captures IPv4 packets with libpcap and emits packet and interface telemetry.

## Run

```sh
go run ./cmd
```

Capture requires interface permissions and libpcap.

## Test

```sh
go test -race -count=1 ./internal/network
go test -race -tags=integration -count=1 ./internal/network
```

The integration test needs Redis on `127.0.0.1:6379`.

## Config

`New(redisAddr)` takes a Redis address. There are no package-specific environment variables.

## Interfaces

- Capture filter: `tcp or udp or icmp` on selected network interfaces.
- `Metrics()` returns packet details, connections and interface counters.
- Raw packet records pass through the [Redis queue](../../docs/reference/redis.md).

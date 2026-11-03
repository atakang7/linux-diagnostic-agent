Part of [linux-diagnostic-agent](../../README.md).

# agent

Coordinates collectors, remote log searches, and outbound TCP messages.

## Run

```sh
go run ./cmd
```

## Test

```sh
go test -race -tags=integration -count=1 ./internal/agent
```

Requires Redis on `127.0.0.1:6379`.

## Config

`New(hostAddr, redisAddr)` takes the two connection addresses; [cmd](../../cmd/README.md) sets their defaults.

## Interfaces

- `Run(ctx)` starts both collectors and processes commands until cancellation.
- `GetStats()` returns search and transport counters.
- The [wire protocol](../../docs/reference/protocol.md) defines command and event types.

Part of [linux-diagnostic-agent](../../README.md).

# tunnel

Owns the TCP connection, one reader, one writer and reconnect attempts.

## Run

```sh
go run ./cmd
```

## Test

```sh
go test -race -count=1 ./internal/tunnel
```

## Config

`New(hostAddr)` takes the receiver address from [cmd](../../cmd/README.md).

## Interfaces

- `Connect(ctx)` starts an outbound TCP connection with a 3-second dial timeout.
- `Send(message)` queues JSON with a 1,000-message memory limit; the write timeout is 5 seconds.
- `Commands()` exposes incoming JSON commands; `Close()` stops transport goroutines.
- Frames and command types are in the [protocol reference](../../docs/reference/protocol.md).

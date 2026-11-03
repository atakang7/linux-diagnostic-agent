Part of [linux-diagnostic-agent](../README.md).

# cmd

The Go entry point reads connection settings and starts the agent.

## Run

```sh
go run ./cmd
```

## Test

```sh
go test ./cmd
```

## Config

| Var | Default | What |
| --- | --- | --- |
| `HOST_ADDR` | `localhost:8081` | Diagnostic client TCP listener |
| `REDIS_ADDR` | `localhost:6379` | Redis address |

## Interfaces

- Handles `SIGINT` and `SIGTERM` by canceling the agent context.
- Sends and receives [protocol messages](../docs/reference/protocol.md) through the TCP client connection.

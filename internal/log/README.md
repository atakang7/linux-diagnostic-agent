Part of [linux-diagnostic-agent](../../README.md).

# log

Discovers local log files and processes keyword searches through Redis.

## Run

```sh
go run ./cmd
```

## Test

```sh
go test -race -tags=integration -count=1 ./internal/log
```

Requires Redis on `127.0.0.1:6379`.

## Config

| Var | Default | What |
| --- | --- | --- |
| `LOG_ROOTS` | `/var/log` | Colon-separated Linux directories to scan |

`New(redisAddr)` receives the Redis address from the entry point.

## Interfaces

- `Refresh(ctx)` inventories directories and `.log`, `.txt`, `.log.gz` files; symbolic links are skipped.
- `ProcessFiles(ctx, req)` reads paths from that inventory and publishes matches through `GetLogChannel()`.
- [Redis storage](../../docs/reference/redis.md) holds inventory and compressed search batches.

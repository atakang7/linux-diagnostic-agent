# Linux Diagnostic Agent

A Linux service that sends log search results and network observations to [linux-diagnostic-client](https://github.com/atakang7/linux-diagnostic-client). Redis buffers incoming packet data and log search batches while a reconnecting TCP connection carries messages to the client.

## Quick start

Requires Linux, Go 1.23.2 or later, Redis, and libpcap development headers. On Ubuntu or Debian:

```sh
git clone https://github.com/atakang7/linux-diagnostic-agent.git
cd linux-diagnostic-agent
sudo apt-get update
sudo apt-get install -y libpcap-dev redis-server
sudo systemctl start redis-server
make build
HOST_ADDR=localhost:8081 REDIS_ADDR=localhost:6379 ./bin/agent
```

The [diagnostic client](https://github.com/atakang7/linux-diagnostic-client) must listen on the address in `HOST_ADDR`. The agent retries its TCP connection if the client is offline. Capturing packets requires permission to open the selected network devices; running without it still permits log collection.

## Configuration

Copy `.env.example` to `.env` and set the following variables:

| Variable | Default | Description |
| --- | --- | --- |
| `HOST_ADDR` | `localhost:8081` | Diagnostic client's TCP listener |
| `REDIS_ADDR` | `localhost:6379` | Redis server |
| `LOG_ROOTS` | `/var/log` | Colon-separated paths to scan for logs |

The binary reads process environment variables. It does not load `.env` itself. To use the file locally:

```sh
cp .env.example .env
set -a
. ./.env
set +a
./bin/agent
```

## How it works

The agent inventories `.log`, `.txt`, and `.log.gz` files beneath `LOG_ROOTS`; symbolic links are skipped during discovery. A `log_search` request can name only files in that inventory. Search matches are batched in Redis, then forwarded as `log_data`. A `log_list` message supplies available paths.

The network collector captures IPv4 TCP, UDP, and ICMP packets from selected interfaces. It batches packet summaries from Redis into `metrics` messages. The transport uses newline-delimited JSON over TCP, reconnects automatically, and buffers up to 1,000 outgoing messages in memory.

## Usage and deployment

Start the diagnostic client listener and Redis, then run `./bin/agent` on the Linux host to observe. For a persistent installation, run the binary as a supervised systemd service with `HOST_ADDR`, `REDIS_ADDR`, and `LOG_ROOTS` set in its environment. Grant packet capture access only when needed.

A client can request a search with one JSON line:

```json
{"type":"log_search","payload":{"files":["/var/log/app.log"],"keywords":["ERROR"]}}
```

The path must appear in the agent's `log_list`. A search accepts 1–128 files and 1–16 keywords. The agent handles up to four files in parallel per search.

## Verification

```sh
make build
make vet
make test
```

With Redis listening on `127.0.0.1:6379`, run the end-to-end tests:

```sh
go test -race -tags=integration -count=1 ./...
```

GitHub Actions runs formatting, module checks, compilation, race-enabled tests, and the Redis/TCP integration suite on pull requests and `main`.

## Limits

- TCP transport has no TLS or authentication. Keep the client listener on a trusted, access-controlled network.
- Buffered outgoing messages are lost on agent restart. A disconnect during a write can cause duplicate delivery.
- Redis holds at most 10,000 raw packets in the input queue. Sustained traffic can drop packet data; processing removes queued packets before the transport confirms delivery.
- Log discovery runs every minute; inventory messages are sent every five minutes after startup. Logs may be unavailable to remote search until discovered.
- Searches are limited to 10 MiB per line. Packet capture depends on host permissions and supported interfaces.

## License

Apache-2.0. See [LICENSE](LICENSE).

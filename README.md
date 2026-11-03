# Linux Diagnostic Agent

Linux collector for log discovery, on-demand log searches, and basic network telemetry. Connects to the [diagnostic client](https://github.com/atakang7/linux-diagnostic-client) over TCP and uses Redis as a bounded processing buffer.

## Data flow

```text
Log roots -- discovery / search -- Redis batches --+
                                                     +-- Agent TCP transport --> Diagnostic client
Network interfaces -- libpcap -- Redis queue --------+
```

The TCP transport uses newline-delimited JSON, a single reader, automatic reconnects, and a bounded in-memory outbound queue.

## Run

Requires Go 1.23.2+, Redis, and Linux with libpcap development headers. Capturing packets from network interfaces requires appropriate OS permissions.

```sh
sudo apt-get install libpcap-dev
make build
REDIS_ADDR=127.0.0.1:6379 HOST_ADDR=127.0.0.1:8081 ./bin/agent
```

Environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HOST_ADDR` | `localhost:8081` | Diagnostic client TCP listener |
| `REDIS_ADDR` | `localhost:6379` | Redis connection |
| `LOG_ROOTS` | `/var/log` | Colon-separated directories to discover |

Discovered files include `.log`, `.txt`, and `.log.gz`. Log search commands may reference **only files already in the discovered inventory**. Symbolic links are excluded from discovery.

## Verify

```sh
make build
make vet
make test
# With Redis running on 127.0.0.1:6379:
go test -race -tags=integration -count=1 ./...
```

CI verifies binary compilation, static analysis, race-enabled tests, TCP framing and reconnects, Redis-backed log and packet processing, and an actual agent-to-test-receiver search.

## Protocol

The agent emits `log_list`, `log_data`, and `metrics` messages. A receiver can send `log_search` with a payload such as:

```json
{"type":"log_search","payload":{"files":["/var/log/app.log"],"keywords":["ERROR"]}}
```

Log entries include a `level` field used by the diagnostic client.

## Operational limits

- Transport is **plain TCP without authentication or TLS**. Run it only across trusted, access-controlled connections; do not expose its receiver publicly.
- Accepted outbound messages are buffered in memory and can be lost on process restart. Delivery across interrupted writes can duplicate messages; this is not an exactly-once system.
- The Redis packet queue is bounded (10,000 raw packets) and packet capture may require elevated privileges. Under sustained overload, input can be dropped.
- Initial log inventory refresh is synchronous; later inventory refreshes occur periodically. Redis must be reachable at startup.
- Redis integration tests simulate packet parsing and queues; they do not prove privileged capture on every Linux distribution.

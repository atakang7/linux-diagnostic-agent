# Linux Diagnostic Agent

Linux collector for log searches and IPv4 network metrics; it uses Redis for buffering and sends newline-delimited JSON to [linux-diagnostic-client](https://github.com/atakang7/linux-diagnostic-client) over TCP.

```text
+-------------------+        +---------------+
| network.Collector |        | log.Collector |
+-------------------+        +---------------+
         |                          |
       metrics                    log_data
         |                          |
         +-------------+------------+
                       |
                       v
                +-------------+ messages +---------------+ TCP +-------------------+
                | agent.Agent | --------> | tunnel.Tunnel | ---> | diagnostic-client |
                +-------------+          +---------------+      +-------------------+
```

## Run

Requires Linux, Go 1.23.2+, Redis, and libpcap headers; start the diagnostic client listener first.

```sh
git clone https://github.com/atakang7/linux-diagnostic-agent.git
cd linux-diagnostic-agent
sudo apt-get update
sudo apt-get install -y libpcap-dev redis-server
sudo systemctl start redis-server
make build
HOST_ADDR=localhost:8081 ./bin/agent
```

Packet capture needs permission to open network interfaces.

## Config

| Var | Default | What |
| --- | --- | --- |
| `HOST_ADDR` | `localhost:8081` | Diagnostic client TCP address |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `LOG_ROOTS` | `/var/log` | Colon-separated log directories |

`.env.example` lists these values; the binary reads environment variables, not `.env` files.

## Limits

- TCP has no authentication or TLS; use only trusted networks.
- Outbound queue holds 1,000 messages in memory; restarts lose them and interrupted writes can duplicate delivery.
- Redis keeps at most 10,000 queued raw packets; overload can drop data.
- Log discovery runs every minute; inventories are sent every five minutes.
- Searches accept 1-128 discovered files and 1-16 keywords; log lines are limited to 10 MiB.
- Packet capture requires interface permissions; discovery skips symlinks.

## Verify

```sh
make build
make vet
make test
go test -race -tags=integration -count=1 ./...
```

```text
+-----------+ trigger +------------------+ source +------------------+
| push / PR | ------> | actions/checkout | -----> | actions/setup-go |
+-----------+         +------------------+        +------------------+
                                                        |
                                                      checks
                                                        v
                                               +--------------------+
                                               | fmt mod build vet  |
                                               +--------------------+
                                                        |
                                                       tests
                                                        v
                                               +--------------------+
                                               | unit / Redis tests |
                                               +--------------------+
```

Integration tests require Redis at `127.0.0.1:6379`; CI starts it automatically.

Licensed under [Apache-2.0](LICENSE).

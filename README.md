# linux-diagnostic-agent

Go collector for Linux hosts that returns log search results and sends network telemetry to the [diagnostic client](https://github.com/atakang7/linux-diagnostic-client).

```text
+---------+  batches  +-------+   JSON   +--------+   TCP   +-------------------+
| log     | --------> | agent | --------> | tunnel | -------> | diagnostic-client |
+---------+           +-------+           +--------+         +-------------------+
     |                   ^
     | search data       | metrics
     v                   |
+-------+             +---------+
| Redis | <-----------| network |
+-------+   packets   +---------+
```

## Quick start

Linux, Go 1.23.2 or later, and a diagnostic client TCP listener on `127.0.0.1:8081` are required.

```sh
sudo apt-get install -y libpcap-dev redis-server
sudo systemctl start redis-server
make build
HOST_ADDR=127.0.0.1:8081 ./bin/agent
```

## Layout

| Path | What it is |
| --- | --- |
| [cmd/](cmd/README.md) | Process entry point and environment |
| [internal/agent/](internal/agent/README.md) | Collector coordination and command handling |
| [internal/log/](internal/log/README.md) | Log discovery and search |
| [internal/network/](internal/network/README.md) | Packet capture and telemetry |
| [internal/tunnel/](internal/tunnel/README.md) | TCP connection and outgoing queue |

## Docs

[Documentation index](docs/README.md): search instructions, TCP messages, Redis storage, and architecture.

## Limits

- The TCP connection has no TLS or authentication. Restrict access to the receiver.
- The outgoing queue is in memory. Restarts lose buffered messages and interrupted writes can cause duplicates.
- The Redis packet queue retains at most 10,000 records; overload discards older packets.
- Packet capture needs interface permissions. Integration tests use generated packets.

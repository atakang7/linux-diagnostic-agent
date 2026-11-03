# TCP protocol

The agent connects to the diagnostic client's TCP listener. Frames are UTF-8 JSON objects separated by newline characters. The agent caps incoming frames at 1 MiB.

## Inbound command

| Type | Payload | Limits |
| --- | --- | --- |
| `log_search` | `files`: path array, `keywords`: string array | 1 to 128 files, 1 to 16 keywords |

```json
{"type":"log_search","payload":{"files":["/var/log/app.log"],"keywords":["ERROR"]}}
```

Paths must appear in the agent's `log_list` inventory and refer to regular log files.

## Outbound events

| Type | Payload |
| --- | --- |
| `log_list` | Array of log file or directory objects |
| `log_data` | Array of matching log entry objects |
| `metrics` | Packet, connection and interface telemetry |

| Payload | Fields |
| --- | --- |
| `log_list` entry | `path`, `parent_path`, `name`, `is_directory`, `size`, `mod_time`, `is_gzipped` |
| `log_data` entry | `filename`, `line`, `line_num`, `timestamp`, `notification_type`, `level` |
| `metrics` | `timestamp`, `packets`, `connections`, `interface_stats` |

The TCP connection has no authentication or TLS. Writes can be duplicated after reconnection.

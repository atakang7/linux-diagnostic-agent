# Redis storage

Both collectors use database 0 at the `REDIS_ADDR` endpoint.

| Name | Type | Content | Lifetime |
| --- | --- | --- | --- |
| `log_files` | String | Gzip-compressed JSON inventory | 24 hours; updated during discovery |
| `search:<timestamp>:meta` | Hash | Search status, selected files, keywords and errors | 24 hours |
| `search:<timestamp>:results` | List | Gzip-compressed JSON log batches | 24 hours |
| `network:packets:queue` | List | JSON packet records | Last 10,000 records |

The network collector adds records with `LPUSH` and takes batches of up to 1,000 with `LPOP`, so the newest records are read first.

Log search batches contain at most 1,000 entries. The consumer sends one batch per second.

# Architecture

The [entry point](../../cmd/README.md) creates an agent with a log collector, network collector, and outbound TCP tunnel.

## Collection

Log searches are requested by the receiver. The collector first inventories allowed paths, then checks each search against that inventory. This boundary prevents the receiver from choosing arbitrary local files.

Network capture is continuous on selected interfaces. The collector uses a bounded [Redis list](../reference/redis.md) between packet capture and metrics delivery; a full list discards older records.

## Processing

Log matches are compressed in Redis before delivery. File workers are limited to four per search, and the consumer meters outgoing batches to avoid sending a complete large search at once.

## Transport

One tunnel owns both socket directions. Its memory queue absorbs temporary disconnects and reconnects with backoff. Accepted messages can be lost on process exit or replayed after an interrupted write.

The [protocol](../reference/protocol.md) is plain TCP, so the diagnostic client listener must be reachable only on a trusted network.

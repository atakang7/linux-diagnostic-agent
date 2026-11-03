# Search a discovered log file

1. Start Redis, the diagnostic client listener, and the agent using the [quick start](../../README.md#quick-start).
2. Read the agent's `log_list` event in the diagnostic client.
3. Select a file path from the event. Directories cannot be searched.
4. Send a `log_search` command with that path in `files` and a search term in `keywords`, using the [command format](../reference/protocol.md#inbound-command).
5. Read the returned `log_data` batches from the same TCP connection.


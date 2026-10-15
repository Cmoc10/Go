# Replicated key/value store

This is a small, beginner-friendly Go example of a replicated key/value
service. It deliberately uses only the standard library.

## Run it

```text
go run . -addr :8080 -wal kv.wal
```

The HTTP API uses `/kv/{key}`:

```text
PUT    /kv/name       (request body is the value)
GET    /kv/name
DELETE /kv/name
```

The store protects its map with `sync.RWMutex`. Every write is first appended
as one JSON record to the write-ahead log and flushed with `fsync` (`File.Sync`)
before the in-memory map is changed. Opening a store replays the log.

`ReplicatedStore` is a deliberately simple leader abstraction. A
`ReplicatedStore` forwards each committed record to registered `Replica`
implementations. `Follower` persists and applies records in order; registering
a follower replays all records it is missing, providing catch-up after a
temporary disconnect. This example does not implement elections, networking
between nodes, or quorum acknowledgements.

Run tests with:

```text
go test ./...
```

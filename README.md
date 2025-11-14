# C4 Object Storage

Distributed object storage system with consistent hashing, replication, and dynamic migration.

## Features

- **Distributed Architecture**: Master node with multiple storage nodes
- **Consistent Hashing**: Efficient key distribution with virtual nodes
- **Data Replication**: Configurable replication factor (N replicas)
  - Primary-Backup Synchronous replication strategy
  - Automatic failover on read operations
  - Version tracking for consistency
- **Dynamic Migration**: Automatic data rebalancing when nodes join/leave
- **gRPC API**: High-performance communication between components
- **Chunk-based Storage**: Efficient handling of large objects

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Master Node  │ ◄─── Consistent Hash Ring + Replication
└──────┬──────┘
       │
       ├───────┬───────┬───────┐
       ▼       ▼       ▼       ▼
   ┌────┐  ┌────┐  ┌────┐  ┌────┐
   │ N1 │  │ N2 │  │ N3 │  │ N4 │  Storage Nodes
   └────┘  └────┘  └────┘  └────┘
   (replicas distributed across nodes)
```

## Documentation

- [Replication Design & Implementation](./REPLICATION.md)
- [Integration Tests](./REPLICATION_TESTS.md)
- [Recent Improvements](./IMPROVEMENTS.md)
- [Migration Demo](./migration_demo.md)
- [Summary](./SUMMARY.md)

## Quick Start

### Building

```bash
cargo build --release
```

### Running Storage Nodes

```bash
# Start first storage node
cargo run --bin db -- --port 50051 --data-path ./data1

# Start second storage node
cargo run --bin db -- --port 50052 --data-path ./data2

# Start third storage node
cargo run --bin db -- --port 50053 --data-path ./data3
```

### Running Master Node

```bash
# With replication factor 2 (each object stored on 2 nodes)
cargo run --bin master -- \
  --port 8080 \
  --storage-nodes "http://localhost:50051,http://localhost:50052,http://localhost:50053" \
  --replication-factor 2
```

## Testing

### Unit Tests

```bash
cargo test
```

### Integration Tests (Replication)

```bash
cargo test --test replication_tests -- --test-threads=1
```

See [REPLICATION_TESTS.md](./REPLICATION_TESTS.md) for detailed test descriptions.

## Configuration

### Master Node Options

- `--port`: HTTP API port (default: 8080)
- `--storage-nodes`: Comma-separated list of storage node URLs
- `--replication-factor`: Number of replicas for each object (default: 1)
- `--virtual-nodes`: Number of virtual nodes per physical node (default: 150)

### Storage Node Options

- `--port`: gRPC port (default: 50051)
- `--data-path`: Directory for storing data (default: ./data)

## Project Structure

```
c4/
├── master/          # Master node (coordinator)
│   ├── hashing/     # Consistent hash ring implementation
│   └── migration/   # Data migration logic
├── db/              # Storage node implementation
│   ├── storage/     # Chunk-based storage engine
│   └── api/         # gRPC API implementation
├── grpc-server/     # Protobuf definitions
└── encoder/         # Data encoding utilities
```

## License

MIT

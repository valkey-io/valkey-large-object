# valkey-large-object

A Valkey module for storing large objects (KV cache tensors, embeddings, blobs) with a tiered DRAM + NVMe architecture, io_uring zero-copy I/O, and optional EFA RDMA transport to GPU memory.

## Architecture

Two operating modes:

- **Dram** (default) — All objects live in a DRAMPool backed by pre-allocated segments with a talc arena allocator. Fastest reads. No NVMe.
- **Tiered** — Objects persist on NVMe files. DRAMPool acts as a read cache: objects are promoted on repeat access (`promote-min-hits`) and cold copies are dropped by LFU score when the pool is full. io_uring ReadFixed/WriteFixed with O_DIRECT for zero-copy NVMe I/O.

Storage is organized as segments (contiguous memory regions) managed by pool allocators:
- **DRAMPool** — Long-lived object cache. Segment memory registered with both io_uring and EFA.
- **NVMePool** — Transient staging buffer for NVMe reads/writes (Tiered mode only).
- **FdPool** — Cached file descriptors for NVMe object files (Tiered mode only).

## Commands

| Command | Description |
|---------|-------------|
| `BLOB.TCP_SET key <data>` | Store object (TCP). Data length is implicit. |
| `BLOB.RDMA_SET key total_len rkey1 addr1 len1 ...` | Store object (RDMA). Server reads `total_len` bytes from the client's memory addresses via RDMA. |
| `BLOB.TCP_GET key` | Retrieve object over TCP. Returns a bulk string. |
| `BLOB.RDMA_GET key rkey1 addr1 len1 ...` | Retrieve object by DMA into the client's memory addresses. Replies `[obj_len, crc32c]`. |
| `BLOB.RDMA_HELLO client_efa_addr_hex` | Establish RDMA session for GPU-direct DMA transfers. |
| `BLOB.INFO key [LEN\|CRC\|TIER]` | Object metadata. No transport involved. |
| `DEL key` | Native Valkey DEL. Triggers module free callback (cleans up NVMe file + pool buffers). |

## Build

```bash
cargo build --release
# Output: target/release/libvalkey_large_object.so
```

## Run

### Dram mode (default)
```bash
valkey-server --port 7380 \
    --loadmodule ./target/release/libvalkey_large_object.so \
        operating-mode Dram \
        dram-maxmemory 1gb \
        segment-size 64mb
```

### Tiered mode (DRAM cache + NVMe persistence)
```bash
valkey-server --port 7380 \
    --loadmodule ./target/release/libvalkey_large_object.so \
        operating-mode Tiered \
        disk-dir /mnt/nvme-data \
        dram-maxmemory 1gb \
        segment-size 64mb \
        disk-maxmemory 10gb \
        disk-staging-size 64mb
```

## Configuration

| Parameter | Default | Mutable | Description |
|-----------|---------|---------|-------------|
| `operating-mode` | `Dram` | Immutable | `Dram` (DRAM-only) or `Tiered` (DRAM cache + NVMe). |
| `disk-dir` | (empty) | Immutable | Directory for NVMe object files. Required in Tiered mode. Must support O_DIRECT. |
| `segment-size` | 1gb | Immutable | Size of each DRAMPool segment. Min 1mb, max 1gb. |
| `disk-maxmemory` | 0 (unlimited) | Yes | Max NVMe disk usage. 0 = unlimited. |
| `disk-staging-size` | 1gb | Immutable | Size of NVMe staging buffer. Min 1mb. |
| `max-object-size` | 512mb | Yes | Maximum allowed object size for BLOB.TCP_SET / BLOB.RDMA_SET. Min 1 byte. |
| `max-promote-size` | 256mb | Yes | Max object size for NVMe→DRAM promotion. 0 = disable promotion. |
| `chunk-size` | 8mb | Immutable | Chunk size for multi-buffer streaming I/O. Min 4kb, max 256mb. |
| `max-buffers-per-op` | 8 | Yes | Max buffers per streaming operation. Min 2, max 64. |
| `min-buffers-per-op` | 2 | Yes | Min buffers to start a streaming operation. Min 1, max 64. |
| `scaling-poll-ms` | 2500 | Yes | Scaling cron poll interval in milliseconds. Min 1000, max 60000. |
| `scaling-expand-watermark` | 80 | Yes | Percent utilization to trigger proactive segment expansion. Min 50, max 95. |
| `scaling-shrink-watermark` | 90 | Yes | Percent used_memory/maxmemory to trigger DRAM segment eviction. Min 50, max 95. |
| `reaper-poll-ms` | 100 | Yes | Scaling cron poll interval in milliseconds while evicted keys wait to be deleted. Min 10, max 60000. |
| `reaper-scan-budget-us` | 1000 | Yes | Microseconds per tick for deleting evicted keys. Min 100, max 100000. |
| `victim-sample-size` | 5 | Yes | Entries sampled per victim search (eviction, DRAM cache drop, fd close); the lowest LFU score goes. Min 1, max 64. |
| `promote-min-hits` | 2 | Yes | Tiered: misses an object needs before a GET promotes it to DRAM. 1 = promote on first GET. Min 1, max 255. |
| `blob-decay-time` | 1 | Yes | Minutes per one-point decay of the LFU score used to rank DRAM objects and fds for eviction. 0 = no decay. Max 65535. |
| `max-cached-fds` | 16384 | Yes | Tiered: cap on cached read fds. 1..1048576. A raise that does not fit above Valkey's client range is rejected. |
| `smartlog-poll-secs` | 60 | Immutable | NVMe SMART poll interval in seconds. 0 = disabled. Max 86400. |
| `worker-threads` | 2 | Immutable | Tokio worker threads for async I/O tasks. Min 1, max 32. |
| `direct-io` | yes | Immutable | Use O_DIRECT for NVMe files. Disable for ASAN builds. |
| `bench-mode` | no | Immutable | BLOB.TCP_GET returns integer size instead of bulk data (isolates NVMe throughput). Hidden. |
| `fabric-provider` | `EfaDirect` | Immutable | libfabric provider for the DMA path: `Emulated` (libfabric over TCP, runs anywhere) or `EfaDirect` (EFA hardware RDMA). |
| `fabric-interfaces` | (empty) | Immutable | Comma-separated fabric domains to serve on. Empty = every domain the provider discovers. |
| `fabric-max-in-flight` | 0 | Immutable | Transfers each fabric service keeps in flight. 0 = provider default. Max 65536. |
| `fabric-crc-pool-threads` | 1 | Immutable | Threads hashing checksummed transfers. Min 1, max 1024. |

All size parameters accept memory notation (`64mb`, `1gb`, etc.).

## Test

```bash
# Full build + test pipeline (fmt, clippy, unit tests, integration tests)
./build.sh

# Build only (no valkey-server needed)
./build.sh build

# Integration tests only (assumes built)
./build.sh integ-test

# Specific test
TEST_PATTERN=test_set_get_roundtrip ./build.sh test
```

## Benchmark

```bash
# Dram only
./bench.sh --port 7380

# All modes on NVMe stripe
./bench.sh --port 7380 --disk-dir /mnt/bigobj-data/bench-test
```

See [BENCHMARK.md](BENCHMARK.md) for full documentation, parameters, and troubleshooting.

## License

BSD-3-Clause

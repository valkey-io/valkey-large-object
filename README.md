# ValkeyLargeObj

A Valkey module for storing large objects (KV cache tensors, embeddings, blobs) with a tiered DRAM + NVMe architecture, io_uring zero-copy I/O, and optional EFA RDMA transport to GPU memory.

## Architecture

Two operating modes:

- **Dram** (default) — All objects live in a DRAMPool backed by pre-allocated segments with a talc arena allocator. Fastest reads. No NVMe.
- **Tiered** — Objects persist on NVMe files. DRAMPool acts as a read cache: objects are promoted on repeat access (`promote-min-hits`) and cold copies are evicted by an LFU score when the pool is full. io_uring ReadFixed/WriteFixed with O_DIRECT for zero-copy NVMe I/O.

Storage is organized as segments (contiguous memory regions) managed by pool allocators:
- **DRAMPool** — Long-lived object cache. Segment memory registered with both io_uring and EFA.
- **NVMePool** — Transient staging buffer for NVMe reads/writes (Tiered mode only).
- **FdPool** — Cached file descriptors for NVMe object files (Tiered mode only).

## Commands

| Command | Description |
|---------|-------------|
| `LO.SET key <data>` | Store object (TCP). Data length is implicit. |
| `LO.SET key len rkey remote_addr` | Store object (EFA). Server reads `len` bytes from client GPU via RDMA. |
| `LO.GET key` | Retrieve object. Returns bulk string (TCP) or DMA to client GPU (EFA). |
| `LO.HELLO` | Establish EFA/RDMA session for GPU-direct DMA transfers. |
| `DEL key` | Native Valkey DEL. Triggers module free callback (cleans up NVMe file + pool buffers). |

## Build

```bash
cargo build --release
# Output: target/release/libvalkey_largeobj.so
```

## Run

### Dram mode (default)
```bash
valkey-server --port 7380 \
    --loadmodule ./target/release/libvalkey_largeobj.so \
        operating-mode Dram \
        dram-maxmemory 1gb \
        segment-size 64mb
```

### Tiered mode (DRAM cache + NVMe persistence)
```bash
valkey-server --port 7380 \
    --loadmodule ./target/release/libvalkey_largeobj.so \
        operating-mode Tiered \
        nvme-dir /mnt/nvme-data \
        dram-maxmemory 1gb \
        segment-size 64mb \
        nvme-maxmemory 10gb \
        nvme-staging-size 64mb
```

## Configuration

| Parameter | Default | Mutable | Description |
|-----------|---------|---------|-------------|
| `operating-mode` | `Dram` | Immutable | `Dram` (DRAM-only) or `Tiered` (DRAM cache + NVMe). |
| `nvme-dir` | (empty) | Immutable | Directory for NVMe object files. Required in Tiered mode. Must support O_DIRECT. |
| `dram-maxmemory` | 0 (unlimited) | Yes | Total DRAM budget. 0 = grow on demand (one segment at a time). |
| `segment-size` | 64mb | Immutable | Size of each DRAMPool segment. Min 1mb. |
| `nvme-maxmemory` | 10gb | Yes | Max NVMe disk usage. Min 1mb. |
| `nvme-staging-size` | 64mb | Immutable | Size of NVMe staging buffer (1 segment). Min 1mb. |
| `max-promote-size` | 256mb | Yes | Max object size for NVMe→DRAM promotion. 0 = disable promotion. |
| `promote-min-hits` | 2 | Yes | Tiered: misses an object needs before a GET promotes it to DRAM. 1 = promote on first GET. Range 1-255. |
| `lfu-decay-time` | 1 | Yes | Tiered: minutes per one-point decay of the LFU score used for DRAM and fd eviction. 0 = no decay. |
| `evict-sample-size` | 5 | Yes | Tiered: cached entries sampled per eviction; the lowest LFU score goes. Range 1-64. |
| `max-open-fds` | 1024 | Yes | Tiered: cap on cached read fds. 0 = unlimited. |
| `worker-threads` | 2 | Immutable | Tokio worker threads for async I/O tasks. |
| `bench-mode` | no | Yes | LO.GET returns integer size instead of bulk data (isolates NVMe throughput). |
| `direct-io` | yes | Immutable | Use O_DIRECT for NVMe files. Disable for ASAN builds. |
| `fabric-provider` | `Emulated` | Immutable | libfabric provider for the DMA path: `Emulated` (libfabric over TCP, runs anywhere) or `EfaDirect` (EFA hardware RDMA). |
| `fabric-interfaces` | (empty) | Immutable | Comma-separated fabric domains to serve on. Empty = every domain the provider discovers. |
| `fabric-max-in-flight` | 0 | Immutable | Transfers each fabric service keeps in flight. 0 = provider-derived default. |
| `fabric-crc-pool-threads` | 1 | Immutable | Threads hashing checksummed transfers off the fabric workers. |

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
TEST_PATTERN=test_lo_set_get_roundtrip ./build.sh test
```

## Benchmark

```bash
# Dram only
./bench.sh --port 7380

# All modes on NVMe stripe
./bench.sh --port 7380 --nvme-dir /mnt/bigobj-data/bench-test
```

See [BENCHMARK.md](BENCHMARK.md) for full documentation, parameters, and troubleshooting.

## License

BSD-3-Clause

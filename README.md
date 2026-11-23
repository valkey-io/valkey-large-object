# valkey-large-object

A Valkey module that stores large binary values (for example model data or cache blocks for machine learning inference) in DRAM or in files on a local NVMe disk.
Clients send and get the values over the normal Valkey connection, or with RDMA directly to and from registered client memory.
The module does not keep data across a restart, in either mode (see [No persistence](docs/GETTING_STARTED.md#no-persistence)).

> **Note:** valkey-large-object is at version 0.x.
> Commands, arguments, replies, and configuration can change before version 1.0.

## Getting started

- [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md): requirements, build, load the module, first commands, and behavior to know (no persistence, transactions, eviction).
- [docs/COMMANDS.md](docs/COMMANDS.md): command reference, the RDMA flow, and the configurations that you are most likely to change.
  This repository has no RDMA client library; a client must implement the flow in [RDMA overview](docs/COMMANDS.md#rdma-overview) with libfabric.

## Architecture

Two operating modes:

- **Dram** (default) — All objects live in the DRAM pool (DRAMPool) of pre-allocated segments. No NVMe.
- **Tiered** — Each object is stored in a file on NVMe. Object files are not kept across a restart. The DRAM pool acts as a read cache: an object is promoted (copied into DRAM) when it is read; `largeobj.promote-min-hits` (default 1) sets on which read. When the pool is full, cached copies with the lowest least-frequently-used (LFU) score are dropped; the objects stay on disk. Disk I/O uses io_uring with O_DIRECT.

Storage is organized as segments (large blocks of memory that the module reserves at one time) managed by pool allocators:
- **DRAMPool** — Long-lived object cache. Segment memory is registered with the fabric (and with io_uring in Tiered mode).
- **NVMePool** — Transient staging buffer for NVMe reads/writes (Tiered mode only).
- **FdPool** — Cached file descriptors for NVMe object files (Tiered mode only).

## Commands

| Command | Description |
|---------|-------------|
| `BLOB.TCP_SET key value` | Store object (TCP). The value length is implicit. |
| `BLOB.RDMA_SET key total-len [CRC crc32c] rkey addr len [rkey addr len ...]` | Store object (RDMA). Server reads `total-len` bytes from the client's memory addresses via RDMA. Optional CRC for end-to-end verification. |
| `BLOB.TCP_GET key` | Retrieve object over TCP. Returns a bulk string. |
| `BLOB.RDMA_GET key rkey addr len [rkey addr len ...]` | Retrieve object by DMA into the client's memory addresses. Replies `[obj_len, crc32c]`. |
| `BLOB.RDMA_HELLO client-rdma-address` | Open an RDMA session on the connection. |
| `BLOB.INFO key [LEN\|CRC\|TIER]` | Object metadata. No transport involved. |
| `DEL key` | Native Valkey DEL. Triggers module free callback (cleans up NVMe file + pool buffers). |

The data commands are blocking commands, so they cannot run in transactions, scripts, functions, or module calls. See [docs/COMMANDS.md](docs/COMMANDS.md) for syntax, replies, and errors.

## Build

```bash
cargo build --release
# Output: target/release/libvalkey_large_object.so
```

The build needs libfabric 2.0 or later with development files, found with `pkg-config`, and `libclang-dev`. See [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md#requirements).

## Run

Set the configurations with their full `largeobj.` names after `--loadmodule`, as config directives after the `loadmodule` line in `valkey.conf`, or with `MODULE LOADEX` (see [Configuration](docs/COMMANDS.md#configuration)). Memory sizes can have units, such as `64mb`. The DRAM limit is the core Valkey `maxmemory` setting: the DRAM pool grows one segment at a time up to it. When a fabric is up, `largeobj.max-rdma-registered-bytes` also limits the DRAM pool, and the maximum number of segments limits it too (see `largeobj.segment-size` in [Configuration](#configuration)). With `maxmemory-policy allkeys-lru`, as in the examples, a write to a full module evicts large objects and does not fail with `OOM`.

Get Valkey from [valkey.io/download](https://valkey.io/download/). If libfabric is not in a default library path, set `LD_LIBRARY_PATH` to its `lib` directory.

### Dram mode (default)
```bash
valkey-server --port 6379 \
    --maxmemory 4gb \
    --maxmemory-policy allkeys-lru \
    --loadmodule ./target/release/libvalkey_large_object.so \
    --largeobj.operating-mode Dram \
    --largeobj.segment-size 64mb \
    --largeobj.max-object-size 32mb
```

### Tiered mode (DRAM cache + NVMe)

> **Note:** In Tiered mode, the module only uses a `largeobj.disk-dir` that is new or empty, or that it used before. It writes a `.valkey-large-object` file in it, and does not load if the directory has other files but not that one, so it never deletes data it did not write. Set `largeobj.disk-dir` to a directory that only this module uses (for example `/mnt/nvme/largeobj`).

```bash
valkey-server --port 6379 \
    --maxmemory 4gb \
    --maxmemory-policy allkeys-lru \
    --loadmodule ./target/release/libvalkey_large_object.so \
    --largeobj.operating-mode Tiered \
    --largeobj.disk-dir /mnt/nvme/largeobj \
    --largeobj.segment-size 64mb \
    --largeobj.max-object-size 32mb \
    --largeobj.max-promote-size 32mb \
    --largeobj.disk-maxmemory 10gb \
    --largeobj.disk-staging-size 64mb
```

`largeobj.disk-dir` must support `O_DIRECT` and user extended attributes. XFS is the only supported file system; other file systems, for example ext4, are not supported.

## Naming conventions

The codebase uses implementation-specific names internally and hardware-neutral names on all client-facing surfaces (error messages, INFO fields, config parameters, command names).

| Internal (code) | Client-facing | Notes |
|---|---|---|
| **EFA** (Elastic Fabric Adapter) | **RDMA** | Commands use `BLOB.RDMA_*`. Error strings say "rdma address". Code uses EFA in type/function names and comments. |
| **NVMe** | **disk** | Config uses `largeobj.disk-dir`, `largeobj.disk-maxmemory`. Code uses NVMe in module names (`nvme.rs`) and internal functions. |
| **NVMe staging pool** | **disk_staging** | INFO fields prefixed `disk_staging_*`, config `largeobj.disk-staging-size`. Refers to the DRAM buffers used for disk I/O, not the disk itself. |
| **address** (unqualified) | **rdma address** or **memory address** | "rdma address" = the client's RDMA fabric endpoint (from `BLOB.RDMA_HELLO`). "memory address" = a client-side buffer described by an `(rkey, addr, len)` triple. Never use bare "address" in client-facing strings. |

When adding new code: use EFA/NVMe freely in Rust identifiers, doc comments, and server log messages. Any string returned to a client must use the client-facing term.

## Configuration

| Parameter | Default | Mutable | Description |
|-----------|---------|---------|-------------|
| `largeobj.operating-mode` | `Dram` | No | `Dram` (DRAM-only) or `Tiered` (DRAM cache + NVMe). |
| `largeobj.disk-dir` | (empty) | No | Dedicated directory for NVMe object files, on XFS (the only supported file system). Required in Tiered mode. Must support O_DIRECT. Must be writable. **Must be new or empty on first use: see the [note](#tiered-mode-dram-cache--nvme).** |
| `largeobj.disk-dir-shards` | 1024 | No | Tiered: subdirectories of `largeobj.disk-dir` that object files are spread over, by object ID, so creates and unlinks do not all serialize on one directory lock. Min 1, max 65536. |
| `largeobj.segment-size` | 1GB | No | Size of each DRAM pool segment and disk staging segment. 1MB to 1GB. Must be at least twice `largeobj.chunk-size`. At most 16384 segments per pool (see [Configuration](docs/COMMANDS.md#configuration)). |
| `largeobj.disk-maxmemory` | 0 (unlimited) | Yes | Max NVMe disk usage. Each file counts as its length rounded up to 4KB, or, on a striped XFS, to the stripe unit for files of at least one stripe unit. On XFS, each file also counts its inode. A file also counts any metadata blocks outside its inode (`largeobj_disk_metadata_spills`). 0 = unlimited. |
| `largeobj.disk-staging-size` | 1GB | No | Total size of the DRAM staging buffers for NVMe I/O. Min 1MB. |
| `largeobj.max-object-size` | 512MB | Yes | Maximum allowed object size for BLOB.TCP_SET / BLOB.RDMA_SET. Min 1 byte. Dram: an object must fit in one segment. BLOB.TCP_SET is also limited by the core `proto-max-bulk-len` (default 512MB). |
| `largeobj.max-promote-size` | 256MB | Yes | Max object size for NVMe→DRAM promotion. Must fit in one segment, with the same overhead as `largeobj.max-object-size` (with 64MB segments, at most 66842624). 0 = disable promotion. |
| `largeobj.chunk-size` | 1MB | No | Chunk size for multi-buffer streaming I/O. 4KB to 256MB, a multiple of 4KB. |
| `largeobj.max-buffers-per-op` | 64 | Yes | Max buffers per streaming operation. Min 2, max 1024. |
| `largeobj.min-buffers-per-op` | 2 | Yes | Min buffers to start a streaming operation. Min 1, max 1024. |
| `largeobj.scaling-poll-ms` | 2500 | Yes | Scaling cron poll interval in milliseconds. Min 1000, max 60000. |
| `largeobj.scaling-expand-watermark` | 80 | Yes | Percent utilization to trigger proactive segment expansion. Min 50, max 95. |
| `largeobj.scaling-shrink-watermark` | 90 | Yes | Percent of `maxmemory` above which the DRAM pool does not grow. Tiered: the pool also releases segments above it. Min 50, max 95. |
| `largeobj.max-rdma-registered-bytes` | 380GB | Yes | With a fabric up: cap on segment bytes registered with EFA (DRAM pool plus disk staging). An expand past it is refused like a `maxmemory` one. 0 = no limit. |
| `largeobj.reaper-poll-ms` | 25 | Yes | Scaling cron poll interval in milliseconds while evicted keys wait to be deleted. Min 10, max 60000. |
| `largeobj.reaper-scan-budget-us` | 2000 | Yes | Microseconds per tick for deleting evicted keys; one uninterruptible event-loop stall, and over `largeobj.reaper-poll-ms` the share of a core reclaim gets. Min 100, max 100000. |
| `largeobj.victim-sample-size` | 5 | Yes | Entries sampled per victim search (DRAM cache drop, fd close); the lowest LFU score goes. Key eviction does not sample: it is exact LRU. Min 1, max 64. |
| `largeobj.promote-min-hits` | 1 | Yes | Tiered: number of disk reads of an object; the read that reaches this number promotes it to DRAM. 1 = promote on the first read. Min 1, max 255. |
| `largeobj.lfu-decay-minutes` | 1 | Yes | Minutes per one-point decay of the least-frequently-used (LFU) score used to rank DRAM cached copies and fds for dropping. 0 = no decay. Max 65535. |
| `largeobj.max-cached-fds` | 16384 | Yes | Tiered: how many read fds to keep cached. 0..1048576; 0 caches nothing. The fd cache max entries is the smaller of this and the fd limit minus `2 x maxclients + 128`; a larger value is clamped with a warning. |
| `largeobj.fd-reserve` | Lenient | No | Tiered: what happens when the open-file limit is below `2 x maxclients + 128` (module start plus the command reserve). `Lenient` loads with object-file fd placement and the fd cache off; `Strict` also needs `largeobj.max-cached-fds` to fit (`2 x maxclients + 128 + largeobj.max-cached-fds`): it refuses to load otherwise, rejects a `CONFIG SET largeobj.max-cached-fds` that does not fit, and aborts the server on a `CONFIG SET maxclients` that breaks the reserve or squeezes the cache. See `docs/OBJECT_FD_PLACEMENT_DESIGN.md`. |
| `largeobj.smartlog-poll-secs` | 60 | No | Poll interval in seconds for the NVMe SMART health log (`largeobj_smartlog_*` INFO fields). 0 = disabled. Max 86400. |
| `largeobj.worker-threads` | 8 | No | Worker threads for asynchronous I/O tasks. Min 1, max 32. |
| `largeobj.direct-io` | yes | No | Use O_DIRECT for NVMe files. Disable for ASAN builds. |
| `largeobj.bench-mode` | no | No | BLOB.TCP_GET returns integer size instead of bulk data (isolates NVMe throughput). Hidden. |
| `largeobj.fabric-provider` | `EfaDirect` | No | libfabric provider for the RDMA path: `Emulated` (libfabric over TCP, runs anywhere) or `EfaDirect` (EFA hardware RDMA). |
| `largeobj.fabric-interfaces` | (empty) | No | Comma-separated fabric domains (network devices as libfabric names them, for example `lo`) to serve on. Empty = every domain the provider discovers. |
| `largeobj.fabric-max-in-flight` | 256 | No | Transfers each fabric service keeps in flight. 0 = provider default. Max 65536. |
| `largeobj.fabric-crc-pool-threads` | 2 | No | Threads hashing checksummed transfers. Min 1, max 1024. |
| `largeobj.enabled` | yes | Yes | `no` rejects every `BLOB.*` command from clients with `DISABLED largeobj module is disabled`. Keys stay; core commands such as `DEL` still work. |

## Test

```bash
# Full build + test pipeline (fmt, clippy, unit tests, integration tests).
# Uses VALKEY_SERVER_PATH if set; else clones and builds Valkey SERVER_VERSION
# (default unstable) from GitHub.
./build.sh

# fmt, clippy, release build, unit tests (no valkey-server needed)
./build.sh build

# Integration tests only (assumes built)
./build.sh integ-test

# Specific test
TEST_PATTERN=test_set_get_roundtrip ./build.sh test
```

## Benchmark

```bash
# Dram only
./bench.sh --port 6379

# All modes on NVMe stripe
./bench.sh --port 6379 --disk-dir /mnt/bigobj-data/bench-test
```

See [BENCHMARK.md](BENCHMARK.md) for full documentation, parameters, and troubleshooting.

## License

BSD-3-Clause

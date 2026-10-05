# Benchmark Guide

## Overview

`bench.sh` measures BLOB.GET throughput across four modes:

| Mode | What it tests |
|------|---------------|
| **fio** | Raw NVMe baseline (O_DIRECT, io_uring, 16 jobs × iodepth 128). No module involved. |
| **Dram** | All objects in DRAM. No NVMe. Baseline for command processing overhead. |
| **Tiered** | NVMe persistence + DRAM cache. First GET promotes from NVMe → DRAM. Subsequent GETs are cache hits. |
| **NVMe** | Pure NVMe reads. No DRAM cache (`max-promote-size 0`). Every GET reads from disk. |

## Quick Start

```bash
# Dram only (no NVMe needed)
./bench.sh --port 7380

# All 4 modes on NVMe stripe (default)
./bench.sh --port 7380 --nvme-dir /mnt/bigobj-data/bench-test

# Specific modes only
./bench.sh --port 7380 --nvme-dir /mnt/bigobj-data/bench-test --modes "NVMe"
./bench.sh --port 7380 --nvme-dir /mnt/bigobj-data/bench-test --modes "Dram NVMe"
```

## Requirements

- `valkey-server`, `valkey-cli`, `valkey-benchmark` in `$PATH`
- Module built: `cargo build --release`
- Python 3 (no pip packages)
- For Tiered/NVMe/fio: O_DIRECT capable mount on real NVMe (XFS or ext4)
- For fio mode: `fio` installed

## How It Works

### fio mode
Runs `fio` directly on the NVMe device for each object size. No module, no server. Establishes the raw disk IOPS/bandwidth ceiling.

### Module modes (Dram, Tiered, NVMe)
For each mode × size combination:

1. **Kill stale server** on the port (graceful shutdown + force kill)
2. **Start fresh server** with mode-specific configs
3. **Verify fresh server** — check uptime < 60s (detects stale servers)
4. **Wait for ready** (up to 30s PING retry — Tiered allocates 32GB+ on startup)
5. **Populate keys** via raw RESP (Python, no dependencies)
6. **Verify DBSIZE** — must match expected key count, exit 1 if not
7. **Record disk reads** from `/sys/block/*/stat`
8. **Run `valkey-benchmark`** for `--duration` seconds
9. **Verify benchmark produced results** — exit 1 if no throughput line
10. **Assert disk reads** — verify actual NVMe I/O happened, exit 1 if not
11. **Shutdown server** and wait for full exit (up to 15s + force kill)

## bench-mode

The module is loaded with `bench-mode yes`. This makes BLOB.GET reply with an integer (the object size) instead of the actual bulk data. This isolates storage + io_uring throughput from TCP output buffer overhead. The full NVMe read still happens — only the reply is shortened.

## Key Format

`valkey-benchmark` replaces `__rand_int__` with a **zero-padded 12-digit** number. Example: `k:__rand_int__` with `-r 500` sends `k:000000000042`.

The populate script uses `f'k:{i:012d}'` to match. If these don't match, all GETs return nil and the benchmark measures nothing. The disk read assertion catches this.

## Disk Read Assertions

For every mode except Dram, the script reads `/sys/block/<device>/stat` before and after the benchmark to count actual disk reads.

| Mode | Expected reads | Failure means |
|------|---------------|---------------|
| **fio** | > 0 | Wrong mount point |
| **Tiered** | ≥ num_keys | Keys not reading from NVMe (key format mismatch, code bug) |
| **NVMe** | >> num_keys | Keys not reading from NVMe on every GET |
| **Dram** | (no check) | No disk I/O expected |

All assertions exit with code 1 and a `FATAL:` message explaining what was expected, what was got, and what to check.

## Error Handling

The script exits immediately (code 1) with a `FATAL:` message on:

| Condition | Message |
|-----------|---------|
| Server won't start | `FATAL: Server failed to start` |
| Stale server on port | `FATAL: Stale server detected (uptime=Xs)` |
| DBSIZE doesn't match keys populated | `FATAL: DBSIZE mismatch — expected N, got M` |
| fio produced zero disk reads | `FATAL: fio produced zero disk reads` |
| Tiered/NVMe disk reads below key count | `FATAL: ... disk read mismatch` |
| Benchmark timed out | `FATAL: Benchmark timed out` |
| Benchmark produced no throughput results | `FATAL: Benchmark produced no throughput results` |

## Sizing

### Keys
Scaled by object size to keep populate time reasonable:

| Object Size | Keys |
|-------------|------|
| < 4MB | 500 |
| 4MB – 15MB | 100 |
| 16MB – 49MB | 50 |
| ≥ 50MB | 20 |

### NVMe Staging
Auto-calculated: `clients × object_size`. Capped at 1GB (kernel hard limit per registered buffer via `IORING_REGISTER_BUFFERS`). 80% of cap is usable (20% reserved for talc metadata). If staging exceeds the usable cap, client count is automatically reduced.

Example: 50MB objects with 200 clients → usable cap = `1GB × 0.8 = 858MB` → `858MB / 50MB = 16` effective clients.

### Clients
Default: 200. Auto-reduced for large objects when staging cap limits concurrency.

## Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | (required) | Valkey server port |
| `--nvme-dir` | (optional) | NVMe directory. If set, default modes = `fio Dram Tiered NVMe`. |
| `--modes` | auto | Space-separated modes. Valid: `fio`, `Dram`, `Tiered`, `NVMe`. |
| `--sizes` | `"4KB 1MB 50MB"` | Object sizes. Valid: 4KB, 16KB, 50KB, 256KB, 1MB, 4MB, 16MB, 50MB. |
| `--clients` | 200 | Concurrent benchmark clients. |
| `--duration` | 10 | Seconds per benchmark run. |
| `--keys` | 500 | Base key count (scaled down for large objects). |
| `--dram-maxmemory` | 34359738368 (32GB) | DRAM budget in bytes. |
| `--segment-size` | 67108864 (64MB) | Segment size in bytes. |
| `--nvme-maxmemory` | 107374182400 (100GB) | NVMe budget in bytes. |
| `--worker-threads` | 2 | Tokio worker threads. |

Config values in bench.sh use **raw bytes** for module load args. The module also accepts memory notation (e.g., `1gb`) via `CONFIG SET` at runtime.

## Environment Overrides

| Variable | Description |
|----------|-------------|
| `MODULE_SO` | Path to module .so (default: `./target/release/libvalkey_largeobj.so`) |
| `VALKEY_SERVER` | Server binary (default: `valkey-server`) |
| `VALKEY_CLI` | CLI binary (default: `valkey-cli`) |
| `VALKEY_BENCH` | Benchmark binary (default: `valkey-benchmark`) |

## Common Pitfalls

### Wrong mount point
`/data` may not exist as a mount — it falls through to the root disk. Always verify with `df <path>`. The script warns if `--nvme-dir` is on the root disk.

### Stale server
The script kills stale servers before each test and checks uptime < 60s after start. If a previous server takes >15s to die (32GB deallocation), it can still race.

### Startup time
Tiered mode with 32GB DRAM allocates 512 × 64MB segments on startup (~5 seconds). The script retries PING for up to 30 seconds.

### 1GB staging limit
`IORING_REGISTER_BUFFERS` has a kernel hard limit of 1GB per buffer. For 50MB objects, effective clients are auto-reduced to fit within the usable staging cap.

### Request coalescing (TODO)
In Tiered mode, concurrent GETs for the same key during NVMe→DRAM promotion each do a separate NVMe read (the Filling state falls through to NVMe fallback). This causes `disk_reads >> num_keys`. With coalescing, subsequent GETs would wait for the in-progress promotion instead of reading again.

## Example Output

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Mode: NVMe
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ── 4KB (4096 bytes) ── [keys=500, clients=200, staging=64MB]
  Populated 500 keys (4KB) in 0.0s (12391 keys/s)
  DBSIZE: 500
  throughput summary: 151152.70 requests per second
          avg       min       p50       p95       p99       max
  Disk reads: 1511649
```

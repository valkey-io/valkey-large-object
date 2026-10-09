# Large Object Module Interface Design

## Architecture

**One `.so`** — single Valkey module with three internal layers:

- **Data Type** — Valkey keyspace: LoValue struct, commands (BLOB.RDMA_HELLO, BLOB.TCP_GET, BLOB.RDMA_GET, BLOB.TCP_SET, BLOB.RDMA_SET), native DEL free callback, OID generation
- **Engine** — Routes commands through (OperatingMode × Transport) matrix. Decides sync vs async. Owns the `EngineResult` pattern.
- **Storage** — SegmentPool + talc allocator, DRAMPool, NVMePool, io_uring (ReadFixed/WriteFixed), FdPool
- **Transport** — EFA/libfabric (callback-based, oneshot bridge to tokio)

```
Commands (parse args, resolve key → LoValue)
    ↓ calls
Engine (routes by mode + transport, decides sync/async, blocks client if needed)
    ↓ uses
Storage (SegmentPool alloc/free, io_uring via UringOp, FdPool)
    ↓ raw pointer + callback
Transport (EFA fi_write/fi_read, callback-based, oneshot bridge)
```

**Runtime:** Module owns a tokio runtime. Async paths (NVMe I/O, EFA DMA) spawn tasks on it. Sync paths (DRAM-only TCP) execute inline on the main thread.

---

## Memory Model

All I/O buffers live in **segments** — large contiguous 4KB-aligned memory regions allocated via `alloc_zeroed`. The `talc` arena sub-allocates within them.

```
SegmentPool
├── segments: Vec<Segment>     (each segment: base ptr + size + iovec_index)
├── allocator: Mutex<Talc>     (sub-allocates within segments)
├── alloc() → SegmentBuffer    (offset + len within a segment)
├── free()                     (returns memory to talc)
└── buffer_ptr() → *mut u8    (absolute pointer for I/O)
```

Two pools, same underlying `SegmentPool`:
- **NVMePool** — transient staging buffers for NVMe read/write (freed after each op)
- **DRAMPool** — long-lived cached objects (freed on DEL or eviction)

Segments are registered with io_uring (`IORING_REGISTER_BUFFERS`) and EFA (`fi_mr_reg`) once at startup. The global `IOVECS` registry ensures `iovec_index` = kernel array position.

---

## Ownership During I/O

**Key principle:** The tokio task that spawns the I/O owns the buffer (via `StreamingContext` or `ObjectContext`) until the operation completes. Neither io_uring nor transport take ownership.

```
Tokio task owns StreamingContext (contains SegmentBuffer)
    ↓ passes raw pointer + iovec_index via UringOp
io_uring poller (kernel reads/writes at that address)
    ↓ fires oneshot tx.send() on CQE completion
Tokio task resumes (rx.await returns)
    ↓ frees buffer via pool.free(&seg_buf)
```

For EFA:
```
Tokio task owns StreamingContext
    ↓ passes raw pointer via session.read/write(buf_ptr, ...)
EFA CQ callback fires tx.send()
    ↓
Tokio task resumes (rx.await)
    ↓ frees buffer
```

**SAFETY:** `buf_ptr` (raw `*mut u8`) must remain valid until the callback fires. The tokio `async move` block captures `stream_ctx` which keeps the buffer alive. This is convention, not type-enforced — future refactors must preserve this invariant.

---

## Engine Dispatch (EngineResult)

Commands call `engine::execute_get()` / `engine::execute_set()` which return `EngineResult`:

```rust
pub enum EngineResult {
    Sync(Result<ValkeyValue, ValkeyError>),  // reply directly, no blocking
    Async,                                    // client blocked, reply comes from tokio task
}
```

Routing matrix:

| Mode | Transport | Path |
|------|-----------|------|
| Dram + TCP | Sync | `serve_get_dram_tcp` / `serve_set_dram_tcp` — inline on main thread |
| Dram + EFA | Async | `execute_get_dram_efa` / `execute_set_dram_efa` — tokio task for EFA DMA |
| Tiered + TCP | Async | `execute_get_tiered` / `execute_set_tiered` — tokio task for NVMe I/O |
| Tiered + EFA | Async | Same tiered functions — EFA + NVMe both in one tokio task |

`block_client()` is called ONLY inside the engine when it decides the async path. The command handler never blocks.

---

## io_uring Interface (UringOp)

```rust
pub struct UringOp {
    pub iovec_index: u16,     // segment position in IORING_REGISTER_BUFFERS array
    pub buf_ptr: *mut u8,     // absolute buffer address within the segment
    pub file_offset: u64,     // offset within the NVMe file (0 for single-chunk today)
    pub len: u64,             // bytes to read/write
}

// Submit and await:
let rx = uring::submit_read(fd, &op);   // returns oneshot Receiver
let result = rx.await;                   // Ok(Ok(bytes)) | Ok(Err(StorageError)) | Err(RecvError)

let rx = uring::submit_write(fd, &op);
let result = rx.await;                   // Ok(Ok(())) | Ok(Err(StorageError)) | Err(RecvError)
```

Internally: `submit_read`/`submit_write` wrap the callback-based `IoRequest` enum with a oneshot channel. The io_uring poller thread fires `tx.send()` on CQE completion.

---

## Transport Interface (EFA)

```rust
impl Session {
    /// SAFETY: buf_ptr must remain valid until on_complete fires.
    pub fn write(&self, buf_ptr: *mut u8, len: usize, rkey: u64, remote_addr: u64,
                 on_complete: Box<dyn FnOnce(*mut u8, Result<(), TransportError>) + Send>);

    pub fn read(&self, buf_ptr: *mut u8, len: usize, rkey: u64, remote_addr: u64,
                on_complete: Box<dyn FnOnce(*mut u8, Result<(), TransportError>) + Send>);
}
```

Engine uses async helpers that wrap the callback in a oneshot:
```rust
async fn efa_read_from_client(session: Arc<Session>, buf_ptr: usize, len, rkey, remote_addr) -> Result<(), ValkeyError>;
async fn efa_write_to_client(session: Arc<Session>, buf_ptr: usize, len, rkey, remote_addr) -> Result<(), ValkeyError>;
```

---

## Tiered GET Flow

```
1. Engine: check DRAMPool hit → serve directly (no I/O)
2. Engine: try_promote_object (DRAMPool has space?) → ReadFixed into DRAMPool, serve, stays cached
3. Engine: NVMePool fallback (DRAMPool full) → transient ReadFixed, serve, free
```

## Tiered SET Flow

```
1. Engine: alloc NVMePool staging buffer
2. TCP: memcpy data into buffer | EFA: efa_read_from_client into buffer
3. do_tiered_nvme_write: open file → WriteFixed → create LoValue in keyspace → free buffer
```

---

## Configs

| Config | Default | Description |
|--------|---------|-------------|
| `operating-mode` | Dram | Enum: `Dram` or `Tiered` |
| `dram-maxmemory` | 0 (no limit) | Total DRAM budget. 0 = grow on demand. |
| `segment-size` | 64mb | Size of each DRAMPool segment |
| `nvme-dir` | (required if Tiered) | Dedicated, module-owned directory for .dat files. Wiped wholesale on startup and teardown, so it must NOT be shared with other files. |
| `nvme-maxmemory` | 10gb | Max disk usage |
| `disk-staging-size` | 64mb | Single NVMe staging segment (DRAM) |
| `max-promote-size` | 256mb | Max object size for DRAMPool promotion. 0 = disable. |
| `worker-threads` | 2 | Tokio runtime thread count |
| `direct-io` | yes | O_DIRECT for NVMe files |

---

## Key Design Decisions

| Concern | Decision |
|---------|----------|
| Buffer ownership during I/O | Tokio task holds StreamingContext/ObjectContext alive. Transport/io_uring get raw pointers. Convention-enforced, not type-enforced. |
| Sync vs async | Engine decides. Dram+TCP = sync (no block_client). Everything else = async (block + tokio spawn). |
| io_uring bridge | Callback-based poller → oneshot channel → tokio task awaits. |
| EFA bridge | Same pattern: callback → oneshot → await. |
| DRAMPool promotion | Inline during GET (try_promote_object). Not background/fire-and-forget. |
| NVMe staging | Transient: alloc → I/O → free per request. Not cached. |
| Segment registration | Global IOVECS vec. append_iovec() at creation = array position = iovec_index. Impossible to mismatch. |
| Free callback | TODO: not concurrency-safe. Needs refcount/drain logic before production. |

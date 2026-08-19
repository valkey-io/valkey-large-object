# Large Object Module + Transport Crate Interface Design

## Architecture

- **One `.so`** — single Valkey module artifact with three internal layers:
  - **Data Type** — Valkey keyspace integration: LoValue struct, commands (LO.HELLO, LO.GET, LO.SET), RDB callbacks, TIERING.REF, BlockClient management
  - **Storage** — Buffer pool + NVMe I/O: Buffer ownership (get → use → Drop returns), io_uring read/write, registered buffers, disk eviction
  - **Transport crate** (libefa-rs) — EFA/libfabric lifecycle, multi-device LB, completion handling. Cargo dependency, reusable by any Rust project. **Runtime-agnostic: does not own threads or an async runtime.**

**Runtime ownership:** The module owns a tokio runtime (created at OnLoad, configured via module args: thread count, core pinning). The transport crate exposes `async fn write/read`. The module `.await`s them from tasks spawned on its runtime. Transport's internal CQ polling is opaque to the module — we don't pass it a Handle or spawn its tasks.

```
Data Type (commands, LoValue, keyspace)
    ↓ calls
Storage (buffer pool, io_uring, NVMe files)
    ↓ passes buffers to
Transport (EFA — exposes async fn, internal CQ polling opaque)
    ↑
Module Runtime (tokio, owned by module — spawns per-command tasks that .await transport)
```

Transport never calls storage or data type. Storage never touches Valkey keys.

---

## Lifecycle

```
Module OnLoad:
  1. Module creates tokio runtime   → thread count + core pinning from module config args
  2. Transport::init()              → discover EFA devices, create fi_fabric + fi_domain (sync, no runtime needed)
  3. Storage allocates buffer pool  → fixed-size buffers, 4KB-aligned, owned by storage
  4. Storage::register_buffers()    → IORING_REGISTER_BUFFERS (kernel pins pages for NVMe DMA)
  5. Transport::register_buffers()  → fi_mr_reg same buffers across all EFA domains (NIC learns phys addrs)
     (Order between 4 and 5 does not matter — both independently pin pages via get_user_pages.
      Dual-registration on the same physical pages has no conflicts. Only concern: each registration
      counts against ulimit -l locked memory accounting, so 2GB pool = ~4GB locked memory reported.
      EC2 instances typically have unlimited memlock.)

LO.HELLO (per client connection):
  5. Session::new(peer_addr)   → fi_av_insert peer on ALL devices, store dest_fi_addr handles

LO.GET / LO.SET (per operation):
  6. buf = storage.pool_get()      → owned Buffer, exclusive access to pinned memory
  7. session.write/read(buf, ...)   → Buffer moves into transport (ownership transfer)
  8. On completion callback (fires on transport CQ thread or io_uring poller thread):
       Buffer returned via callback parameter → caller can use or let Drop
       Drop fires → Buffer returns to pool automatically
       UnblockClient(bc, private_data)
  9. Reply callback fires on main thread:
       ctx.reply_ok() or ctx.reply_error()

Client disconnect:
  10. Session::close()              → destroy endpoints, remove AV entries

Module unload:
  11. Transport::deregister_buffers() → fi_mr_dereg
  12. Transport::shutdown()           → close domains, free global EFA state
```

---

## Data Type Layer (commands + keyspace)

The data type layer owns:
- `LoValue` struct in Valkey's keyspace (accessed via `ValkeyModule_OpenKey`)
- Command handlers (LO.HELLO, LO.GET, LO.SET)
- Native Valkey `DEL` triggers module free callback → deletes NVMe file
- OID generation (monotonic counter)
- RDB callbacks (save/load references)
- TIERING.REF replication
- BlockClient lifecycle

Command handlers read `LoValue` from keyspace to get `object_id` and `len`, then call storage for I/O. Storage never touches Valkey keys.

```rust
/// LoValue — the Valkey data type value struct, stored in Valkey's keyspace.
/// Accessed via ValkeyModule_OpenKey → ModuleTypeGetValue on the main thread.
/// This is NOT in the storage layer. Command handlers read this to get file info before calling storage.
#[derive(Debug, Clone)]
pub struct LoValue {
    pub object_id: ObjectId,   // monotonic per-node OID (used as filename)
    pub len: u64,              // object size in bytes
    pub crc32c: u32,           // integrity checksum (verified on replication pull)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectId(pub u64);
// ObjectId IS the file path: deterministic mapping OID → "{data_dir}/{oid:016x}.dat"
// No lookup table. Compact u64 safe for replication streams, RDB, and LoValue.
```

---

## Storage Layer API (buffer pool + NVMe I/O)

Storage operates on **OIDs and file paths**, never on Valkey keys. The command handler resolves key → OID via the data type layer, then calls storage.

Buffer ownership is the core principle: holding a `Buffer` = exclusive access to pinned memory. Drop returns it to the pool automatically. No explicit pin/unpin/pool_put.

```rust
/// Types:
///   PinnedBuffer (types.rs) — raw 4KB-aligned Box<[u8]>, registered with io_uring + EFA
///   Buffer (storage/buffer.rs) — owned handle wrapping &'static PinnedBuffer + idx
///     - Holding it = exclusive access. Moving it = transferring ownership.
///     - Drop = automatic return to pool. No explicit put needed.
///   BufferPool (storage/buffer.rs) — Mutex<Vec<Buffer>>. get() pops, Drop pushes back.

/// Error types for storage operations.
#[derive(Debug)]
pub enum StorageError {
    IoError { code: i32 },     // io_uring read/write failed
    PoolExhausted,             // no free buffers available
    ObjectTooLarge,            // object exceeds pool buffer size
}

trait Storage {
    // ─── Buffer Pool ─────────────────────────────────────────────────────

    /// Get a buffer from the pool. Returns None if pool exhausted.
    /// Returned Buffer is owned — Drop returns it to the pool automatically.
    fn pool_get(&self) -> Option<Buffer>;

    /// Pool buffer size (all buffers are this fixed size).
    fn pool_buf_size(&self) -> usize;

    // ─── Registration ────────────────────────────────────────────────────

    /// Register pool buffers with io_uring (IORING_REGISTER_BUFFERS).
    fn register_buffers(&self) -> Result<(), StorageError>;

    /// Deregister pool buffers from io_uring.
    fn deregister_buffers(&self) -> Result<(), StorageError>;

    // ─── NVMe I/O ───────────────────────────────────────────────────────

    /// Read object bytes from NVMe into buf. Async via io_uring ReadFixed.
    /// Takes Buffer by value (ownership transfers to storage during I/O).
    /// Returns (Buffer, bytes_read) in callback — caller gets buf back.
    fn read_into(
        &self,
        object_id: ObjectId,
        buf: Buffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<u64, StorageError>) + Send>,
    );

    /// Write buf to NVMe as a new object. Async via io_uring.
    /// Takes Buffer by value. Returns (Buffer, ObjectId, crc32c) via callback.
    fn write_new(
        &self,
        buf: Buffer,
        len: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(ObjectId, u32), StorageError>) + Send>,
    );

    /// Delete an object file from NVMe. Called on key deletion or eviction.
    fn delete(&self, object_id: ObjectId);
}
```

---

## Transport Crate API (libefa-rs)

```rust
/// PinnedBuffer (types.rs) — raw kernel-pinned memory. Transport uses this for DMA registration.
/// Buffer (storage/buffer.rs) — owned handle. Transport receives this for DMA operations.

/// Error types for transport operations.
#[derive(Debug)]
pub enum TransportError {
    DeviceNotFound,
    RegistrationFailed,
    SessionCreateFailed,
    WriteFailed { code: i32 },
    ReadFailed { code: i32 },
    RegionOutOfBounds,
    Timeout,
    SessionClosed,
}

pub struct EfaContext { /* fi_fabric + fi_domain per device, registered MRs */ }

/// EFA endpoint address — 32 bytes, opaque to callers.
pub struct EfaAddress(pub [u8; 32]);

impl EfaContext {
    /// Discover EFA devices. Synchronous — no runtime needed.
    pub fn init() -> Result<Self, TransportError>;

    pub fn device_count(&self) -> usize;
    pub fn is_available(&self) -> bool;

    /// Register buffers for DMA. Takes slice of byte slices (from PinnedBuffer memory).
    pub fn register_buffers(&self, bufs: &[&[u8]]) -> Result<(), TransportError>;
    pub fn deregister_buffers(&self) -> Result<(), TransportError>;
    pub fn shutdown(self);
}

/// Client-side memory region descriptor. Received during LO.HELLO.
pub struct ClientRegion {
    pub rkey: u64,
    pub remote_addr: u64,
    pub len: u64,
}

pub struct Session { /* endpoints, AV entries, client regions */ }

impl Session {
    pub fn new(ctx: &EfaContext, peer_addr: &EfaAddress) -> Result<Self, TransportError>;

    pub fn server_addrs(&self) -> Vec<EfaAddress>;

    /// DMA write: server buffer → client memory at (rkey, remote_addr).
    /// Takes Buffer by value (ownership during DMA). Returns Buffer in callback.
    pub fn write(
        &self,
        buf: Buffer,
        len: usize,
        rkey: u64,
        remote_addr: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    );

    /// DMA read: client memory at (rkey, remote_addr) → server buffer.
    /// Takes Buffer by value. Returns Buffer in callback.
    pub fn read(
        &self,
        buf: Buffer,
        len: usize,
        rkey: u64,
        remote_addr: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    );

    pub fn close(self);
}
```

---

## Client Unblocking

The module uses `ValkeyModule_BlockClient` with `ThreadSafeContext` (valkeymodule-rs does not yet expose reply_callback):

```rust
// Main thread:
bc = ctx.block_client();

// Any background thread (io_uring poller, EFA CQ thread):
let thread_ctx = ThreadSafeContext::with_blocked_client(bc);
thread_ctx.reply(Ok(ValkeyValue::...));  // unblocks + replies
```

**Future:** When valkeymodule-rs adds reply_callback support, keyspace writes (LoValue SET) should move to the reply callback (runs on main thread with real ctx). Currently done via ThreadSafeContext lock.

---

## LO.GET Command Flow

```
(a) Main thread (command handler):
    key = args[1]
    rkey = args[2]
    remote_addr = args[3]

    // Data type layer: read LoValue from Valkey keyspace
    lo_value = OpenKey(key) → ModuleTypeGetValue()
    if lo_value.is_none() { return reply_null() }
    // If object exceeds fixed pool buffer size, allocate a one-off buffer of exact size.
    // This buffer is still 4KB-aligned (for O_DIRECT) but not from the pool's free-list.
    // It must be individually registered with io_uring and EFA before use, and deregistered after.
    // TODO: implement oversized buffer path (pool_get_sized or direct mmap + register)
    // OR we will have to reject the command during the LO.SET itself to not allow divergence.
    buf = if lo_value.len <= storage.pool_buf_size() {
        storage.pool_get()          // fast path: pre-registered pool slot
    } else {
        storage.pool_get_oversized(lo_value.len)  // slow path: custom-sized allocation
    }
    if buf.is_none() { return reply_error("pool exhausted") }

    session = sessions.get(client_id)
    storage.pin(buf)
    bc = BlockClient(ctx, dma_get_reply_fn, free_fn)

    // Storage layer: read from NVMe by OID (not key)
    storage.read_into(lo_value.object_id, buf, lo_value.len, move |read_result| {
        match read_result {
            Err(e) => {
                storage.unpin(buf); storage.pool_put(buf);
                UnblockClient(bc, ErrResult(e));
            }
            Ok(bytes_read) => {
                // Transport layer: send to client GPU (module spawns task on its runtime)
                runtime.spawn(async move {
                    let result = session.write(buf, bytes_read, rkey, remote_addr).await;
                    storage.unpin(buf);
                    storage.pool_put(buf);
                    UnblockClient(bc, result.into());
                });
            }
        }
    });
    return NoReply

(b) Main thread (reply callback):
    dma_get_reply_fn(ctx, private_data):
        match private_data { Ok → ctx.reply_ok(), Err(e) → ctx.reply_error(e) }
```

---

## LO.SET Command Flow

```
(a) Main thread (command handler):
    key = args[1]
    len = args[2]
    rkey = args[3]
    remote_addr = args[4]

    // TODO: The alternative is to do an one-off allocation of a custom size, after
    // dual registering it. 
    if len > storage.pool_buf_size() { return reply_error("object exceeds buffer") }

    session = sessions.get(client_id)
    buf = storage.pool_get()
    storage.pin(buf)
    bc = BlockClient(ctx, dma_set_reply_fn, free_fn)

    // Transport layer: read from client GPU into buf (module spawns task on its runtime)
    runtime.spawn(async move {
        let read_result = session.read(buf, len, rkey, remote_addr).await;
        match read_result {
            Err(e) => {
                storage.unpin(buf); storage.pool_put(buf);
                UnblockClient(bc, ErrResult(e));
            }
            Ok(()) => {
                // Storage layer: write to NVMe (returns new OID + crc)
                storage.write_new(buf, len, move |write_result| {
                    storage.unpin(buf);
                    storage.pool_put(buf);
                    UnblockClient(bc, write_result.into());
                });
            }
        }
    });
    return NoReply

(b) Main thread (reply callback):
    dma_set_reply_fn(ctx, private_data):
        match private_data {
            Ok((oid, crc)) => {
                // Data type layer: create/update LoValue in keyspace
                set_lo_value(key, LoValue { object_id: oid, len, crc32c: crc });
                ctx.replicate("TIERING.REF", key, oid, len, crc);
                ctx.reply_ok();
            }
            Err(e) => ctx.reply_error(e)
        }
```

---

## TCP vs EFA Transport Paths

Large objects need to support both RDMA-capable clients (GPU inference with EFA) and regular TCP clients (general applications, debugging, migration tooling).

**Current thinking: same command, optional args determine transport.**

```
LO.GET key [rkey remote_addr len]
  - With args:    client has LO.HELLO session → NVMe read → RDMA write to client GPU
  - Without args: no session required → NVMe read → TCP bulk reply

LO.SET key len [rkey remote_addr]
  - With args:    client has LO.HELLO session → RDMA read from client GPU → NVMe write
  - Without args: no session required → client sends bytes inline (TCP bulk) → NVMe write
```

Server knows if the connection has a DMA session. Trailing args give the client explicit control over GPU memory placement.

**Alternative: separate commands.**

```
LO.GET  key                           → always TCP reply
LO.GET key rkey remote_addr len  → always RDMA (requires LO.HELLO)
```

Pros: no ambiguity, no arg-count dispatch. Cons: two commands for the same logical operation.

**Decision: TBD.** Starting with optional trailing args (fewer commands, one client library path). Can split later if the arg-count dispatch causes issues.

---

## Key Design Decisions

| Concern | Decision |
|---|---|
| Layer separation | Data type = keyspace + commands. Storage = pool + disk I/O (by OID, not key). Transport = EFA. Each layer has a clean API boundary. |
| Runtime ownership | Module owns tokio runtime (thread count + core pinning). Transport crate exposes `async fn` — internal CQ polling is opaque. No Handle passed. |
| Concurrency | NVMe ops: io_uring with callbacks (CQ poller on module runtime). EFA ops: async functions awaited on module runtime. No thread ever blocks on another subsystem. |
| Reply mechanism | `BlockClient` with `reply_callback`. `UnblockClient` called from any thread/task. Reply fires on main thread. No ThreadSafeContext needed. |
| Memory registration | Buffer pool dual-registered with io_uring + EFA once at startup. Same physical pages, no conflicts. Zero per-op registration cost. |
| rkey model | Client sends 1-8 `ClientRegion` descriptors during LO.HELLO. Per-op specifies `region_idx` + `remote_offset`. |
| Multi-EFA LB | Internal to `Session`. Best-of-two on in-flight count. Storage/data type unaware. |
| Buffer ownership | Storage owns PinnedBuffer (Box<[u8]>, 4KB-aligned). Buffer is an owned handle (&'static PinnedBuffer + idx). Holding Buffer = exclusive access. Drop = return to pool. No pin/unpin. |
| Error handling | Typed errors (`TransportError`, `StorageError`) propagated through callbacks/await to `UnblockClient` → reply callback → client. |
| Transport API style | `async fn write/read` — module spawns tasks on its runtime. No callback variants needed. |
| Storage addressing | Storage takes `ObjectId`, not Valkey keys. Key→OID resolution is the data type layer's job (reads LoValue from keyspace). |

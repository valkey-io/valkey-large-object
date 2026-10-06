# Storage Design

**Date:** 2026-08-21  **Status:** In-Review  **Author:** @KarthikSubbarao

---

## 1. Problem

The module stores large objects (15KB to multi-GB (TBD)) and must serve them via two transports:
- **TCP:** standard RESP reply
- **EFA:** RDMA fi_write directly to client GPU memory (requires EFA NIC; detected at startup — if fabric init fails, `BLOB.HELLO` returns an EFA-unavailable error, no EFA session can be established, and the module operates TCP-only for its lifetime)

Two operating modes:
- **DRAM-only:** All objects live in DRAM. No NVMe. Fastest reads. Limited by DRAM capacity.
- **DRAM + NVMe:** All objects persist on NVMe. DRAM is a read cache (hot objects promoted on access). Larger capacity, slightly higher latency on cold reads.

---

## 2. Hard Constraints

These two constraints drive every design decision:

**1. EFA registration (fi_mr_reg):**
Any buffer used as source for `fi_write` must be pre-registered with the NIC. Registration pins physical pages and programs the NIC's translation table. Cost: ~300-500μs per call (measured ~333μs on i8ge EFA, not size-proportional). Must be done at startup or on rare resize events — never on the data path. Per-request registration destroys throughput by 45x (measured: 138K rps pre-registered vs 3K rps per-request at 4KB).

**2. io_uring registration (IORING_REGISTER_BUFFERS):**
Any buffer used for `ReadFixed`/`WriteFixed` must be pre-registered with the kernel's io_uring ring. Enables kernel-bypass I/O (no per-op address translation). Cost: one-time at startup. The kernel hard-caps the number of registered entries at `UIO_MAXIOV` = **1024** (`IORING_REGISTER_BUFFERS` returns `EINVAL` above it); the segment model keeps the count far below this (a handful of ≤1 GiB entries).

**Consequence:** A buffer registered with both can serve NVMe I/O AND EFA transfers without copying. An unregistered buffer can only serve TCP replies.

### io_uring ReadFixed/WriteFixed Mechanics

`IORING_REGISTER_BUFFERS` takes an array of `iovec` structs. Each entry is one "buffer" from io_uring's perspective. `ReadFixed`/`WriteFixed` operations reference a buffer by its array index (`buf_index`) plus an offset and length within it.

**Critical insight:** `ReadFixed`/`WriteFixed` address a buffer by its array index (`buf_index`) plus an offset and length within it — so a single segment can hold many objects, selected by offset.

**Hard limit — 1 GiB per registered buffer:** Each iovec entry passed to `IORING_REGISTER_BUFFERS` must have `iov_len <= 1 GiB`. The kernel rejects any entry larger than 1 GiB with `EFAULT` (observed at scale on i8ge). A segment is exactly one iovec entry, so **every segment (which is io_uring registered) must be capped at 1 GiB.** Larger capacity is achieved by registering *multiple* ≤1 GiB segments, not one large one.

```
Registration:  [ iovec{segment0, 1GiB}, iovec{segment1, 1GiB}, iovec{segment2, 1GiB} ]
                  buf_index=0             buf_index=1            buf_index=2

ReadFixed:  buf_index=1, offset=0x5000, len=4MB
            → reads 4MB from NVMe into segment1 at byte offset 0x5000
```

**Count ceiling:** `IORING_REGISTER_BUFFERS` accepts at most `UIO_MAXIOV` = 1024 entries — beyond that the kernel returns `EINVAL` (it is a hard limit, not a gradual slowdown). Registering many small buffers also degrades the kernel's per-op buffer lookup; the segment model sidesteps both by registering a handful of large entries (2–8), so neither the ceiling nor the lookup cost is a concern.

**No alternative API:** There is no way to use `ReadFixed`/`WriteFixed` without `IORING_REGISTER_BUFFERS`. The registration is what gives the kernel pre-pinned page tables to avoid per-I/O `get_user_pages()`. Plain `read`/`write` ops work without registration but pay the page-pinning cost every time (~15-20% throughput loss).

**Resize implication:** Adding or removing a segment requires `IORING_UNREGISTER_BUFFERS` (drops all registrations) then `IORING_REGISTER_BUFFERS` with the new array. During this window (~μs), no `ReadFixed`/`WriteFixed` can be issued. In-flight ops already submitted are unaffected (kernel has their pages pinned). New submissions must wait or fall back to plain read/write.

---

## 3. Object Size Distribution

Object sizes are **unknown at design time**. The module stores opaque blobs from clients (LMCache, vLLM, custom inference frameworks). Sizes depend on model architecture, page size, attention type, and layer grouping — none of which we control.

**Known lower bound:** ~15KB (compressed attention blocks, small metadata).
**Known upper bound:** Unbounded in theory. KDA checkpoints ~150MB, full KV cache for long contexts can reach GBs. The module handles large objects via multi-buffer parallel I/O (§7).

Example ranges observed in hybrid-attention models:

| Object type | Typical size | Notes |
|---|---|---|
| Compressed attention blocks | 15–50 KB | Small, numerous |
| Metadata / indexer state | 10–50 KB | Small |
| Compressed KV (4x) | 0.5–1 MB | Common |
| Sliding window KV | 0.5–2 MB | Fixed per model |
| KDA recurrent state / checkpoints | 2–150 MB | Hot, good DRAMPool candidates |
| Full-attention KV blocks | 0.5 MB – multi-GB | Linear with context length |

**Variance: 1000x+ across object types.**

The client stores each chunk as a separate key. Our module sees individual opaque blobs at varying sizes.

---

## 4. Storage Approaches

Two ways to handle the size variance. This is the fundamental design choice.

### 4.1 Approach A: Fixed-Size Buffer Classes

Pre-allocate buffers in 2-3 size classes. Each object goes in the smallest class that fits. Unused space in the buffer is DRAM padding (wasted memory, but not wasted on NVMe or EFA — those use exact `len`).

```
Class 1: 64KB buffers × 2000   (serves 15-64KB objects)
Class 2: 1MB buffers  × 500    (serves 65KB-1MB objects)
Class 3: 8MB buffers  × 250    (serves 1-8MB objects)
```

**How it works:**
- Each class is a Vec of pre-allocated, 4KB-aligned buffers
- All buffers registered with io_uring (`IORING_REGISTER_BUFFERS`) and EFA (`fi_mr_reg`) at startup
- Alloc = pop buffer index from class free list. O(1).
- Free = push buffer index back to free list. O(1).
- No fragmentation possible (fixed slots, never split or merged)

**Registration:**
- io_uring: Yes (each buffer is a registered fixed buffer — enables `ReadFixed`/`WriteFixed`)
- EFA: Yes (each buffer is within a registered region)

**Pros:**
- Zero-copy on ALL paths (NVMe read → keep buffer as cache → fi_write from same buffer)
- O(1) deterministic alloc/free
- No fragmentation, ever
- io_uring `ReadFixed` (fastest NVMe path — measured 156K rps vs 130K without)
- Simple implementation (~100 lines)

**Cons:**
- Up to 50% DRAM waste per object (500KB object in 1MB buffer = 500KB wasted)
- Fixed capacity per class decided at startup
- Cannot handle objects larger than largest class (reject with ERR)

### 4.2 Approach B: Arena with Slab Allocator (talc) [Recommended/Chosen]

Allocate one or more large contiguous memory segments at startup. Register each segment with EFA. Sub-allocate exact-sized slots from the segments using a general-purpose allocator (talc).

```
┌────────────────────────────────────────────────────────────┐
│ Segment 0 (1GiB, io_uring buf_index=0)                    │
│ ┌──────┐┌─────────┐┌──┐┌─────────────┐┌──────┐ ...      │
│ │ 47KB ││  820KB  ││4K││    6.2MB    ││ 91KB │          │
│ └──────┘└─────────┘└──┘└─────────────┘└──────┘          │
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│ Segment 1 (1GiB, io_uring buf_index=1)                    │
│ ...                                                        │
└────────────────────────────────────────────────────────────┘
```

**How it works:**
- Each segment owns its own `talc` allocator, claiming exactly that segment's range
  (`talc.claim(span)` at segment creation). Alloc picks a target segment first, then
  mallocs from that segment's talc. (Earlier drafts used one shared talc across all
  segments; §8.7 explains why per-segment was chosen.)
- Alloc = `talc.malloc(Layout::from_size_align(len, 4096))`. Finds contiguous free block within the chosen segment.
- Free = `talc.free(ptr, layout)`. Returns space, coalesces with adjacent free blocks in that segment.
- Objects tracked as `(segment_idx, offset, len)` — `segment_idx` known at alloc time, so no pointer→segment reverse lookup.

**Registration:**
- io_uring: Yes — register each segment as one large buffer (`buf_index = segment_idx`, use offset within it for each I/O). Enables `ReadFixed`/`WriteFixed`.
- EFA: Yes (entire segment registered as one region)

**Pros:**
- Near-zero DRAM waste (allocate exact bytes + 4KB alignment overhead)
- Handles any size up to segment capacity without class boundaries
- io_uring `ReadFixed`/`WriteFixed` via segment-as-buffer (same NVMe throughput as Approach A)
- EFA zero-copy on DRAM hit (fi_write from any offset within registered segment)
- NVMe read can land directly in arena slot (ReadFixed with offset) — zero-copy promotion possible
- Industry precedent for the pattern (pre-registered large memory regions with sub-allocation to avoid per-request registration cost): Mooncake, NVIDIA DOCA, SPDK

**Cons:**
- Fragmentation possible after many alloc/free cycles with varied sizes
- Alloc is O(1) amortized but O(n) worst case on fragmented arena
- More complex implementation (~315 lines)
- Shrinking requires drain + evacuation (cannot simply return a buffer)
- Adding/removing segments requires `IORING_UNREGISTER_BUFFERS` + re-register (brief I/O pause)

### 4.3 Fragmentation in Approach B

**What causes it:** Interleaved alloc/free of varied sizes. Example: allocate [1MB][64KB][1MB][64KB], then free the 1MB blocks → two 1MB holes separated by 64KB live objects. Cannot allocate 2MB contiguously despite 2MB total free.

**What talc does automatically:**
- Coalescing: when you `free()` a block, talc merges it with adjacent free blocks. This is the primary repair mechanism and it's free.

**What talc cannot do:**
- Compaction (moving live objects to consolidate free space). Would invalidate all pointers/offsets.

**Mitigation strategy:** Open question. Separate segments per layer (§4.5) prevents the worst case (short-lived NVMePool churn fragmenting long-lived DRAMPool). Within each layer, coalescing may be sufficient — needs production data to determine if active mitigation is required. See §10 Open Questions. Other mitigations include (1) banding into segments based on value size (2) scaling out and scaling in to delete fragmented segments.

### 4.4 Comparison

| | Approach A (fixed classes) | Approach B (talc arena) |
|---|:---:|:---:|
| DRAM efficiency | ≤50% waste per class | ~95%+ efficient |
| io_uring ReadFixed | Yes | Yes (segment-as-buffer + offset) |
| NVMe read throughput | ~156K rps | ~156K rps (same — ReadFixed works) |
| EFA zero-copy on hit | Yes | Yes |
| Promotion zero-copy (NVMe → cache) | Yes (keep buffer) | Yes (ReadFixed into arena slot directly) |
| Alloc speed | O(1) guaranteed | O(1) amortized, O(n) worst |
| Fragmentation | Impossible | Possible (coalescing + separate segments mitigate) |
| Defrag mechanism | N/A | Open question (§9) |
| Code complexity | ~100 lines | ~315 lines |
| Best for | Simplicity, predictable latency | Memory efficiency, varied object sizes |

### 4.5 Decision: Approach B (Separate Segments Per Layer)

We use Approach B (talc arena). Object sizes are unknown at design time — talc provides exact-fit allocation regardless of what sizes production traffic produces.

**Per-segment talc — each segment owns its own allocator (see §8.7 for the full
rationale):**

```
NVMePool:    N segments (each ≤1GiB)   — each segment: own Mutex<Talc>, high churn, short-lived StreamingContexts
DRAMPool:    N segments (each ≤1GiB)   — each segment: own Mutex<Talc>, low churn, long-lived ObjectContexts

io_uring registration: [iovec{seg, ≤1GiB}, iovec{seg, ≤1GiB}, ...] — every segment in one array, each ≤1GiB
EFA registration:      fi_mr_reg per segment — enables fi_write from any buffer in either pool
```

**Each segment carries its own `Talc` that `claim`s exactly its own `[base, base+size)`
range (never `talc.extend`).** There is no shared allocator across segments and no
pointer→segment reverse lookup: alloc picks the target segment first (least-loaded)
and mallocs from that segment's own talc, so every allocation lies wholly within
exactly one segment. This single-segment-ownership invariant is what makes the
addressing model valid: one allocation maps to one segment → one io_uring `buf_index`,
one segment refcount, one EFA MR. (See §8.7 for why per-segment talc was chosen over a
single shared heap with reverse lookup.)

**Both pools are io_uring registered (IORING_REGISTER_BUFFERS) in Tiered mode:**
- NVMePool: ReadFixed/WriteFixed for NVMe I/O staging (primary use case)
- DRAMPool: ReadFixed during promotion (NVMe → DRAMPool direct fill, §7.3.4). Without registration, promotion falls back to plain `read` (~15-20% slower per chunk — acceptable but suboptimal).

In Dram mode, there is no io_uring engine — NVMe I/O does not exist — so neither pool is io_uring-registered.

**Both pools are EFA registered (fi_mr_reg), if EFA is available:**
- NVMePool: fi_write to client during serve-and-discard GET
- DRAMPool: fi_write to client from cached objects (the hot serving path)

If EFA init fails at startup, `fi_mr_reg` is skipped for all segments. EFA-transport
command paths are unusable for the module's lifetime: `BLOB.HELLO` fails, and
`BLOB.SET`/`BLOB.GET` with addresses require a HELLO session. The fabric is opened once
at load; there is no runtime re-detection. TCP-transport paths continue normally.

**Why separate segments per layer:**
- Prevents lifetime-mixing fragmentation: NVMePool high-churn alloc/free cycles cannot create holes between long-lived DRAMPool objects
- Each layer's segments only see objects of similar lifetime — fragmentation is self-healing (NVMePool: FIFO churn reclaims space naturally; DRAMPool: infrequent evictions don't leave Swiss-cheese)
- Independent sizing: NVMePool sized for max concurrent I/O, DRAMPool sized for working set
- Independent scaling: expand/shrink one layer without affecting the other

### 4.6 Common Requirements

**O_DIRECT alignment (NVMe mode only — does not apply to DRAM-only mode):**
O_DIRECT bypasses the kernel page cache for direct NVMe I/O. It imposes two constraints:
1. **Buffer address** must be 4KB-aligned (filesystem block size). Handled by `PinnedBuffer::new()` via `Layout::from_size_align(size, 4096)`.
2. **Write length** must be 4KB-aligned (the module rounds to the 4096 filesystem block via `object_disk_len`). Objects not naturally aligned are padded on disk: `ceil(len / 4096) * 4096`. Up to 4095 bytes waste on disk. Reads return only `len` bytes (stored in LoValue metadata).

**Both read and write paths round up the I/O length.** The kernel rejects non-aligned lengths with `EINVAL`; O_DIRECT does not auto-pad. The module rounds up explicitly via `object_disk_len()` (`ceil(len / 4096) * 4096`) on both paths. On SET the reserved and written disk length is this aligned value, and the on-disk size is asserted to equal it.

EFA `fi_write` has no alignment constraint — sends exact `len`.

Without O_DIRECT (DRAM-only mode, or `direct-io no`), neither constraint applies.

**Max object size enforcement:**
- **`max-object-size` (all modes):** Objects whose length exceeds this configurable limit are rejected at `BLOB.SET`, regardless of transport (TCP or EFA) or operating mode (Dram or Tiered). This is a single global cap on object size.
- **TCP additionally:** Valkey's querybuf accumulates the whole payload before dispatch and cannot stream, so TCP is bounded by `max-object-size` in the same way (§7.7).
- **EFA:** Objects larger than a single buffer are chunked internally via multi-buffer parallel I/O (§7.3) using `chunk-size`, still subject to `max-object-size`.
- **NVMe capacity:** Objects exceeding available NVMe space are also rejected at `BLOB.SET` today. Module-driven eviction of NVMe objects to make room is planned (§8.1).

---

## 5. Operating Modes

Both approaches follow the same command-level flow. "Alloc" and "free" refer to `talc.alloc`/`talc.free` from either NVMePool (transient I/O) or DRAMPool (cached objects). The resulting memory is pre-registered with io_uring and EFA (when available).

**EFA availability:** EFA transport is detected at startup. If EFA init succeeds, the `EFA` command variants below are enabled; if not, they are rejected and only the `TCP` paths are available.

### 5.1 DRAM-Only Mode

All objects live exclusively in DRAM. No NVMe storage. Fastest possible reads. Capacity limited by available DRAM.

```
BLOB.SET key len <payload> [rkey remote_addr]:
  1. Alloc buffer (registered memory)
  2a. TCP: copy payload into buffer
  2b. EFA: fi_read from client GPU into buffer (zero-copy)
  3. Track buffer: key → buffer location + len

BLOB.GET key [rkey remote_addr len]:
  4. Lookup in HashMap → buffer pointer
  5a. TCP: reply from buffer
  5b. EFA: fi_write from buffer to client GPU (zero-copy)

DEL key:
  6. Free buffer (return to pool / arena)
  7. Untrack buffer / delete object
```

**Key property:** Data exists ONLY in DRAM. Dropping an object from the DRAMPool = data loss = equivalent to DEL. Today two things do it: Valkey's `maxmemory-policy` (whole-key eviction) and the module's segment shrink under server memory pressure, which deletes the keys on the released segment (§8.3, §8.5). Module-driven eviction of individual objects from the DRAMPool object map is planned (§8.1).

### 5.2 DRAM + NVMe Mode

All objects persist on NVMe (write-through). DRAM is a read cache — hot objects promoted on GET only.

```
BLOB.SET key len <payload> [rkey remote_addr]:
  1. If key has existing DRAMPool entry: free that buffer (invalidate stale data)
  2. Alloc buffer (registered memory)
  3a. TCP: copy payload into buffer
  3b. EFA: fi_read from client GPU into buffer (zero-copy)
  4. io_uring WriteFixed to NVMe (O_DIRECT, 4KB-aligned length via object_disk_len)
  5. Free buffer (no DRAMPool caching on write path)
  6. Track file in key/object: key → NVMe location only

BLOB.GET key [rkey remote_addr len] — DRAMPool hit:
  7. Lookup in HashMap → buffer is cached in DRAMPool
  8a. TCP: reply from buffer
  8b. EFA: fi_write from buffer (zero-copy)

BLOB.GET key [rkey remote_addr len] — DRAMPool miss (serve-and-discard, no promotion):
  9. Alloc buffer from NVMePool (registered memory)
  10. io_uring ReadFixed from NVMe into NVMePool buffer (O_DIRECT)
  11a. TCP: reply from NVMePool buffer
  11b. EFA: fi_write from NVMePool buffer (zero-copy)
  12. Free NVMePool buffer

BLOB.GET key [rkey remote_addr len] — DRAMPool miss + promotion (promotion policy says YES):
  9. Alloc ObjectContext with N buffers directly in a DRAMPool segment, insert as Filling
  10. io_uring ReadFixed from NVMe directly into the DRAMPool buffers (no memcpy, no NVMePool involvement)
  11. Mark ObjectContext Ready when complete; serve the client from it:
      11a. TCP: reply from DRAMPool buffers
      11b. EFA: fi_write from DRAMPool buffers (zero-copy)
  12. Buffers stay — they ARE the cached object (served directly on subsequent hits)
      Concurrent GETs coalesce on this Filling ObjectContext (§7.3.5)

Eviction (DRAMPool pressure):
  13. Free buffer. Data safe on NVMe.
  14. Remove DRAMPool pointer from HashMap (keep NVMe reference)

DEL key:
  15. Free buffer (if cached in DRAM)
  16. Delete NVMe file
  17. Remove from HashMap
```

**Key properties:**
- SET always invalidates any stale DRAMPool entry then writes to NVMe. No caching on write path.
- DRAMPool is populated only on the GET path (promotion). Admission policy is a single decision point at the start of the promotion path (steps 9–12). Promotion reads directly into DRAMPool buffers via ReadFixed — no memcpy, no intermediate NVMePool buffer (§7.3.4).
- DRAMPool is expendable. Eviction is cheap (data persists on NVMe). Cache miss costs one NVMe read (~15μs on i8ge).

---

## 6. Data Type Struct and Object References

### 6.1 LoValue (Per-Key Metadata)

Stored in Valkey's keyspace via the module data type. One per LO key. ~20 bytes. Serialized to RDB.

```rust
#[derive(Debug)]  // not Clone: each LoValue is one counted object (INFO num_objects)
pub struct LoValue {
    pub object_id: ObjectId,          // Monotonic per-node OID (used as NVMe filename)
    pub len: u64,                     // Object size in bytes (exact)
    pub crc32c: Crc,                  // Integrity checksum (verified on replication pull)
    pub file: Option<Arc<ObjectFile>>,// On-disk handle (Some in Tiered, None in Dram).
                                      // NOT serialized — rebuilt on load.
}
```

The three durable fields (`object_id`, `len`, `crc32c`) are serialized to RDB. The
`file` handle is runtime-only (an `Arc<ObjectFile>` tracking on-disk existence and,
lazily, an open read fd) and is never serialized or reconstructed on load. No other
runtime state (DRAM cache location, flags) lives on LoValue — those are in
module-internal structures:
- **FdPool:** `OIDIndexedMap<FdEntry>` (each entry wraps an `Arc<OwnedFd>` plus an LFU `AccessStats` score) — rebuilt on load, not serialized (§6.4)
- **DRAMPool:** `ObjectMaps` — `Arc<ObjectContext>` per oid plus a per-segment oid index; buffers in DRAMPool segments. Tiered: populated by GET promotion, demoted independently. Dram: populated by SET
- **NVMePool inflight:** transient `StreamingContext` per in-flight request — buffers in NVMePool segments, dropped on completion
- **Allocators:** one `Mutex<Talc>` **per segment** (inside each `Segment`), in both pools — no shared cross-segment allocator (§4.5, §8.7)

### 6.2 ObjectContext, StreamingContext, and SegmentBuffer

Module-internal runtime companions to LoValue. Not serialized — rebuilt on load, evicted independently of commands.

```rust
// A descriptor of a slice within a segment. NOT refcounted, NO Drop — freed
// explicitly by the owning context's Drop (or pool.free on error paths).
// NOT Clone/Copy: duplicating it means a real buffer copy, via `TryClone`.
#[derive(Debug)]
struct SegmentBuffer {
    segment_idx: u16,      // Pool-local segment index (distinct from the global iovec_index)
    offset: u64,           // Byte offset within that segment
    len: u32,              // This chunk's requested size
}
// Always within a registered segment → ReadFixed + EFA fi_write capable

struct ObjectContext {
    buffers: Vec<SegmentBuffer>, // ALL chunks (complete object). Allocated from DRAMPool.
    state: AtomicU8,             // ObjectState: Ready | Filling
    chunks_ready: AtomicU32,     // Advances per batch during promotion fill
    stats: AccessStats,          // LFU access score for the cache policy
}
// Neither context stores total_len or chunk counts: they derive from LoValue.len
// and chunk-size.

// ObjectState values encoded in the AtomicU8:
//   Ready    — fully filled, servable
//   Filling  — promotion in progress (§7.3.4); chunks_ready tracks progress

struct StreamingContext {
    buffers: Vec<SegmentBuffer>, // Rotating window of X buffers. Allocated from NVMePool.
    // NOTE: progress lives in the task-local ChunkIterator, not here.
    // NOTE: the SET CRC32c is a LOCAL variable in the tokio SET task,
    //       NOT a field on this struct.
}
```

**ObjectContext (DRAMPool — long-lived, complete):**
- ALL N buffers for the entire object allocated upfront from DRAMPool segment
- `state = Filling` during promotion (§7.3.4): batched ReadFixed fills buffers, `chunks_ready` advances per batch
- `state = Ready`: all buffers filled, object servable
- A GET that finds `Filling` does not wait: Tiered reads NVMe independently; the Dram EFA path is `todo!()` (§7.3.5)
- Stored in: `DRAMPool.objects` (`ObjectMaps`)

**StreamingContext (NVMePool — short-lived, partial window):**
- X buffers (batch size), reused across batches
- Used for: SET writes to NVMe, GET serve-and-discard (no promotion)
- Freed entirely after operation completes

**SegmentBuffer:**
- Segment-agnostic: works for both DRAMPool and NVMePool segments
- Same struct regardless of lifetime or pool; a descriptor with no `Drop`
- `segment_idx` is the pool-local slot index; the io_uring `buf_index` is looked up at submit time via `iovec_index_for_buf` (it changes on every table rebuild, §8.6)

### 6.3 NVMe File Reference

Each object version is one file, named directly from its `ObjectId` under the
configured `nvme-dir`: `{nvme-dir}/{oid:016x}.dat` (via `ObjectId::file_path`).
There is no extra subdirectory — `nvme-dir` is the module-owned directory. The
runtime handle is `ObjectFile { object_id, disk_len }` (held behind
`Arc<ObjectFile>`); it stores no path and no fd — the path is derived from the OID
and the fd is managed separately by the FdPool (§6.4).

**File header (`FILE_HEADER_SIZE` = 4096 bytes at start of file; data starts at
offset 4096 for O_DIRECT alignment):**
```rust
// consts in storage/nvme.rs
pub const FILE_HEADER_SIZE: u64 = 4096;
pub const FILE_HEADER_MAGIC: &[u8; 4] = b"LOBJ";
pub const FILE_HEADER_VERSION: u8 = 1;
// Packed wire size, derived from field types (NOT hand-counted):
pub const FILE_HEADER_WIRE_LEN: usize =
    size_of::<[u8; 4]>() + size_of::<u8>() + size_of::<u64>()
    + size_of::<u64>() + size_of::<u32>();
const _: () = assert!(FILE_HEADER_WIRE_LEN <= FILE_HEADER_SIZE as usize);

pub struct FileHeader {
    pub magic: [u8; 4],     // b"LOBJ" — identifies file as Large Object module data
    pub version: u8,        // 1 — enables future format changes
    pub object_id: u64,     // Matches LoValue.object_id
    pub len: u64,           // True object length (before O_DIRECT padding)
    pub crc32c: Crc,        // Integrity checksum (same as LoValue.crc32c)
}
```

The header is serialized to the page on write and deserialized on read for magic/version validation. It is never byte-cast, so no `#[repr(C)]` is needed.

**File layout:**
```
[0..WIRE_LEN):          FileHeader fields (little-endian, packed)
[WIRE_LEN..4096):       Zero padding (aligns data start to 4KB for O_DIRECT)
[4096..4096+len):       Object data
[4096+len..):           O_DIRECT write padding to 4KB boundary
```

Object data starts at offset 4096 so all ReadFixed/WriteFixed operations on the
data portion are naturally 4KB-aligned. The header page is read only during
recovery/reconciliation — never on the hot serving path (LoValue in the keyspace
carries all metadata needed to serve).

**Runtime references:**
- The read fd is opened lazily on the first GET and cached in FdPool as `Arc<OwnedFd>` (SET opens its own private fd, not via FdPool — §6.4)
- Lookup: `fd_pool.get_or_open(object_id, nvme_dir)` → `Arc<OwnedFd>` for io_uring submission
- File size = `4096 + ceil(len / 4096) * 4096` (header page + O_DIRECT-padded data)
- Actual object length stored in `LoValue.len` (hot path) and `FileHeader.len` (recovery path)
- On DEL/free: `ObjectFile::Drop` removes the fd from FdPool (closes on last ref) and unlinks the file

### 6.4 FdPool (File Descriptor Management)

Caches open NVMe read file descriptors, keyed by `ObjectId`, so repeated GETs on a
hot object reuse one fd instead of re-opening per request. The base structure is a
map of reference-counted owned fds; the cap and eviction policy layer on top of it
without changing that primitive.

```rust
struct FdEntry {
    fd: Arc<OwnedFd>,    // The owned fd; in-flight readers hold clones of this Arc
    stats: AccessStats,  // packed LFU score (counter + last-decay minute)
}

struct FdPool {
    fds: RwLock<OIDIndexedMap<FdEntry>>, // IndexMap: dense storage enables sampled reclaim
    reclaims: AtomicU64,                 // fds dropped to stay under max-cached-fds
}
// The cap is the `max-cached-fds` config (0 = unlimited), read at call time.
```

The lifetime-safety primitive is `Arc<OwnedFd>`: each in-flight read holds a clone,
so an fd removed from the map — by DEL or by eviction — stays valid until the last
reader finishes, and `OwnedFd` closes it via RAII on the final drop. **Everything
else is policy built on this primitive; none of it can cause a use-after-close.**

**Operations:**
- **GET:** `fd_pool.get_or_open(oid, nvme_dir)` — read-lock fast path returns a clone
  of the cached `Arc<OwnedFd>` and bumps its `AccessStats`; on a miss it
  takes the write lock, re-checks (double-checked locking), `libc::open`s the file
  (`O_RDONLY`, plus `O_DIRECT` when `direct-io` is on), wraps it in `Arc<OwnedFd>`,
  inserts an `FdEntry`, and returns a clone. Returns `None` only on a genuine
  `open()` failure.
- **SET:** does **not** use FdPool — the write path opens its own private `OwnedFd`
  that closes when the SET task's scope ends.
- **DEL/free:** `ObjectFile::Drop` calls `fd_pool.remove(oid)`, dropping the map's
  `Arc<OwnedFd>`. The fd closes once the last in-flight-read clone is also dropped.

**Cap and eviction (LFU):** when an insert would exceed `max-cached-fds`, the pool
samples up to `reclaim-sample-size` entries and drops the lowest decayed LFU score
(`reclaim_one`, the same `AccessStats` the DRAMPool cache uses). Eviction is just a
map removal and does **not** check `Arc::strong_count`: a reader still holding a
reclaimed fd keeps it open through its own clone, and `OwnedFd` closes it when that
read finishes — exactly as for DEL.

### 6.5 ObjectContext Lifetimes

ObjectContext exists in two layers with different lifetimes. Same struct, same SegmentBuffer type, but allocated from **separate talc instances in separate segments** (§4.5).

**DRAMPool (long-lived):**
- ObjectContext created on cache promotion (BLOB.GET hit policy admits it)
- Buffers allocated from a DRAMPool segment via `dram_pool.alloc()` (picks a segment, mallocs from that segment's own talc)
- Held in `HashMap<ObjectId, ObjectContext>` for the object's entire cached lifetime
- Buffers remain allocated and serve repeated BLOB.GET hits directly
- On DRAMPool eviction (policy-based — LRU/LFU/memory pressure): ObjectContext dropped → `dram_pool.free()` for each buffer (frees into the owning segment's talc)
- Object survives on NVMe. Next GET is a cache miss (NVMePool serves it).

**NVMePool (short-lived):**
- StreamingContext created per in-flight I/O request
- Buffers allocated from an NVMePool segment via `nvme_pool.alloc()` (segment's own talc)
- On request completion: StreamingContext dropped → `nvme_pool.free()` for each buffer
- If promotion policy says yes: separate ReadFixed directly into DRAMPool buffers (§7.3.4). No memcpy from NVMePool. NVMePool buffers freed independently after serving the current request.

### 6.6 Relationship Diagram

Example: 50MB object cached in DRAMPool (long-lived). NVMePool would look the same structurally but with short-lived StreamingContexts in NVMePool segments.

```
Valkey keyspace                Module internals
──────────────                 ────────────────
key "obj-A"
  └─ LoValue {oid=42,         fd_pool.get_or_open(42) → Arc<OwnedFd> → {nvme-dir}/000000000000002a.dat (50MB)
       len=50MB,                                                ▲
       crc32c=0xAB12}                                           │  N buffers : 1 file
                                                                │  (parallel ReadFixed/WriteFixed
                               object_contexts[42] → ObjectContext    at different file offsets)
                                 buffers: [                     │
                                   buf[0] {seg=0, off=0x0000, 8MB}  ──→ file offset 0MB
                                   buf[1] {seg=0, off=0x80_0000, 8MB} → file offset 8MB
                                   buf[2] {seg=1, off=0x0000, 8MB}  ──→ file offset 16MB
                                   buf[3] {seg=0, off=0x100_0000, 8MB} → file offset 24MB
                                   buf[4] {seg=1, off=0x80_0000, 8MB} → file offset 32MB
                                   buf[5] {seg=0, off=0x180_0000, 8MB} → file offset 40MB
                                   buf[6] {seg=1, off=0x100_0000, 2MB} → file offset 48MB
                                 ]
                                 total_len: 50MB
                                                     │
                    ┌────────────────────────────────┘
                    ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ DRAMPool Segment 0 (1GiB, io_uring buf_index=0)               │
  │ [...buf[0]...][...buf[1]...][...buf[3]...][...buf[5]...]    │
  └─────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────┐
  │ DRAMPool Segment 1 (1GiB, io_uring buf_index=1)               │
  │ [...buf[2]...][...buf[4]...][...buf[6]...]                  │
  └─────────────────────────────────────────────────────────────┘
```

---


## 7. Large Object I/O: Multi-Buffer Parallel

Objects can be much larger than a single I/O buffer (e.g., 10GB object with 64MB buffers). The module handles this by streaming through multiple buffers in parallel — never allocating the full object in DRAM at once.

**Transport-dependent behavior:**
- **TCP:** Valkey's command dispatch accumulates the full payload in `querybuf` before calling the module handler. Replies use single-allocation `VM_ReplyWithStringBuffer`. There is no incremental/streaming API. **TCP rejects objects above `max-object-size`.** Multi-buffer parallel I/O applies only to EFA.
- **EFA:** The module controls chunk size via async transport.read/transport.write. Multi-buffer parallel I/O is the primary large object path.

### 7.1 NVMe Representation: Single File Per Object

Each object is one contiguous file on NVMe regardless of size:

```
Object "key123" (10GB):
  NVMe: {nvme-dir}/000000000000002a.dat   (10GB file, XFS extent-allocated)
  LoValue: {oid=42, len=10GB, crc32c=0xAB12}
```

**Why single file, not multiple:**
- io_uring parallelizes via offset within one fd — no need for multiple files
- NVMe controller sees LBAs, not files. Same parallelism either way.
- Atomicity: single unlink = atomic delete. No partial-object cleanup.
- Simpler fd management, RDB serialization, and error handling.
- mdraid0 stripes across all drives regardless of file count.

### 7.2 Buffer Size Translation (Client Args → Server Chunks)

The server decides chunk size — the client never specifies or sees it.

**Server config:** `chunk-size` (e.g., 8MB). This determines the allocation unit for all I/O operations.

**BLOB.SET translation:**
```
Client sends:  BLOB.SET key 50MB <payload or rkey+addr+len>
Server sees:   total_len=50MB, chunk_size=8MB → N=7 chunks
Server does:   alloc 7 buffers from shared segments
               partition incoming data into 8MB pieces
               submit 7 parallel WriteFixed SQEs to NVMe
```

**BLOB.GET translation:**
```
Client sends:  BLOB.GET key [rkey addr 50MB]
Server sees:   LoValue.len=50MB, chunk_size=8MB → N=7 chunks
Server does:   alloc 4-8 buffers (pipeline depth)
               submit ReadFixed SQEs at file offsets 0, 8MB, 16MB, ...
               TCP: write each chunk to reply buffer sequentially (client sees one bulk string)
               EFA: transport.write each chunk to client at addr + i*chunk_size
```

**Key invariant:** The client provides `total_len` and a destination (TCP socket or EFA address). The server partitions into `ceil(total_len / chunk-size)` internal operations. The last operation uses `len = total_len % chunk-size` (partial chunk). O_DIRECT write path pads the final write to the 4KB boundary on disk (§4.6). The chunk boundary is invisible to the client protocol.

### 7.3 Chunked Streaming I/O

All I/O operations (SET and GET) use the same chunked streaming pattern. "Full pipeline" and "degraded streaming" are the same code path — the difference is how many buffers are available (max X vs min Y). There is no separate "parallel I/O" path.

#### 7.3.1 Core Pattern: Batched Submission with Oneshot Bridge

Every chunked I/O operation runs as a tokio task with X buffers (the batch/pipeline depth). The task fans out a batch of up to X source reads and hands each chunk to its target (NVMe write / client transfer) as soon as that chunk's read completes — produce and consume interleave within the batch (`stream::drive_window`, `FuturesUnordered`). The batch fully drains before its X buffers are reused for the next batch. The pseudocode below shows the batching, not the interleave.

```rust
async fn stream_batched(buffers: &mut [SegmentBuffer], fd: RawFd, total_len: u64, chunk_size: usize) {
    let x = buffers.len();  // batch size
    let total_chunks = ceil(total_len, chunk_size);
    
    for batch_start in (0..total_chunks).step_by(x) {
        let batch_end = min(batch_start + x, total_chunks);
        let batch_size = batch_end - batch_start;
        
        // One call to io layer — submits all X SQEs as a single batch
        let rx = io_layer.submit_batch(
            fd,
            &buffers[..batch_size],
            base_offset: batch_start * chunk_size,
            chunk_size,
        );  // internally: queues X SQEs → one io_uring_submit() syscall
        
        // Await batch completion (all X CQEs reaped)
        rx.await;
        
        // Batch complete: all X buffers are now filled (read) or flushed (write)
        // Process results, reuse all X buffers for next batch
    }
}
```

**Key invariant:** After awaiting a batch, buffers `[0..batch_size]` are ALL complete. Progress advances by `batch_size * chunk_size` bytes atomically. No partial-batch state. The io layer handles CQE ordering internally — the tokio task sees only "batch done" or "batch failed."

**Pipeline depth = batch size = X = number of buffers allocated for this operation.**

#### 7.3.2 Context Types

See §6.2 for full struct definitions. Summary:

- **StreamingContext** (NVMePool): rotating window of X buffers, used for SET writes and GET serve-and-discard. Short-lived.
- **ObjectContext** (DRAMPool): complete allocation of ALL N buffers, with `ObjectState` (`Ready` or `Filling{chunks_ready, total}`). Long-lived. Coalescing point for concurrent GETs during promotion.

#### 7.3.3 BLOB.SET Flows

**SET + EFA + NVMe mode:**
```
Main thread: validate, fallocate NVMe file, BlockedClient
Spawn tokio task with StreamingContext (X buffers from NVMePool):

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Fill X buffers from client via EFA
    for i in 0..batch_size:
      transport.read(client_addr + (batch_start+i)*chunk_size, buffer[i], chunk_size).await
      crc_hasher.update(buffer[i][..chunk_len])
    
    // Single batch submission to io layer (one io_uring_submit syscall)
    io_layer.submit_batch(fd, &buffers[..batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion
    
    // Batch done. All X buffers reusable for next batch.

  Finalize: verify CRC. Match → create LoValue, unblock OK. Mismatch → unlink, unblock ERR.
  Free all StreamingContext buffers back to NVMePool.
```

**SET + TCP + NVMe mode:**
```
Main thread: validate (payload already in querybuf), fallocate NVMe file, BlockedClient
Spawn tokio task with StreamingContext (X buffers from NVMePool):

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Fill X buffers from querybuf (memcpy — data already in memory)
    for i in 0..batch_size:
      memcpy(buffer[i], querybuf + (batch_start+i)*chunk_size, chunk_len)
      crc_hasher.update(buffer[i][..chunk_len])
    
    // Single batch submission + await
    io_layer.submit_batch(fd, &buffers[..batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion

  Finalize: verify CRC, create LoValue, unblock.
```

**SET + EFA + DRAM-only mode:**
```
Main thread: validate, BlockedClient
Spawn tokio task — alloc N buffers from DRAMPool segment (final storage):

  for i in 0..N:
    transport.read(client_addr + i*chunk_size, dram_buffers[i], chunk_size).await
    crc_hasher.update(dram_buffers[i])

  Verify CRC → create LoValue + ObjectContext{buffers, state=Ready}, unblock.
  (Buffers stay — they ARE the cached object. No free.)
```

**SET + TCP + DRAM-only mode:**
```
Main thread (synchronous — no tokio task needed):
  Alloc N buffers from DRAMPool segment
  for i in 0..N:
    memcpy(dram_buffers[i], querybuf + i*chunk_size, chunk_len)
    crc_hasher.update(dram_buffers[i])
  Verify CRC → create LoValue + ObjectContext{buffers, state=Ready}, reply OK.
```

#### 7.3.4 BLOB.GET Flows

**GET + DRAMPool hit (both transports):**
```
ObjectContext.state == Ready:
  EFA: transport.write(buffer[i], chunk_len, client_addr + i*chunk_size, rkey).await for each chunk
  TCP: VM_ReplyWithStringBuffer from ObjectContext buffers (or reject if > TCP threshold)
  No I/O. No StreamingContext. Direct serve from DRAMPool.
```

**GET + DRAMPool miss + NO promotion (serve and discard):**
```
Spawn tokio task with StreamingContext (X buffers from NVMePool):

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Single batch ReadFixed submission
    io_layer.submit_read_batch(fd, &buffers[..batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion
    
    // Batch done — all X buffers filled. Send to client:
    for i in 0..batch_size:
      EFA: transport.write(buffer[i], chunk_len, client_addr + (batch_start+i)*chunk_size, rkey).await
      TCP: write to reply buffer (if within TCP threshold)
    
    // All X buffers reusable for next batch.

  Unblock client. Free StreamingContext buffers.
```

**GET + DRAMPool miss + promotion (direct fill into DRAMPool):**
```
1. Alloc FULL ObjectContext in DRAMPool segment (N buffers for entire object)
2. Insert into DRAMPool HashMap with state = Filling{chunks_ready: 0, total: N}
3. Spawn tokio task — reads directly into DRAMPool buffers in batches:

  for batch_start in 0..total_chunks step X:
    batch_size = min(X, total_chunks - batch_start)
    
    // Single batch ReadFixed directly into DRAMPool buffers
    io_layer.submit_read_batch(fd, &dram_buffers[batch_start..batch_start+batch_size], base_offset=batch_start*chunk_size, chunk_size)
    await batch_completion
    
    // Batch complete — advance contiguous progress
    object_context.state = Filling{ chunks_ready: batch_start + batch_size, total: N }
    // Wake coalesced waiters: they can now serve bytes [0..(batch_start+batch_size)*chunk_size]

4. Set state = Ready. Wake all remaining waiters.
5. Serve the original request from the now-complete ObjectContext.
```

**Why batched for promotion:** `chunks_ready` advances by X at a time (one batch). After a batch completes, all chunks `[0..chunks_ready]` are contiguous and valid. Waiters can safely serve `[0..chunks_ready * chunk_size]` — no holes, no out-of-order risk. CQEs within a batch may arrive in any order; we wait for the whole batch before advancing.

#### 7.3.5 Coalescing During Promotion

**Not implemented.** A Tiered GET that finds its object `Filling` reads NVMe
independently (`cmd_get_tiered`, `TODO: coalesce`), so concurrent GETs during a
promotion duplicate the NVMe read. The Dram EFA GET's `Filling` arm is `todo!()`.

Planned design:

```
GET arrives → ObjectContext exists, state = Filling:
  - Do NOT start a new NVMe read or allocate NVMePool buffers
  - Register as a waiter on this ObjectContext
  - Wake and serve when chunks_ready covers the request (or state = Ready)
```

All concurrent GETs for a key would then share the single fill: the `Filling` map entry
is the coalescing point, with no separate singleflight structure. It needs a
waiter/notify channel on `ObjectContext`; `advance_chunks_ready` advances the counter
but notifies no one today.

#### 7.3.6 Buffer Budget

| Config | Default | Meaning |
|---|---|---|
| `max-buffers-per-op` (X) | 8 | Max buffers per operation = batch size. All X submitted simultaneously. |
| `min-buffers-per-op` (Y) | 2 | Min buffers to start (below = reject). Y=2 enables double-buffering. |

> **Note (open tuning item):** the values above are placeholders. The maximum
> buffers per operation (X) and the chunk size (`chunk-size`) still need to be
> chosen empirically to optimize throughput/performance — they trade pipeline
> depth against per-op pool pressure and SQE count, and the sweet spot depends on
> object-size distribution and device behavior. To be settled with benchmarks.

**X = batch size = pipeline depth.** Each batch keeps up to X chunks in flight and drains completely before its buffers are reused.

Decision logic on NVMePool alloc:
- Got X buffers → full pipeline speed (8 concurrent SQEs per batch)
- Got ≥ Y but < X → proceed at reduced batch size (degrades gracefully)
- Got < Y → reject with `ERR NVMe staging buffer pool exhausted`

Pool exhaustion mid-stream: impossible. Buffers are allocated once at the start of the operation and reused across batches. The loop never allocates mid-flight.

#### 7.3.7 Data Correctness

- **CRC32c (SET/write path):** TCP computes it in one pass over the inline payload; EFA records each chunk's transport CRC as its `fi_read` completes and combines them in chunk order (`ChunkIterator::combine_checksums`)
- **CRC32c (GET/read path)** the expected value is known upfront (`LoValue.crc32c`), so verification is a single equality check against the file header's `crc32c` — not accumulated over the streamed bytes. It confirms byte integrity, not offset layout.
- **LoValue** created ONLY after all chunks written AND CRC verified
- **Client disconnect mid-SET:** unlink partial file, no LoValue created
- **Server crash mid-SET:** orphan file without LoValue → reconciliation deletes
- **Invariant:** `LoValue exists ⟺ NVMe file is complete AND CRC-verified`
- **Promotion correctness:** ObjectContext in `Filling` state is visible but only servable up to `chunks_ready * chunk_size` bytes. `chunks_ready` advances by batch (X chunks at a time) — never partial batches. Waiters blocked on content beyond `chunks_ready` wait for the next batch to complete.

### 7.4 Chunk Size Selection

| Chunk size | Buffers for 10GB | SQE count | Tradeoff |
|---|---|---|---|
| 4MB | 2500 (sequential) | 2500 | Minimal pool usage, high SQE overhead |
| 64MB | 160 (sequential) | 160 | Good balance |
| 256MB | 40 (sequential) | 40 | Fewer SQEs, larger pool reservation |

With pipelining (4–8 buffers in flight), only 4–8 buffers are checked out at once regardless of object size. Total SQE count determines total I/O time; pipeline depth determines pool pressure.

**Recommended:** chunk_size = `chunk-size` config value. No special "large object" buffer — reuse the same shared segment allocator. The chunking is purely an I/O scheduling pattern, not a storage decision.

### 7.5 DRAMPool for Large Objects

Objects larger than `max-promote-size` are **never promoted to DRAMPool**:
- Cost/benefit is poor (a large object evicts many smaller hot objects from cache)
- Promotion threshold is configurable: `max-promote-size` (default: 256MB; `0` disables promotion)
- Objects above this threshold always read from NVMe via the parallel pipeline
- Objects below this threshold can be promoted to DRAMPool on repeated access (§5.2 promotion path, §7.3.4)

### 7.6 EFA Transport for Large Objects

**Both cases resolve to the same thing: an upfront chunk→client-memory mapping.**
The server always chunks the object by `chunk-size` (chunk `i` covers logical
bytes `[i*chunk_size, (i+1)*chunk_size)`). Before any I/O, it computes a plan mapping
each chunk to where it lands in client memory — `chunk_index → [(rkey, addr, len), …]`
— using inputs all known at command time (`total_len`, `chunk-size`, and the
client's destination). The I/O loop then just executes that plan. The two cases
differ only in what the destination is: a single address (Case 2) or a list of
addresses (Case 1).

Two cases for how EFA handles large objects:

**Case 1: Client provides multiple address/len pairs in the command**

The command includes multiple client addresses:

```
BLOB.SET key <total_len> <rkey1 addr1 len1> <rkey2 addr2 len2> ...
BLOB.GET key <rkey1 addr1 len1> <rkey2 addr2 len2> ...
```

- The server treats the addresses as **one logical contiguous destination** (address1
  then address2 …) and chunks that logical space by `chunk-size` — the server
  owns the chunking; the client's address sizes need no alignment.
- The upfront plan maps each chunk to its address(es): a chunk that fits inside one
  address is one transport post; a chunk that **straddles** an address boundary is split
  into two posts (tail of one address + head of the next). This is the only extra work
  Case 1 adds over Case 2.
- Client memory layout is just a destination map — it does **not** control the
  server's chunking or parallelism.
- Use case: client has multiple GPU registrations (multi-GPU, or several buffers on
  one GPU).

**Case 2: Client provides a single large address/len that exceeds comfortable buffer size**

The client provides one address larger than the server's buffer size. This is the
**trivial instance of the same plan** — one destination address, so every chunk maps
to exactly one post with no straddling: chunk `i` → `addr + i*chunk_size`. Two
sub-options:

- **Reject:** Return ERR if `len > max_efa_transfer_size`. Simple, forces client to use Case 1.
- **Accept and split (preferred — product requirement):** Server internally splits the single large address into chunk-sized fi_write/fi_read calls at sequential offsets within the client's address:

```
Client provides: rkey=R, remote_addr=A, len=10GB
Server internally:
  transport.write(buf[0], chunk_size, A + 0*chunk_size, R).await
  transport.write(buf[1], chunk_size, A + 1*chunk_size, R).await
  transport.write(buf[2], chunk_size, A + 2*chunk_size, R).await
  ...
```

- Transparent to client — single registration, single addr, server handles the chunking
- Server pipelines: NVMe ReadFixed fills buffer[i], fi_write sends it, buffer returned to pool
- No API change from the small-object case — same command syntax, server detects large size and splits

**v1 decision:** Reject over TCP for large objects. Over EFA both cases are implemented: a command takes up to 256 `(rkey, addr, len)` triples (`MAX_EFA_ADDRESSES`); `ChunkIterator` maps chunks onto them, splitting a chunk that straddles an address boundary. Case 2 is the one-triple case. There is no `max_efa_transfer_size`: the Reject sub-option was not built.

### 7.7 TCP Path: Large Object Rejection

Valkey's RESP command dispatch accumulates the full payload in `client->querybuf` before calling the module handler. Replies use single-allocation `VM_ReplyWithStringBuffer`. There is no incremental streaming API for either direction.

**Consequence:** A 10GB BLOB.SET over TCP requires 10GB in querybuf before the module even runs. This is untenable.

**v1 behavior:**
- `BLOB.SET`: reject with `ERR max object size exceeded` if payload > `max-object-size` (configurable, applies to all modes and transports)
- `BLOB.GET`: no size check — a stored object is served even if `max-object-size` was later lowered
- Over TCP this is the only size bound (querybuf cannot stream); EFA clients are additionally chunked via multi-buffer parallel I/O (Cases 1/2 above) but remain subject to `max-object-size`

**Future (v2+):** If Valkey adds a streaming/incremental module API for reading from client socket and writing chunked replies, TCP could support larger objects. Until then, large objects require EFA.

---
## 8. Expanding and Shrinking of Segments

Expanding and shrinking applies only to **DRAMPool segments**. NVMePool segments are fixed at startup (sized for max concurrent I/O) and never resized — if NVMePool is exhausted, the module back-pressures new requests until buffers are freed.

### 8.1 Memory Model (prerequisite)

**1. One shared RAM budget.** **Every** module allocation goes through Valkey's
allocator (zmalloc) — DRAMPool segments, NVMePool staging segments, the FdPool map,
ObjectContext/StreamingContext metadata, the tracking HashMaps — so it all counts
against the server's `used_memory` and shares the single server `maxmemory` ceiling
with the core keyspace. There is no module-local DRAM budget: the DRAMPool grows on
demand and its only ceiling is the server `maxmemory` (§11.2).

**2. Freeing an object does not lower `used_memory`.** An object's buffers return to
its segment's talc; the segment's memory stays allocated. `used_memory` drops only when
a whole segment is released. So core's own eviction cannot relieve pressure caused by
LargeObjects — evicting an LO key just makes room inside a segment. Only the module's
shrink, which releases segments, gives memory back to core.

**3. Three eviction actors.**

| Actor | What it does | Data loss? | Modes |
|---|---|---|---|
| **Core maxmemory eviction** | Valkey's `maxmemory-policy` deletes a victim key (LO keys included) and calls the module free callback. `noeviction` → `BLOB.SET` fails (OOM). Frees space inside a segment, not `used_memory` (fact 2). | **Yes** | Both |
| **Module cache eviction** (`make_room_for`) | Demotes cached DRAM copies to make room for a promotion; the object stays on NVMe. | No | Tiered |
| **Module segment shrink** (`try_shrink`) | Under server memory pressure, releases the least-loaded DRAM segment. | Tiered: no (data on NVMe). Dram: **yes** — the segment's keys are deleted | Both |
| **Module DRAM object eviction** *(planned)* | Evicts individual objects from the DRAMPool object map; their keys are deleted. | **Yes** | Dram |
| **Module NVMe object eviction** *(planned)* | Evicts objects from the filesystem to free NVMe capacity (`nvme-maxmemory`); their keys are deleted. | **Yes** | Tiered |

The two planned actors complete the reclaim family: like Dram shrink, they free an
object's storage first and put its oid on the reclaim list (§8.5), and both feed the
`reclaims` counter. `reclaim-sample-size` and the reclaim-named internals are reserved
for them.

**Detection:**

- **Dram expand:** reactive (SET alloc-miss → expand + retry inline) and proactive (cron, utilization > watermark).
- **Tiered expand:** proactive only (cron). Tiered SETs write to NVMe; the DRAM pool fills only by GET promotion, which returns None on pool-full and serves from NVMe — no inline expand on the GET path.
- **Shrink (both modes):** proactive only (cron observes server memory pressure; nothing "fails" to trigger it).

**Startup allocation.** The DRAMPool starts with **one** segment and grows on demand
(`storage::init`). A deployment that loads the module but never stores a LargeObject
still pays one `segment-size` segment. The alternative — start with zero segments and
allocate (plus EFA-register, ~333µs) on the first `BLOB.SET` — is a pre-ship decision.

### 8.2 When to Expand

Expansion adds one `segment-size` segment (≤1GiB, §2).

| Trigger | Action |
|---------|--------|
| DRAMPool allocation fails (no segment has room) — Dram SET path | Add a segment immediately, then retry the alloc |
| Pool utilization > `scaling-expand-watermark` (cron) | Add a segment proactively |

Both are gated by the server ceiling: an expand that would push `used_memory` past
`scaling-shrink-watermark` × `maxmemory` is refused (`would_cross_memory_watermark`),
so the pool never grows into memory the shrink would immediately take back. With
`maxmemory = 0` growth is unbounded, as in core Valkey. In Dram mode a refused
reactive expand rejects the `BLOB.SET` (OOM); in Tiered mode the cache stops growing,
and a promotion that finds no room is skipped (the GET reads NVMe).

**Why Dram is reactive-primary.** The alloc-fail row grows *because* a request just
needed space. That is acceptable because growth is cheap and safe — add a segment,
retry, continue; the cost is one stalled alloc, nothing is lost. The watermark row is
the proactive complement that removes even that stall. Tiered has no reactive row:
promotion never blocks the GET on pool-full, it just serves from NVMe.

The proactive expand is skipped on a tick where a shrink is still unfinished (§8.3).

### 8.3 When to Shrink

The scaling cron reads `used_memory / maxmemory` on every tick (`scaling-poll-ms`).
Above `scaling-shrink-watermark` it shrinks: it picks the live, non-draining segment
with the fewest allocated bytes, drops every object on it, and releases it (§8.5).

| Mode | Condition | Effect on the victim's objects |
|---|---|---|
| Tiered | ratio > watermark | Cached copies dropped; objects stay on NVMe, next GET reads NVMe. |
| Dram | ratio > watermark **and** the keyspace holds a non-LargeObject key | The objects *are* the data. Their oids go on the reclaim list; the keys read as missing and the cron deletes them (§8.5). |

The Dram non-LO-key gate (`reclaim::has_non_lo_keys`: total `DbSize` across dbs >
`num_objects`) exists because Dram shrink frees memory for other data types: with only
LargeObjects in the keyspace there is nothing to make room for, so the cron deletes
nothing.

**Why proactive only.** Nothing "fails" to prompt a shrink: the trigger is external
(the server approaching `maxmemory`), which the module only sees by polling. An alloc
that fails between ticks takes the normal OOM path (§8.2); there is no reactive
shrink, because reacting to a failure is already too late to reclaim gracefully.

**One shrink at a time.** While a segment is draining or the reclaim list is
non-empty, the cron only releases drained segments and deletes reclaimed keys — no
expand, no new shrink. A SET that needs room meanwhile still expands reactively.

**Victim choice.** Fewest allocated bytes = the least data dropped per segment freed
(in Dram mode, the fewest keys deleted). Recency is not considered yet; the LFU score
only moves on Tiered GET hits.

**NVMe staging is never shrunk** — it is fixed-at-startup I/O buffers, not a cache (§11.4).

### 8.4 How Expansion Works

1. **Allocate** a new segment (`alloc_zeroed`, ≤1GiB). Counted in `used_memory`.
2. **Create the segment's talc:** the new `Segment` owns its own `Talc` and `claim`s
   exactly `[base, base+size)`.
3. **Register with EFA** (`fi_mr_reg` for this segment only, both modes, if a fabric
   is up). Existing MRs are untouched.
4. **Re-register with io_uring** — **Tiered only** (Dram mode has no io_uring
   engine). `submit_reregister(Dram)` asks the DRAM ring to rebuild its whole table
   (§8.6).

The new segment is usable for allocation immediately. Until the rebuild marks it
registered, its I/O goes non-fixed (plain Read/Write).

### 8.5 How Shrinking Works

`DRAMPool::try_shrink` (main thread, scaling cron):

1. **Pick and mark.** `find_shrink_victim` → `mark_segment_draining`. The alloc
   picker skips draining segments, and `get_object` refuses to hand out a new
   `Arc<ObjectContext>` for an object on a draining segment (Tiered GETs fall back to
   NVMe), so no new references enter the segment.
2. **Drop the objects.** `ObjectMaps::remove_by_segment_id` removes every object on
   the victim from the map (the per-segment index makes this one call, no map scan).
   Dropping the map's `Arc` frees an object's buffers as soon as no in-flight reader
   holds a clone.
3. **Dram only: list the oids.** The removed oids go on `RECLAIM_LIST`, under the
   list lock held across step 2 (§9.3). From now on BLOB.GET, BLOB.INFO and COPY treat
   those keys as missing (`LoValue::reclaim_in_progress`).
4. **Release.** The segment's `refcount` counts live allocations (+1 per alloc, −1
   per free). The cron releases every `draining && refcount == 0` segment
   (`release_all_releasable`) at the top of each tick, and once right after a shrink
   so an unheld victim is freed in the same tick. Release takes the segment out of its
   slot and drops it: `Segment::drop` deregisters EFA, then deallocates the memory
   (the segment's talc metadata lives inside it). In Tiered mode the DRAM ring then
   rebuilds its table without the segment (§8.4 step 4). `used_memory` drops.
5. **Dram only: delete the keys.** `reclaim::delete_reclaimed_keys` runs every tick
   while the list is non-empty, re-arming the cron at `reclaim-poll-ms` instead of
   `scaling-poll-ms`. Each tick spends at most `reclaim-scan-budget-us` of main-thread
   time: it scans the keyspace with `VM_Scan`, resuming its db and cursor across
   ticks, and UNLINKs every key whose oid is listed, from inside the scan callback
   (VM_Scan allows deleting the current key). An oid leaves the list when its key is
   deleted, by the cron or any other path (`lo_free` → `remove_object`).

The segment's memory is freed in step 4, independent of step 5: a slow key cleanup
only lengthens the time reclaimed keys stay visible to core commands (EXISTS, SCAN, …).

**Registry slots.** Segments live in `slots: Vec<Option<Segment>>`; release leaves a
`None` hole that the next expand reuses. The slot index is the `segment_idx` stamped
into every `SegmentBuffer`, so it must never change while the segment lives — which is
why release leaves a hole rather than swap-removing the tail segment into the gap
(that would rewrite the tail segment's index under its live buffers). The io_uring
`iovec_index` is a separate number (§8.6).

**Why commands check the reclaim list up front.** In Dram mode a `LoValue` without an
`ObjectContext` is a logic bug (the DRAM GET paths panic on it). Between step 3 and
step 5 that is exactly the state of a reclaimed key, so every command that reaches the
DRAMPool for an existing key checks `reclaim_in_progress()` first. The check and the
shrink both run on the main thread, so a shrink cannot land between them. The EFA
DRAM SET commits from a tokio worker; `commit_lo_value` refuses a commit whose object
`get_object` no longer returns (removed by a shrink, or inserted onto a draining
segment), so a key is never attached to an object that is not served.

### 8.6 io_uring Registration (Tiered)

Each pool has its own ring and its own fixed-buffer table (`IORING_REGISTER_BUFFERS`
is per ring fd). Only the DRAM pool scales, so only the DRAM ring ever re-registers;
the NVMe ring registers once at startup and never stalls.

**Two indexes per segment:**

| Index | Where | Stable? | Used for |
|---|---|---|---|
| slot index (`segment_idx`) | `SegmentPool.slots`, stamped into every `SegmentBuffer` | Yes, for the segment's lifetime | free, pointer lookup, refcount |
| `iovec_index` | the segment's position in its ring's registered table | No — recomputed on every rebuild | `buf_index` of ReadFixed/WriteFixed |

An op resolves `iovec_index` from its segment when it is submitted
(`iovec_index_for_buf`), never from a cached copy.

**Startup.** `UringEngine::new` registers the pool's live segments
(`register_buffers(startup_iovecs)`, positions 0..N) and marks them registered. A
registration failure fails module load.

**Why a whole-table rebuild.** The production kernel floor is 5.10 (AL2). It has no
`register_buffers_update` / sparse tables (5.13+), so the only primitive is
`unregister_buffers` + `register_buffers(&all)`. That destroys and recreates the
kernel's registration object, so **every** in-flight fixed op on the ring — not just
ops on the changed segment — must complete before it runs.

**Rebuild protocol** (poller thread, after `submit_reregister` on expand or release):

1. `IoRequest::Reregister` sets `registration_pending`.
2. **Kill-switch:** while pending, every op is issued non-fixed (plain Read/Write,
   which names an address, not a table index), so no new op can reference the table
   about to change.
3. When `fixed_in_flight` reaches 0, `rebuild_dense_iovecs` reindexes the live
   segments densely from 0 (holes dropped), marks them registered, and the poller runs
   `unregister_buffers` + `register_buffers(dense)`. A failure here panics.
4. `registration_pending` clears; ops go fixed again with their new `iovec_index`.

There is a window where nothing is registered (between the two syscalls). It is safe
because of the drain + kill-switch, not because the swap is atomic. A newly expanded
segment is unregistered until step 3 marks it; `is_buf_io_uring_registered` keeps its
I/O non-fixed until then, so it never issues a fixed op against an index that does not
exist (EFAULT).

**On release** the segment's entry in the pool's `iovecs` is set to `None` and
`Segment::drop` does no io_uring work; the rebuild that follows omits it. Dropping is
safe before the rebuild: `refcount == 0` means no live allocation, so no fixed op can
target it, and the kernel's page pins from the old registration are dropped by the
`unregister_buffers` of that rebuild.

**Kernels ≥ 5.13** (AL2023 6.1+) would allow a sparse table with one-slot
`register_buffers_update`, removing the drain and the rebuild. Not implemented: the
5.10 path is the only one that runs in production.

### 8.7 Segment Selection on Alloc (Per-Segment talc — Chosen)

**Design chosen: one talc allocator per segment**, not one shared talc across all
segments. Each `Segment` owns a `Mutex<Talc<ErrOnOom>>` that claims exactly its own
`[base, base+size)` range. `SegmentPool` no longer has a shared allocator, a
reverse-lookup index, or `sorted_bases`.

Alloc picks a target segment *before* calling malloc:

```rust
// Least-loaded live, non-draining segment (single O(N) min pass, no sort).
// N = segment count (≤1024), not objects. Two relaxed atomic loads per segment.
let seg_idx = slots.iter().enumerate()
    .filter(|(_, s)| eligible(s))          // live, not draining, passes fast byte filter
    .min_by_key(|(_, s)| s.allocated_bytes)?;
// Then lock THAT segment's own talc and malloc from it.
let ptr = seg.talc.lock().malloc(layout)?;
```

Because the segment is known *before* malloc, the returned pointer's owner is known
by construction — `segment_idx` is stamped directly into the `SegmentBuffer`. **There
is no pointer→segment reverse lookup at all.** `free` uses `buf.segment_idx` directly.

**Why per-segment talc was chosen (this reverses the earlier recommendation in this
section — the reasons that flipped it):**

1. **It eliminates the reverse-lookup problem entirely, not just optimizes it.** The
   shared-heap design had to map a bare `malloc`-returned pointer back to a segment
   (talc exposes no owner — verified in talc 4.4.3: `malloc` returns a bare
   `NonNull<u8>`, `claim` dissolves the `Span` into free-list bins and keeps no span
   list). That forced an O(log N) binary search over a `sorted_bases` index
   maintained on every expand/release. Per-segment talc makes the owner known *before*
   malloc, so the entire index and its maintenance disappear. Net **−128 lines**.

2. **Lock parallelism.** The shared talc was one `Mutex<Talc>` serializing every
   alloc/free across all segments. Per-segment talc gives each segment its own lock,
   so concurrent allocs on different segments never contend. (Microbench: per-segment
   is slower at low thread counts by a small constant but scales — 1.3× @24 threads,
   2.7× @64, 5.2× @500 — purely from lock parallelism.)

3. **`claim`/release symmetry — no `talc.truncate` at all.** With per-segment talc,
   each segment's talc metadata lives *inside that segment's own memory*. Releasing a
   drained segment is just `slots[i].take()` + drop — `Segment::drop` deallocs the
   backing memory and the talc vanishes with it. The shared design required
   `talc.truncate(claim_span, Span::empty())` to carve the range out of the shared
   free-list, which needs the exact `claim`-returned `Span` and panics if any live
   allocation remains. Per-segment talc removes that hazard entirely.

**Costs accepted (why they're fine):**
- **Least-loaded pick is O(N) over segments** (N ≤ 1024, two relaxed atomic loads
  each, no allocation, L1-resident). It is dwarfed by the malloc it precedes and the
  NVMe I/O that follows. A heap keyed on load would be O(log N) but must re-sift on
  every free (tokio threads) behind its own lock — reintroducing the contention
  per-segment talc just removed, to optimize a microsecond-scale advisory pick. Not
  worth it. Fewer/larger segments (bigger `segment-size`) is the real lever if N ever
  matters.
- **Loses cross-segment best-fit packing.** talc can only best-fit *within* one
  segment now. Acceptable: chunks are fixed-size (`buffer_size`, 4 KiB-aligned) and
  segments are uniform, so intra-segment packing is regular; the least-loaded picker
  spreads load evenly.

**Fit check.** The picker's byte filter (`allocated_bytes + request > segment size`)
does not count talc's per-chunk overhead on the request, so it can admit a segment
that then cannot fit it. `talc.allocate` is the authoritative, all-or-nothing check:
on `Err` nothing is committed and the allocation fails (a Dram SET then expands
reactively).

**Single-segment ownership invariant (still load-bearing):** every allocation lies
wholly within exactly one segment — trivially guaranteed now, since each segment has
its own talc claiming only its own range and `extend` is never used. One allocation →
one `buf_index` (io_uring ReadFixed/WriteFixed address a single registered buffer),
one segment refcount, one EFA MR. This is what makes the addressing model valid.

### 8.8 NUMA Locality - Will be addressed later. Skip for now.

Cross-NUMA-node memory access is a real cost on the multi-socket hosts this runs on
(i8ge: node0 = CPU 0–95, node1 = CPU 96–191, EFA NIC on node1). The hardware numbers
(measured on i8ge):

| Path | Latency | Bandwidth |
|---|---|---|
| Local (CPU node1 → mem node1) | 123 ns | ~412 GB/s |
| Remote (CPU node1 → mem node0) | 260 ns | ~77 GB/s |
| Cross-socket bandwidth penalty | 2x latency | **5x bandwidth** |

**Two separate NUMA concerns — one settled, one open:**

**1. EFA/NVMe segment placement — settled.**
EFA DMA accesses the registered segment memory directly over PCIe. If segments live on
the far NUMA node, every DMA transfer crosses the inter-socket link at 77 GB/s instead
of 412 GB/s. For EFA bulk streaming at current scale (100K rps 4KB, 50K rps 1MB),
this cross-socket bandwidth cap does not bite — EFA does not approach 77 GB/s at these
object sizes. Kevin confirmed: "doesn't really matter for the NIC streaming."

**Action:** Preferentially allocate DRAMPool/NVMe segments on node1 (the NIC's node).
Mechanism: pin the creating thread to node1 CPUs before `alloc_zeroed` (first-touch
on node1), then `mbind(MPOL_BIND, node1)` the segment range. Read the EFA device's
node at runtime from `/sys/class/infiniband/<dev>/device/numa_node`. Low effort, worth
doing proactively even if not currently a bottleneck — other instance types or higher
QPS may hit the cap.

**2. Valkey core data structures — open.**
The Valkey main thread first-touches all core allocations (dict, hashtable, LoValue
structs), homing them on the main thread's node. Workers on node1 chasing those
pointers pay 260ns per pointer dereference. Whether this materially hurts QPS at
scale has not been measured. Kevin raised this explicitly as unsettled.

One approach discussed: allocate a large enough DRAMPool upfront (>50% of RAM) so
core Valkey data structures are naturally pushed onto node0, while segment memory
stays on node1 — workers do DRAM access locally, EFA DMA is local to the NIC. Kevin
also raised a "two DRAM tiers" idea (segment memory on node1, core structures on
node0, EFA offloads the transfer so CPU doesn't touch it). Neither has been
benchmarked.

> **[Open — needs NUMA-aware benchmark on i8ge to determine whether the 260ns
> cross-node keyspace lookup penalty materially affects QPS at production scale.
> The EFA streaming side is settled; the core data structure side is not.]**

## 9. Concurrency, Refcounting, and Lifecycle

This section describes how shared state is protected, which structures are refcounted, and how the free callback coordinates with in-flight operations.

### 9.1 Refcounting Summary

| Component | Mechanism | Who holds refs | Drop-to-0 action |
|---|---|---|---|
| ObjectContext (DRAMPool) | `Arc<ObjectContext>` | DRAMPool HashMap (1), each in-flight GET reader (1 each) | `Drop` impl: `dram_pool_talc.lock().free()` for each buffer, decrement segment refcounts |
| StreamingContext (NVMePool) | Owned by single tokio task, no Arc needed | The one task that owns it | Task completion: `nvme_pool_talc.lock().free()` for each buffer |
| SegmentBuffer | **Not refcounted** — a plain move/copy descriptor (`segment_idx`, `offset`, `len`), no `Drop` | ObjectContext or StreamingContext (never shared independently) | Freed by the parent context's `Drop` (or explicit `pool.free(&buf)` on error paths) — `talc.free()` + segment refcount decrement |
| ObjectFile | `Arc<ObjectFile>` | LoValue (1), each in-flight GET request (1 each) | `Drop` impl: remove fd from FdPool, `remove_file`, `decrease_nvme_disk_usage` |
| Open fd | `Arc<OwnedFd>` (inside `FdEntry`) | FdPool map (1), each in-flight I/O op (1 each) | RAII: `OwnedFd` closes when last `Arc` drops. The strong count *is* the in-flight count; eviction just drops the map's clone, and a reader's clone keeps the fd open until it finishes |
| Segment | `AtomicU32` refcount + `AtomicBool draining` + own `Mutex<Talc>` | Each live buffer allocated from this segment (+1 per alloc, −1 per free) | Releasable when draining && refcount==0. The scaling cron takes it out of its slot and drops it: `Segment::drop` deregisters EFA, then deallocs the backing memory (the segment's talc metadata lives inside it). Tiered: the DRAM ring then rebuilds its fixed-buffer table without it (§8.4). |

### 9.2 Threading Model: Which Thread Does What

**Rule:** Main thread serves directly only when no NVMe I/O and no EFA transport is needed. All other paths spawn a tokio task and block the client.

**TCP paths:**

| Path | Threads | Why |
|---|---|---|
| GET → DRAMPool hit | Main thread only | Data already in DRAM. RwLock read + reply inline. Zero I/O. |
| GET → DRAMPool miss (Tiered mode) | Main → tokio task | Needs NVMePool buffer alloc + NVMe ReadFixed. BlockedClient until read completes. |
| SET (Tiered mode) | Main → tokio task | Needs NVMePool buffer alloc + NVMe WriteFixed. BlockedClient until write + CRC verified. |
| SET (DRAM-only mode) | Main thread only | DRAMPool alloc + memcpy from querybuf. Synchronous. No NVMe, no tokio. |

Note: DRAM-only mode has no "miss" — all objects live in DRAMPool. A GET on a non-existent key returns key-not-found, not a cache miss.

**EFA paths:**

| Path | Threads | Why |
|---|---|---|
| All EFA commands (GET and SET, both modes) | Main → tokio task | EFA transport (fi_read/fi_write) is async. Always BlockedClient + tokio task. |

**Summary:** Main thread handles only the synchronous fast paths (DRAM hit over TCP, DRAM-only SET over TCP). Everything involving NVMe I/O or EFA transport goes through tokio.

### 9.3 Global State and Lock Table

| Global | Type | Lock | Threads that access |
|---|---|---|---|
| DRAMPool object map | `HashMap<ObjectId, Arc<ObjectContext>>` | `RwLock` | Main (read on GET hit, remove on free callback), tokio (read for coalesce check, write for promotion insert + Filling state update) |
| DRAMPool allocators | one `Mutex<Talc>` **per segment** (inside each `Segment`) | Mutex (per segment) | Main (free callback — talc.free via Arc Drop), tokio (promotion alloc, DRAM-only SET alloc). Allocs on different segments never contend. |
| NVMePool allocators | one `Mutex<Talc>` **per segment** (inside each `Segment`) | Mutex (per segment) | Tokio (alloc for all Tiered I/O — SET and GET miss), Arc Drop from any thread (free on StreamingContext drop) |
| FdPool | `OIDIndexedMap<FdEntry>` (entry = `Arc<OwnedFd>` + LFU `AccessStats`) | `RwLock` | Main (remove via ObjectFile::Drop on DEL/free), tokio (get_or_open lazily on first GET; LFU eviction takes the write lock to remove a cold entry). SET does NOT use FdPool — it opens a private fd. |
| NVMe disk-usage counter | `AtomicU64` (NVME_DISK_USAGE) | lock-free atomic | Tokio (reserve on SET/COPY), any thread (decrement on ObjectFile::Drop / error rollback) |
| Segment registry | `Mutex<SegmentState { slots: Vec<Option<Segment>> }>` (per pool) | Mutex | All threads take it briefly to reach a segment (alloc picker, free, buffer_ptr, iovec lookup). Slots are mutated only by expand/release on the main thread; the Tiered DRAM ring's poller rewrites `iovecs` and each segment's `iovec_index` during a rebuild (§8.6). Each segment's `refcount`/`allocated_bytes`/`draining` are atomics; its talc is a separate per-segment Mutex. |
| Reclaim list | `RECLAIM_LIST: LazyLock<ReclaimList>` (`HashSet<ObjectId>` behind a `Mutex`) | Mutex | Main (add on Dram shrink; `contains` on BLOB.GET/INFO/COPY; cron scan + remove; INFO), BIO (`lo_free` → `remove_object` removes), tokio (refused EFA commit → `remove_object`). Lock order: reclaim list before `DRAMPool.objects`. All readers run on the main thread, so a RwLock would buy nothing. |
| Reclaim scan cursor | `thread_local! RECLAIM_SCAN` (db + `KeysCursor`) | none (main thread only) | Scaling cron |

**Why RwLock for DRAMPool HashMap and FdPool:** GET hit is the hot path — main thread reads frequently. RwLock allows parallel reads. Writes (promotion insert from tokio, remove from main on free callback) are infrequent and take exclusive lock briefly.

**Why Mutex for talc, and why per-segment:** Allocator operations modify internal
free-list state — no concurrent access to one talc is possible. Giving each segment
its own talc Mutex means allocs/frees on *different* segments proceed in parallel;
only same-segment operations serialize. Hold time ~10-50ns. Negligible contention.
(See §8.7 for why per-segment talc was chosen over one shared allocator.)

**Consistency with §9.2:** Main thread never allocs from NVMePool (all Tiered I/O goes through tokio). Main thread allocs from DRAMPool only in DRAM-only SET (synchronous path). Main thread frees via Arc Drop in free callback (which may call the owning segment's talc.free if last ref).

**NVMe disk-usage accounting:** a single process-global `AtomicU64` (`NVME_DISK_USAGE`) tracks bytes committed on NVMe. Writes reserve and increment in one atomic step via `try_reserve_nvme_disk_usage(disk_len)`, which does a checked `fetch_update` against `nvme-maxmemory` and fails the SET if it would exceed the cap (`nvme-maxmemory` of 0 = unlimited). Module-driven NVMe eviction (planned, §8.1) will free space instead of failing. This is the only increment path in production (a separate `increase_nvme_disk_usage` exists but is test-only). Decrements happen on `ObjectFile::Drop` (file deleted) and on every SET error/rollback path (write error, stale-version discard, open failure, RecvError). The reserved/written length is the O_DIRECT-aligned `object_disk_len(obj_len)`, not the raw object length.

### 9.4 Free Callback Lifecycle

When Valkey DELs or evicts a key, the data type's `free` callback (`lo_free`) runs. `free_effort` returns `0`, so Valkey always runs the free lazily on the background (BIO) path rather than inline on the main thread.

`lo_free` itself does almost nothing — it removes the key's DRAM entry and then relies on `Arc` drops to do the real cleanup:

```
lo_free(LoValue):
  0. Reconstruct the Box<LoValue> and drop it at the end of scope.

  1. DRAMPool map: remove_object(oid) → drops the map's Arc<ObjectContext>,
     then takes oid off the reclaim list (map first: a shrink running in between
     no longer sees the object, so cannot list it).
     → If no in-flight GET readers hold clones: ObjectContext::Drop runs now
       → dram_pool.free() for each SegmentBuffer (talc.free + segment refcount decrement).
     → If readers hold clones: Drop is deferred until the last reader finishes,
       on whatever thread drops the last Arc. No blocking.

  2. LoValue drops → its Option<Arc<ObjectFile>> drops.
     → On the last Arc, ObjectFile::Drop performs the disk teardown:
         - FdPool.remove(oid)      → drops the pool's Arc<OwnedFd> (fd closes when
                                      the last in-flight-read clone is also gone)
         - std::fs::remove_file(path)   (path derived from the ObjectId)
         - decrease_nvme_disk_usage(disk_len)
     → Teardown runs inline on the dropping thread, EXCEPT if that thread is the
       Valkey main thread (is_main_thread()), in which case it is spawned onto
       tokio so the main thread never does blocking fs work.
```

**Invariant:** `lo_free` never blocks on in-flight operations. The key stops being visible immediately (new requests get key-not-found). Buffers, fd, and the NVMe file are reclaimed lazily via `Arc` drops — the two-Arc model (`Arc<ObjectFile>` for existence + `Arc<OwnedFd>` for the fd) keeps any in-flight read safe until it completes, and disk teardown is kept off the main thread.

### 9.5 Segment Draining Lifecycle

When the scaling cron shrinks the DRAMPool (§8.5, both modes):

```
1. mark_segment_draining(i)
   — the alloc picker skips draining segments (no new allocations)
   — get_object refuses a new Arc<ObjectContext> on a draining segment
     (Tiered GET falls back to NVMe)

2. remove_by_segment_id(i) drops the map's Arcs (Dram: oids → reclaim list).
   In-flight holders drop theirs as they finish; each buffer free decrements
   the segment refcount.

3. Every cron tick (and right after the shrink): release_all_releasable()
   releases each draining && refcount == 0 segment:
     → slots[i].take()          — leaves a None hole for the next expand
     → drop(segment)            — Segment::drop: EFA deregister, then dealloc;
                                  used_memory decreases
     → submit_reregister(Dram)  — Tiered: rebuild the DRAM ring's table
```

No spin and no blocking wait: release is polled by the cron, so a segment whose last
holder drops between ticks is released on the next tick.

---

## 10. Open Questions

1. Dram shrink review questions (noeviction, keyspace notifications, the non-LO-key
   gate, EFA commit race): `docs/DRAM_SHRINK_REVIEW.md`.
2. Scaling tuning: the shrink/expand watermarks and `scaling-poll-ms` cadence need
   benchmarks, so the cron keeps ahead of memory spikes without thrashing grow/shrink.
3. Dram mode with server `maxmemory = 0`: the only bound is physical RAM and the OOM
   killer. A loud startup warning is proposed, not implemented.
4. Should we use Scale Out and Scale In to handle overly fragmented segments? Requires live transition (drain + evacuate). May be over-engineering — talc free-coalescing may be sufficient. Needs tests to determine fragmentation rate in practice.

---

## 11. Configuration & Memory Model

This section defines what each config knob *means as an operational experience* —
what its values do, how they bound memory, and how they permit the pool to scale
out and in. Mechanics of enforcement live in the sections cross-referenced below;
this section owns the semantics.

### 11.1 Config Catalog

| Config | Default | Min | Max | Live/Immutable | Governs |
|---|---|---|---|---|---|
| `operating-mode` | `Dram` | — | — | Immutable | `Dram` (DRAMPool is the store) vs `Tiered` (NVMe is the store, DRAMPool is a cache) |
| `segment-size` | `1GiB` | `1MB` | `1GiB` | Immutable | Uniform segment size for both pools. DRAMPool growth unit |
| `nvme-staging-size` | `1GiB` | `1MB` | `1GiB` | Immutable | NVMePool staging capacity, split into `ceil(nvme-staging-size / segment-size)` segments of `segment-size`. Sized for max concurrent I/O, not object capacity |
| `nvme-maxmemory` | `0` (unlimited) | `0` | i64::MAX | Live | NVMe **disk** ceiling; SET-admission bound in Tiered (enforcement: §9.3) |
| `nvme-dir` | `""` | — | — | Immutable | NVMe object directory (Tiered) |
| `direct-io` | `yes` | — | — | Immutable | O_DIRECT on NVMe files (must be `no` on tmpfs) |
| `max-promote-size` | `256MB` | `0` (disable) | `1TB` | Live | Promotion eligibility; objects above this never enter DRAMPool (detail: §7.5) |
| `promote-min-hits` | `2` | `1` | `255` | Live | Admission-filter misses before a GET promotes an object |
| `tiered-decay-time` | `1` | `0` (off) | `65535` | Live | LFU decay: minutes per one-point decrement |
| `reclaim-sample-size` | `5` | `1` | `64` | Live | Objects sampled per cache-eviction round; the lowest LFU score is demoted |
| `max-cached-fds` | `1024` | `0` (unlimited) | `1048576` | Live | FdPool cap on cached read fds |
| `max-object-size` | `512MB` | `1` | i64::MAX | Live | Global per-object cap; SET rejected above it (detail: §4.6, §7.7) |
| `chunk-size` | `8MB` | `4096` | `256MB` | Immutable | I/O chunk / allocation unit (detail: §7.2) |
| `max-buffers-per-op` / `min-buffers-per-op` | 8 / 2 | 2 / 1 | 64 / 64 | Live | Streaming pipeline depth (detail: §7.3.6) |
| `worker-threads` | `2` | `1` | `32` | Immutable | tokio worker threads |
| `scaling-poll-ms` | `5000` | `1000` | `60000` | Live | Scaling cron interval: how often it checks utilization and memory pressure |
| `scaling-expand-watermark` | `80` | `50` | `95` | Live | Pool utilization % above which the cron adds a segment proactively |
| `scaling-shrink-watermark` | `90` | `50` | `95` | Live | Server `used_memory / maxmemory` % above which the cron shrinks a segment; also the ceiling expand may not cross |
| `reclaim-poll-ms` | `100` | `10` | `60000` | Live | Cron interval while the reclaim list is non-empty (Dram shrink key cleanup); `scaling-poll-ms` resumes once it is empty |
| `reclaim-scan-budget-us` | `1000` | `100` | `100000` | Live | Main-thread time per cron tick spent scanning for and deleting reclaimed keys |
| `smartlog-poll-secs` | `60` | `0` (off) | `86400` | Immutable | NVMe SMART poll interval (Tiered) |
| `fabric-provider` | `Emulated` | — | — | Immutable | libfabric provider: `Emulated` (tcp provider) or `EfaDirect` (EFA hardware) |
| `fabric-interfaces` | `""` (all) | — | — | Immutable | Comma-separated fabric domains to open a service on |
| `fabric-max-in-flight` | `0` (provider default) | `0` | `65536` | Immutable | Transfers each fabric service keeps in flight |
| `fabric-crc-pool-threads` | `1` | `1` | `1024` | Immutable | Threads hashing checksummed transfers off the fabric workers |

**Segment size is capped at 1 GiB** for both `segment-size` and
`nvme-staging-size`. This is the `IORING_REGISTER_BUFFERS` per-buffer limit (§2):
NVMe staging is always io_uring-registered, and DRAMPool is io_uring-registered in
Tiered mode (for promotion ReadFixed). Larger capacity comes from *more* segments,
never bigger ones. (EFA `fi_mr_reg` itself has no such cap — a Dram-mode segment is
EFA-only — but we cap uniformly at 1 GiB for a single mental model.)

### 11.2 The Ceiling Is Always Valkey `maxmemory`

DRAMPool segments count against the server's shared `used_memory` and compete with
the core keyspace under one `maxmemory` ceiling (§8.1). There is no module-local DRAM
budget: the pool grows on demand, and an expand that would cross
`scaling-shrink-watermark` × `maxmemory` is refused. With `maxmemory = 0` the only
bound is physical RAM, as in core Valkey.

### 11.3 What Hitting the Ceiling Means, per Mode

| Mode | At the ceiling |
|---|---|
| Dram | A `BLOB.SET` that needs a new segment is rejected (OOM). If the keyspace also holds non-LargeObject keys, the cron shrinks a segment and deletes its keys so core data types fit (§8.3). |
| Tiered | The cache stops growing; the cron shrinks segments (cached copies dropped, data stays on NVMe). |

Segment **count** is always derived from demand, never a knob. Every segment is
exactly `segment-size`.

### 11.4 Budget Semantics

- **Tiered mode:** DRAMPool is a cache, reclaimable under pressure without data loss.
  NVMe capacity is a hard SET bound today; module-driven NVMe eviction is planned (§8.1).
- **Dram mode:** DRAMPool is the data. Under server memory pressure, with non-LO keys
  present, the cron deletes the keys on the least-loaded segment (§8.5); core's
  `maxmemory-policy` evicts whole keys independently but cannot lower `used_memory`
  by itself (§8.1 fact 2). Module-driven per-object eviction is planned (§8.1).

**NVMe staging is not a scalable budget.** It sizes transient I/O buffers for
concurrency, is fixed at startup and never shrinks.

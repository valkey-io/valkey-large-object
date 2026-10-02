//! Storage Layer — DRAMPool + NVMePool + io_uring I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crc_fast::CrcAlgorithm;

/// A CRC32C checksum value. Aliased so a `Crc` in a signature reads as "this
/// integer is a checksum", not a length or an id. It is a `u32` on the wire and
/// in the `FileHeader`/`LoValue`; the `checksum_combine` accumulator widens to
/// `u64` internally, which is a crc-fast API detail, not this type.
pub type Crc = u32;
pub mod cache_policy;
pub mod context;
pub mod dram_pool;
pub mod fd_pool;
pub mod nvme;
pub mod nvme_pool;
pub mod object_file;
pub mod scaling;
pub mod segment;
pub mod segment_pool;
pub mod uring;

// Re-exports for convenience.
pub use context::{ObjectContext, SegmentBuffer, StreamingContext};
pub use object_file::ObjectFile;

// Re-exports from nvme.rs
pub use nvme::{
    object_disk_len, open_nvme_file_for_write, read_and_verify_file_header,
    validate_and_clean_nvme_dir, write_file_header, FileHeader, FILE_HEADER_MAGIC,
    FILE_HEADER_SIZE, FILE_HEADER_VERSION, FILE_HEADER_WIRE_LEN,
};

// Re-export for crate-internal use only.
pub(crate) use nvme::warn_failed_unlink;

pub use dram_pool::DRAMPool;
pub use fd_pool::FdPool;
pub use nvme_pool::NVMePool;

/// O_DIRECT / io_uring alignment requirement (XFS default block size).
/// Both buffer address and I/O length must be multiples of this.
pub const IO_ALIGN: usize = 4096;

/// Round up to IO_ALIGN boundary. Used by the allocator (buffer size)
/// and the uring layer (I/O length) to satisfy O_DIRECT requirements.
pub fn align_up(n: usize) -> usize {
    (n + IO_ALIGN - 1) & !(IO_ALIGN - 1)
}

/// Whether an object of `obj_len` is guaranteed allocatable in a single empty
/// segment. `alloc_exact` co-locates every chunk of an object in one segment,
/// and talc charges one IO_ALIGN block of boundary tag per allocation plus one
/// for the segment's heap header.
pub fn object_fits_segment(segment_size: usize, obj_len: usize, chunk_size: usize) -> bool {
    if obj_len == 0 {
        return true;
    }
    if chunk_size == 0 {
        return false;
    }
    let chunks = obj_len.div_ceil(chunk_size);
    // What we allocate: each chunk is aligned up to IO_ALIGN for O_DIRECT.
    let full = (chunks - 1).saturating_mul(align_up(chunk_size));
    let last = align_up(obj_len - (chunks - 1) * chunk_size);
    // What talc needs: one IO_ALIGN boundary tag per allocation plus one for
    // the segment's heap header.
    let slack = chunks.saturating_add(1).saturating_mul(IO_ALIGN);
    full.saturating_add(last).saturating_add(slack) <= segment_size
}

/// User data length for a given chunk. All chunks are `chunk_size` except
/// the last, which may be shorter (the remainder of `total_len / chunk_size`).
pub(crate) fn chunk_user_data_len(
    chunk_index: usize,
    total_chunks: usize,
    total_len: usize,
    chunk_size: usize,
) -> usize {
    if chunk_index == total_chunks - 1 {
        let rem = total_len % chunk_size;
        if rem == 0 {
            chunk_size
        } else {
            rem
        }
    } else {
        chunk_size
    }
}

// ─── TryClone Trait ──────────────────────────────────────────────────────────

/// Fallible deep-copy. Like Clone but returns None on failure modes when not possible.
pub trait TryClone: Sized {
    fn try_clone(&self) -> Option<Self>;
}

// ─── Error Types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum StorageError {
    IoError { code: i32 },
    PoolExhausted,
    ObjectTooLarge,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError { code } => write!(f, "I/O error (code {})", code),
            Self::PoolExhausted => write!(f, "buffer pool exhausted"),
            Self::ObjectTooLarge => write!(f, "object exceeds max size"),
        }
    }
}

// ─── Global Pool Instances ───────────────────────────────────────────────────

use std::sync::OnceLock;

/// Maximum number of registered segments in ANY single pool.
/// In tiered mode, each pool (DRAM, NVMe) has its OWN ring and OWN iovecs table,
/// so this bounds each table independently.
/// `init()` validates each pool's startup count against it, and
/// `SegmentPool::expand` enforces it on every runtime growth (a `u16`
/// iovec_index also caps a table at 65536; 16384 is the tighter io_uring
/// IORING_MAX_REG_BUFFERS dense-table size).
pub const MAX_SEGMENTS: usize = 16_384;

pub(super) static DRAM_POOL: OnceLock<DRAMPool> = OnceLock::new();
pub(super) static NVME_POOL: OnceLock<NVMePool> = OnceLock::new();
pub(super) static FD_POOL: OnceLock<FdPool> = OnceLock::new();

pub fn get_dram_pool() -> &'static DRAMPool {
    DRAM_POOL.get().expect("DRAMPool not initialized")
}

pub fn get_nvme_pool() -> &'static NVMePool {
    NVME_POOL.get().expect("NVMePool not initialized")
}

pub fn get_fd_pool() -> &'static FdPool {
    FD_POOL.get().expect("FdPool not initialized")
}

// ─── Initialization ──────────────────────────────────────────────────────────

/// Initialize storage layer: validate config, create pools, spawn the io_uring
/// and SMART log pollers (Tiered only).
/// All OnceLock statics are set at the very end after everything succeeds.
/// On failure, local variables drop naturally — no cleanup needed, module load retryable.
/// Returns Ok(summary string) on success, Err(message) on validation/environment failure.
pub fn init(mode: crate::OperatingMode, nvme_dir: &str) -> Result<String, String> {
    let dram_seg_size = crate::dram_segment_size();
    let nvme_staging = crate::nvme_staging_size();
    // DRAMPool always starts with 1 segment and grows on demand — reactively
    // when an allocation can't fit, and proactively via the scaling cron. Growth
    // is gated by server `maxmemory` (unbounded when maxmemory is 0, like core
    // Valkey).
    let dram_segment_count = 1;
    // NVMe staging is split into uniform `segment_size` segments (based on startup configs).
    // Ceiling division so total staging capacity is never below the requested
    // nvme-staging-size (floor would under-provision: 100MB / 64MB = 1 seg = 64MB).
    let nvme_segments: usize = if mode == crate::OperatingMode::Tiered {
        (nvme_staging.div_ceil(dram_seg_size)).max(1)
    } else {
        0
    };
    if dram_segment_count > MAX_SEGMENTS || nvme_segments > MAX_SEGMENTS {
        return Err(format!(
            "too many segments (DRAM {}, NVMe {}). \
             Max {} per pool. \
             Increase segment-size or decrease nvme-staging-size",
            dram_segment_count, nvme_segments, MAX_SEGMENTS,
        ));
    }
    // ── Create all resources as locals (no OnceLock yet) ──
    // NVMePool + FdPool: only needed in Tiered mode.
    let nvme_pool = if mode == crate::OperatingMode::Tiered {
        Some(NVMePool::new(nvme_segments, dram_seg_size))
    } else {
        None
    };
    let fd_pool = if mode == crate::OperatingMode::Tiered {
        Some(FdPool::new())
    } else {
        None
    };
    // DRAMPool: always needed (both modes).
    let policy = (mode == crate::OperatingMode::Tiered).then(cache_policy::CachePolicy::new);
    let dram_pool = DRAMPool::new(dram_segment_count, dram_seg_size, policy);
    // io_uring engines: only in Tiered mode. TWO independent engines — one for
    // the DRAM ring, one for the NVMe ring — each registering ONLY its own pool's
    // segments. Ring creation + buffer registration happen on this (main) thread
    // so failures return Err, not panic in the poller. On failure the local pools
    // drop naturally, so a module-load retry starts fresh.
    let engines = if mode == crate::OperatingMode::Tiered {
        let nvme_pool_ref = nvme_pool.as_ref().expect("NVMePool exists in Tiered mode");
        let dram_engine =
            uring::UringEngine::new(uring::PoolType::Dram, dram_pool.startup_iovecs())
                .map_err(|e| format!("io_uring DRAM engine: {}", e))?;
        let nvme_engine =
            uring::UringEngine::new(uring::PoolType::Nvme, nvme_pool_ref.startup_iovecs())
                // dram_engine drops here (Drop sets its shutdown flag) as the Err
                // short-circuits before we move it into `engines`.
                .map_err(|e| format!("io_uring NVMe engine: {}", e))?;
        Some((dram_engine, nvme_engine))
    } else {
        None
    };
    // ── All succeeded — commit to globals. No failure possible after this point. ──
    if let Some(pool) = nvme_pool {
        if NVME_POOL.set(pool).is_err() {
            panic!("NVMePool already initialized");
        }
    }
    if let Some(pool) = fd_pool {
        if FD_POOL.set(pool).is_err() {
            panic!("FdPool already initialized");
        }
    }
    if DRAM_POOL.set(dram_pool).is_err() {
        panic!("DRAMPool already initialized");
    }
    if let Some((dram_engine, nvme_engine)) = engines {
        uring::set_engine(uring::PoolType::Dram, dram_engine);
        uring::set_engine(uring::PoolType::Nvme, nvme_engine);
        // Each engine's constructor ran IORING_REGISTER_BUFFERS over its own
        // pool's startup segments, so those segments ARE in that ring's kernel
        // buffer table: mark them registered so their I/O uses the fixed
        // (ReadFixed/WriteFixed) path. Segments added later by expand() stay
        // unregistered until the post-expand rebuild flips them. Tiered-only —
        // Dram mode has no io_uring engine, so the flag is never consulted.
        DRAM_POOL
            .get()
            .expect("DRAMPool set above")
            .mark_all_registered();
        NVME_POOL
            .get()
            .expect("NVMePool set above")
            .mark_all_registered();

        // Background SMART log poller: reads the controllers once per interval;
        // INFO only ever serves the latest snapshot. First read populates it.
        // smartlog-poll-secs 0 disables polling and its INFO section.
        let smartlog_secs = crate::smartlog_poll_secs();
        if smartlog_secs > 0 {
            crate::smartlog::start_poller(std::time::Duration::from_secs(smartlog_secs));
        }
    }
    Ok(format!(
        "mode={:?} nvme_dir={} dram_segments={}x{}MB nvme_staging={}MB",
        mode,
        nvme_dir,
        dram_segment_count,
        dram_seg_size / (1024 * 1024),
        nvme_staging / (1024 * 1024),
    ))
}

/// Get combined iovecs for transport registration (fi_mr_reg per segment).
pub fn all_segment_slices() -> Vec<&'static [u8]> {
    let mut slices = Vec::new();
    if let Some(nvme_pool) = NVME_POOL.get() {
        nvme_pool.with_live_segment_slices(|base, size| {
            slices.push(unsafe { std::slice::from_raw_parts(base, size) });
        });
    }
    get_dram_pool().with_live_segment_slices(|base, size| {
        slices.push(unsafe { std::slice::from_raw_parts(base, size) });
    });
    slices
}

/// Rebuild ONE pool's dense io_uring fixed-buffer table from its live segments,
/// reassigning each a dense pool-local `iovec_index`, marking it registered, and
/// returning the iovec array for that ring's `register_buffers`. Dense because
/// 5.10 can only re-register the whole table. Only `PoolType::Dram` re-registers
/// in practice; NVMe is fixed-size.
pub fn rebuild_dense_iovecs_for(pool: uring::PoolType) -> Vec<libc::iovec> {
    match pool {
        uring::PoolType::Dram => get_dram_pool().rebuild_dense_iovecs(),
        uring::PoolType::Nvme => unreachable!("NVMe ring never reregisters"),
    }
}

// ─── Chunk / ChunkIterator ───────────────────────────────────────────────────

/// A client EFA memory address: (remote_addr, size, rkey).
/// Each registered memory region on the client has its own rkey.
/// Used for both user-provided addresses (from command args) and internally
/// computed addresses (from ChunkIterator's incremental mapping).
pub type ClientEFAAddress = (u64, usize, u64);

/// A lightweight reference to one chunk of the object — its position, which
/// window buffer holds it, and (for EFA) the client memory regions to transfer
/// into. Pure metadata: it does NOT own the chunk's bytes, and the stream driver
/// clones one per chunk into its per-chunk futures.
///
/// INVARIANT — keep this type cheap to clone. It is cloned once per chunk on the
/// batch-snapshot hot path, so it must never grow a field that carries a real or
/// large allocation (payload bytes, buffers, big owned collections). Only small
/// coordinates belong here: indices, lengths, and the short `addrs` region list.
/// If you need heavyweight per-chunk state, store it in the owning context/pool
/// and reference it by `buffer_idx`, not inline here.
#[derive(Debug, Clone)]
pub struct ChunkRef {
    /// Absolute chunk index within the object (0-based).
    pub index: u32,
    /// Exact user data bytes in this chunk.
    pub user_data_len: usize,
    /// Index into the owning context's Vec<SegmentBuffer>.
    /// For ObjectContext: buffer_idx == chunk_index (1:1).
    /// For StreamingContext: buffer_idx == chunk_index % num_buffers (rotating).
    pub buffer_idx: usize,
    /// Per-chunk EFA transfer addresses. None for TCP paths.
    /// Each entry is a ClientEFAAddress (remote_addr, len, rkey) — one fi_write/fi_read per entry.
    /// Populated incrementally by ChunkIterator::next_chunk().
    pub addrs: Option<Vec<ClientEFAAddress>>,
}

/// Task-local iterator over chunks. One per tokio task.
/// Handles chunk geometry, buffer index rotation, and incremental client
/// address mapping. Does NOT own buffers — indexes into the owning context's
/// Vec<SegmentBuffer>.
pub struct ChunkIterator {
    /// Pre-computed chunk metadata (user_data_len, buffer_idx). Addresses populated lazily.
    chunks: Vec<ChunkRef>,
    /// Next chunk to return.
    cursor: usize,
    /// Client-provided EFA remote memory addresses. None for TCP paths.
    client_efa_addrs: Option<Vec<ClientEFAAddress>>,
    /// Index into client_efa_addrs for the current address being consumed.
    addr_idx: usize,
    /// Byte offset within the current client_efa_addrs entry.
    addr_offset: usize,
    /// Per-chunk CRC32C from EFA transport completions. Indexed by chunk index.
    /// Only used on SET paths (record_checksum + combine_checksums); GET paths
    /// already have the stored CRC and never write to this vec.
    /// None for TCP paths (CRC computed inline via rolling digest).
    /// Some(...) for EFA paths; inner `None` entries indicate chunks not yet recorded.
    checksums: Option<Vec<Option<Crc>>>,
}

impl ChunkIterator {
    /// Create a new ChunkIterator.
    /// - `user_len`: total user data size in bytes (must be > 0).
    /// - `chunk_size`: chunk-size config value (must be > 0).
    /// - `num_buffers`: number of buffers in the owning context.
    ///   For ObjectContext (all buffers upfront): num_buffers == total_chunks.
    ///   For StreamingContext (rotating window): num_buffers == window size.
    /// - `client_addrs`: Flattened EFA (addr, size, rkey) entries. None for TCP.
    ///
    /// Creates all Chunk metadata upfront (user_data_len, buffer_idx).
    /// Client address mapping is deferred to next_chunk() calls.
    pub fn new(
        user_len: u64,
        chunk_size: usize,
        num_buffers: usize,
        client_addrs: Option<Vec<ClientEFAAddress>>,
    ) -> Self {
        assert!(user_len > 0, "ChunkIterator: user_len must be > 0");
        assert!(chunk_size > 0, "ChunkIterator: chunk_size must be > 0");
        assert!(num_buffers > 0, "ChunkIterator: num_buffers must be > 0");
        let total_chunks = user_len.div_ceil(chunk_size as u64) as u32;
        let mut chunks = Vec::with_capacity(total_chunks as usize);
        for i in 0..total_chunks {
            chunks.push(ChunkRef {
                index: i,
                user_data_len: chunk_user_data_len(
                    i as usize,
                    total_chunks as usize,
                    user_len as usize,
                    chunk_size,
                ),
                buffer_idx: i as usize % num_buffers,
                addrs: None,
            });
        }
        let is_efa = client_addrs.is_some();
        Self {
            chunks,
            cursor: 0,
            client_efa_addrs: client_addrs,
            addr_idx: 0,
            addr_offset: 0,
            checksums: if is_efa {
                Some(vec![None; total_chunks as usize])
            } else {
                None
            },
        }
    }

    /// Total number of chunks for the object.
    pub fn total_chunks(&self) -> u32 {
        self.chunks.len() as u32
    }

    /// Advance to the next chunk, populating its client regions if EFA.
    /// Returns None when all chunks have been consumed.
    pub fn next_chunk(&mut self) -> Option<&ChunkRef> {
        if self.cursor >= self.chunks.len() {
            return None;
        }
        let idx = self.cursor;
        self.cursor += 1;
        // Incremental client address mapping (EFA only).
        // Skip if addresses were already populated (e.g. re-iteration after reset_cursor).
        if self.chunks[idx].addrs.is_some() {
            return Some(&self.chunks[idx]);
        }
        if let Some(efa_addrs) = &self.client_efa_addrs {
            let mut remaining = self.chunks[idx].user_data_len;
            let mut addrs = Vec::new();
            while remaining > 0 {
                if self.addr_idx >= efa_addrs.len() {
                    unreachable!(
                        "ChunkIterator: client addresses exhausted with {} bytes remaining in chunk {} \
                         — command-level validation should have rejected this",
                        remaining, idx
                    );
                }
                let (base_addr, total_size, rkey) = efa_addrs[self.addr_idx];
                let avail = total_size - self.addr_offset;
                if avail == 0 {
                    self.addr_idx += 1;
                    self.addr_offset = 0;
                    continue;
                }
                let take = remaining.min(avail);
                addrs.push((base_addr + self.addr_offset as u64, take, rkey));
                self.addr_offset += take;
                remaining -= take;
            }
            self.chunks[idx].addrs = Some(addrs);
        }
        Some(&self.chunks[idx])
    }

    /// Random access to a chunk by absolute index. Does NOT advance cursor.
    /// Used by completion handlers to look up chunk metadata (buffer_idx, addrs)
    /// after next_chunk() populated it during batch submission.
    pub fn peek_chunk(&self, idx: u32) -> &ChunkRef {
        &self.chunks[idx as usize]
    }

    /// Reset cursor to the beginning. Used when re-iterating (e.g. collect_dram_bytes).
    /// Does NOT reset client address mapping state — addresses already populated stay.
    pub fn reset_cursor(&mut self) {
        self.cursor = 0;
    }

    /// Record a per-chunk CRC32C from an EFA transport completion.
    /// Chunks may arrive out of order; the checksum is stored by chunk index.
    pub fn record_checksum(&mut self, chunk_index: u32, crc: Crc) {
        self.checksums
            .as_mut()
            .expect("record_checksum called on TCP path")[chunk_index as usize] = Some(crc);
    }

    /// Inter-chunk accumulation: combine each chunk's per-chunk CRC (from
    /// `record_checksum`) in chunk order into the whole-object CRC32C.
    /// Only valid for EFA paths. Panics if any chunk's checksum has not been recorded.
    pub fn combine_checksums(&self) -> Crc {
        let checksums = self.checksums.as_ref().expect("checksums not initialized");
        let mut combined: u64 = 0;
        for (i, chunk) in self.chunks.iter().enumerate() {
            let crc =
                checksums[i].unwrap_or_else(|| panic!("chunk {i} checksum not recorded")) as u64;
            if i == 0 {
                combined = crc;
            } else {
                combined = crc_fast::checksum_combine(
                    CrcAlgorithm::Crc32Iscsi,
                    combined,
                    crc,
                    chunk.user_data_len as u64,
                );
            }
        }
        combined as u32
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── ChunkIterator: geometry ─────────────────────────────────────────

    #[test]
    fn test_chunk_iter_single_chunk_exact() {
        // obj_len == chunk_size -> exactly 1 full chunk.
        let mut it = ChunkIterator::new(4096, 4096, 1, None);
        assert_eq!(it.total_chunks(), 1);
        let c = it.next_chunk().unwrap();
        assert_eq!(c.user_data_len, 4096);
        assert_eq!(c.buffer_idx, 0);
        assert!(it.next_chunk().is_none());
    }

    #[test]
    fn test_chunk_iter_single_byte() {
        // Smallest possible object: 1 byte -> 1 chunk of 1 byte.
        let mut it = ChunkIterator::new(1, 4096, 1, None);
        assert_eq!(it.total_chunks(), 1);
        let c = it.next_chunk().unwrap();
        assert_eq!(c.user_data_len, 1);
    }

    #[test]
    fn test_chunk_iter_exact_multiple() {
        // obj_len is an exact multiple of chunk_size -> all chunks are full.
        let mut it = ChunkIterator::new(16384, 4096, 4, None);
        assert_eq!(it.total_chunks(), 4);
        for i in 0..4 {
            let c = it.next_chunk().unwrap();
            assert_eq!(c.user_data_len, 4096);
            assert_eq!(c.buffer_idx, i);
        }
        assert!(it.next_chunk().is_none());
    }

    #[test]
    fn test_chunk_iter_partial_last_chunk() {
        // obj_len = 3 * chunk_size + 1 -> last chunk has 1 byte.
        let mut it = ChunkIterator::new(12289, 4096, 4, None);
        assert_eq!(it.total_chunks(), 4);
        assert_eq!(it.next_chunk().unwrap().user_data_len, 4096);
        assert_eq!(it.next_chunk().unwrap().user_data_len, 4096);
        assert_eq!(it.next_chunk().unwrap().user_data_len, 4096);
        assert_eq!(it.next_chunk().unwrap().user_data_len, 1);
    }

    #[test]
    fn test_chunk_iter_large_object() {
        // 50 MB object with 8 MB chunks -> 7 chunks, last has 2 MB.
        let obj_len = 50 * 1024 * 1024u64;
        let chunk_size = 8 * 1024 * 1024;
        let mut it = ChunkIterator::new(obj_len, chunk_size, 7, None);
        assert_eq!(it.total_chunks(), 7);
        for _ in 0..6 {
            assert_eq!(it.next_chunk().unwrap().user_data_len, chunk_size);
        }
        assert_eq!(it.next_chunk().unwrap().user_data_len, 2 * 1024 * 1024);
    }

    #[test]
    fn test_chunk_iter_buffer_rotation() {
        // 5 chunks but only 2 buffers -> rotating indices.
        let mut it = ChunkIterator::new(20480, 4096, 2, None);
        assert_eq!(it.total_chunks(), 5);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 0);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 1);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 0);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 1);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 0);
    }

    // ─── ChunkIterator: incremental client region mapping ────────────────

    #[test]
    fn test_map_single_contiguous_address() {
        // Single client address covering the full object.
        let cr = vec![(0x1000u64, 8193usize, 42u64)];
        let mut it = ChunkIterator::new(8193, 4096, 3, Some(cr));
        assert_eq!(it.total_chunks(), 3);
        // Chunk 0: full chunk.
        let c0 = it.next_chunk().unwrap();
        let a0 = c0.addrs.as_ref().unwrap();
        assert_eq!(a0.len(), 1);
        assert_eq!(a0[0], (0x1000, 4096, 42));
        // Chunk 1: full chunk.
        let c1 = it.next_chunk().unwrap();
        let a1 = c1.addrs.as_ref().unwrap();
        assert_eq!(a1.len(), 1);
        assert_eq!(a1[0], (0x1000 + 4096, 4096, 42));
        // Chunk 2: partial last chunk (1 byte).
        let c2 = it.next_chunk().unwrap();
        let a2 = c2.addrs.as_ref().unwrap();
        assert_eq!(a2.len(), 1);
        assert_eq!(a2[0], (0x1000 + 8192, 1, 42));
    }

    #[test]
    fn test_map_multiple_addresses_chunk_straddling() {
        // Two addresses under one rkey: 5000 + 3193 bytes.
        // Object is 8193 bytes with chunk_size=4096 -> 3 chunks.
        // Chunk 1 straddles both addresses.
        let cr = vec![(0x1000u64, 5000usize, 7u64), (0x2000, 3193, 7)];
        let mut it = ChunkIterator::new(8193, 4096, 3, Some(cr));
        // Chunk 0: single address from first entry.
        let c0 = it.next_chunk().unwrap();
        let a0 = c0.addrs.as_ref().unwrap();
        assert_eq!(a0.len(), 1);
        assert_eq!(a0[0], (0x1000, 4096, 7));
        // Chunk 1: straddles two addresses.
        let c1 = it.next_chunk().unwrap();
        let a1 = c1.addrs.as_ref().unwrap();
        assert_eq!(a1.len(), 2);
        assert_eq!(a1[0], (0x1000 + 4096, 904, 7)); // remaining from addr 0
        assert_eq!(a1[1], (0x2000, 3192, 7)); // from addr 1
                                              // Chunk 2: single address from second entry.
        let c2 = it.next_chunk().unwrap();
        let a2 = c2.addrs.as_ref().unwrap();
        assert_eq!(a2.len(), 1);
        assert_eq!(a2[0], (0x2000 + 3192, 1, 7));
    }

    #[test]
    fn test_tcp_path_no_addrs() {
        // TCP path: no client addresses configured.
        let mut it = ChunkIterator::new(8192, 4096, 2, None);
        let c0 = it.next_chunk().unwrap();
        assert!(c0.addrs.is_none());
        let c1 = it.next_chunk().unwrap();
        assert!(c1.addrs.is_none());
    }

    #[test]
    fn test_reset_cursor() {
        let mut it = ChunkIterator::new(8192, 4096, 2, None);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 0);
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 1);
        assert!(it.next_chunk().is_none());
        it.reset_cursor();
        assert_eq!(it.next_chunk().unwrap().buffer_idx, 0);
    }

    #[test]
    fn test_record_and_combine_single_chunk() {
        let data = b"hello world";
        let whole_crc = crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, data) as u32;
        // Pass Some(addrs) to enable per-chunk checksums (EFA path).
        let addrs = vec![(0x1000u64, data.len(), 1u64)];
        let mut iter = ChunkIterator::new(data.len() as u64, 4096, 1, Some(addrs));
        iter.next_chunk(); // advance past the single chunk
        iter.record_checksum(0, whole_crc);
        assert_eq!(iter.combine_checksums(), whole_crc);
    }

    #[test]
    fn test_record_and_combine_multi_chunk() {
        let chunk_size = 4;
        let data = b"abcdefghij"; // 10 bytes -> 3 chunks: 4, 4, 2
        let whole_crc = crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, data) as u32;
        // Pass Some(addrs) to enable per-chunk checksums (EFA path).
        let addrs = vec![(0x1000u64, data.len(), 1u64)];
        let mut iter = ChunkIterator::new(data.len() as u64, chunk_size, 3, Some(addrs));
        // Record per-chunk CRCs (simulating out-of-order arrival).
        let crc1 = crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, &data[4..8]) as u32;
        iter.record_checksum(1, crc1);
        let crc2 = crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, &data[8..10]) as u32;
        iter.record_checksum(2, crc2);
        let crc0 = crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, &data[0..4]) as u32;
        iter.record_checksum(0, crc0);
        assert_eq!(iter.combine_checksums(), whole_crc);
    }

    #[test]
    #[should_panic(expected = "checksum not recorded")]
    fn test_combine_panics_on_missing_checksum() {
        let addrs = vec![(0x1000u64, 100usize, 1u64)];
        let mut iter = ChunkIterator::new(100, 50, 2, Some(addrs));
        iter.record_checksum(0, 123);
        // chunk 1 not recorded — should panic.
        iter.combine_checksums();
    }
}

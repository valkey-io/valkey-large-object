//! Storage Layer — DRAMPool + NVMePool + io_uring I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crate::data_type::ObjectId;
use std::mem::size_of;

pub mod context;
pub mod dram_pool;
pub mod fd_pool;
pub mod nvme_pool;
pub mod object_file;
pub mod scaling;
pub mod segment;
pub mod segment_pool;
pub mod uring;

// Re-exports for convenience.
pub use context::{ObjectContext, SegmentBuffer, StreamingContext};
pub use object_file::ObjectFile;

/// O_DIRECT / io_uring alignment requirement (XFS default block size).
/// Both buffer address and I/O length must be multiples of this.
pub const IO_ALIGN: usize = 4096;

/// Round up to IO_ALIGN boundary. Used by the allocator (buffer size)
/// and the uring layer (I/O length) to satisfy O_DIRECT requirements.
pub fn align_up(n: usize) -> usize {
    (n + IO_ALIGN - 1) & !(IO_ALIGN - 1)
}

/// O_DIRECT-aligned on-disk size of an object with `logical_len` payload bytes.
/// Shared helper function to ensure no drift between expected and actual file sizes.
pub fn object_disk_len(logical_len: u64) -> u64 {
    align_up(logical_len as usize) as u64
}
pub use dram_pool::DRAMPool;
pub use fd_pool::FdPool;
pub use nvme_pool::NVMePool;

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

use std::sync::{Mutex, OnceLock};

/// Global sparse iovec table. Slot `i` = iovec_index for io_uring ReadFixed/WriteFixed.
/// `None` = empty slot (no page pinned, no buffer registered at this index).
/// `Some((ptr, len))` = live segment registered at this index.
///
/// Grows as segments are added via `append_iovec`. Bounded by `u16::MAX` (65535)
/// since iovec_index is u16 — in practice a handful of entries.
/// Per-slot updates via `clear_iovec` mirror the io_uring sparse table model:
/// nulling a slot costs nothing (no page pinning for null entries).
static IOVECS: Mutex<Vec<Option<(usize, usize)>>> = Mutex::new(Vec::new());

/// Called by SegmentPool when creating each segment.
/// Fills the first `None` hole in the sparse table (or appends if no hole).
/// This matches the "first None hole, else append" policy used by
/// `SegmentPool::expand` when placing the new segment in `slots`, so the
/// returned `iovec_index` always equals the segment's slot index. Callers
/// depend on `segment.iovec_index == slot_idx`; using a different policy here
/// would silently violate that invariant when the two Vecs have holes in
/// different positions.
///
/// Only invoked from the main event-loop thread — no cross-thread contention
/// over which hole to fill.
pub fn append_iovec(iov: libc::iovec) -> u16 {
    let mut iovecs = IOVECS.lock().expect("IOVECS lock unavailable");
    let entry = Some((iov.iov_base as usize, iov.iov_len));
    match iovecs.iter().position(|s| s.is_none()) {
        Some(i) => {
            iovecs[i] = entry;
            u16::try_from(i).expect("iovec index overflow (>65535)")
        }
        None => {
            let i = iovecs.len();
            iovecs.push(entry);
            u16::try_from(i).expect("iovec index overflow (>65535)")
        }
    }
}

/// Called by SegmentPool during segment drain completion.
/// Nulls the sparse slot so the io_uring registration can be cleared.
pub fn clear_iovec(iovec_index: u16) {
    let mut iovecs = IOVECS.lock().expect("IOVECS lock unavailable");
    let idx = iovec_index as usize;
    if idx < iovecs.len() {
        iovecs[idx] = None;
    }
}

pub(super) static DRAM_POOL: OnceLock<DRAMPool> = OnceLock::new();
pub(super) static NVME_POOL: OnceLock<NVMePool> = OnceLock::new();
static FD_POOL: OnceLock<FdPool> = OnceLock::new();

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
    let dram_max = crate::dram_maxmemory();
    let nvme_staging = crate::nvme_staging_size();

    // DRAMPool segment count: if maxmemory=0, start with 1 segment (grow later).
    // Otherwise pre-allocate maxmemory / segment_size segments.
    let dram_segment_count = if dram_max == 0 {
        1
    } else {
        ((dram_max as usize) / dram_seg_size).max(1)
    };

    // Total registered iovecs (DRAM + NVMe) must fit in u16 for io_uring IORING_REGISTER_BUFFERS.
    //
    // NVMe staging is split into uniform `segment_size` segments (the io_uring/EFA
    // per-buffer cap is 1 GiB, and segment-size is bounded to ≤1 GiB). Ceiling division
    // so total NVMe staging capacity is never less than the requested nvme-staging-size
    // (floor would under-provision: e.g. 100MB staging / 64MB segment = 1 segment = 64MB,
    // 36MB short).
    let nvme_segments: usize = if mode == crate::OperatingMode::Tiered {
        (nvme_staging.div_ceil(dram_seg_size)).max(1)
    } else {
        0
    };
    let total_segments = dram_segment_count + nvme_segments;
    if total_segments > u16::MAX as usize + 1 {
        return Err(format!(
            "too many segments ({} DRAM + {} NVMe = {}). \
             Max {} (io_uring iovec_index is u16). \
             Increase segment-size or decrease dram-maxmemory",
            dram_segment_count,
            nvme_segments,
            total_segments,
            u16::MAX as usize + 1,
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
    let dram_pool = DRAMPool::new(dram_segment_count, dram_seg_size);

    // io_uring NVMe engine: only in Tiered mode. Ring creation + buffer registration
    // happen on this (main) thread so failures return Err, not panic in the poller.
    let nvme_engine = if mode == crate::OperatingMode::Tiered {
        let pairs = IOVECS.lock().expect("IOVECS lock unavailable").clone();
        let iovecs: Vec<libc::iovec> = pairs
            .iter()
            .filter_map(|opt| {
                opt.map(|(ptr, len)| libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: len,
                })
            })
            .collect();
        let engine = uring::UringNvmeEngine::new(iovecs).map_err(|e| {
            // Engine failed — clear IOVECS so a retry starts fresh.
            IOVECS.lock().expect("IOVECS lock unavailable").clear();
            format!("io_uring engine: {}", e)
        })?;
        Some(engine)
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
    if let Some(engine) = nvme_engine {
        uring::set_nvme_engine(engine);

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

/// Warn that an object file couldn't be unlinked (the next
/// `validate_and_clean_nvme_dir` reclaims the orphan).
pub(crate) fn warn_failed_unlink(during: &str, path: &str, err: &std::io::Error) {
    valkey_module::logging::log_warning(format!(
        "largeobj: failed to unlink object file {path} during {during}: {err}"
    ));
}

/// Reset the NVMe object directory (Tiered mode only): delete it and everything
/// under it, then recreate it empty. `nvme-dir` is a dedicated, module-owned
/// directory (see the `nvme-dir` config docs), so wiping it is safe. A no-op
/// in Dram mode, which never touches disk.
///
/// Called both to reclaim a previous run's leftovers at startup and to clear
/// this instance's files at shutdown. Returns `Ok(())` once nvme-dir exists and
/// is empty (or immediately, in Dram mode); `Err` if nvme-dir is unset in Tiered
/// mode, or the directory could not be removed or recreated.
pub fn validate_and_clean_nvme_dir(mode: crate::OperatingMode, dir: &str) -> std::io::Result<()> {
    if mode != crate::OperatingMode::Tiered {
        return Ok(());
    }
    if dir.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "nvme-dir is required in Tiered operating mode",
        ));
    }
    // remove_dir_all errors if `dir` is absent — but "absent" is already the
    // state we want, so treat NotFound as success.
    if let Err(e) = std::fs::remove_dir_all(dir) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(e);
        }
    }
    std::fs::create_dir_all(dir)
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

// ─── Chunk Helpers ───────────────────────────────────────────────────────────

/// Compute the number of chunks for an object of `obj_len` bytes.
pub fn chunk_count(obj_len: u64, chunk_size: usize) -> u32 {
    obj_len.div_ceil(chunk_size as u64) as u32
}

/// Compute the data length of chunk `i` (last chunk may be shorter).
pub fn chunk_data_len(i: u32, total_chunks: u32, obj_len: u64, chunk_size: usize) -> usize {
    if i == total_chunks - 1 {
        let rem = (obj_len % chunk_size as u64) as usize;
        if rem == 0 {
            chunk_size
        } else {
            rem
        }
    } else {
        chunk_size
    }
}

// ─── FileHeader ──────────────────────────────────────────────────────────────

pub const FILE_HEADER_SIZE: u64 = 4096;
pub const FILE_HEADER_MAGIC: &[u8; 4] = b"LOBJ";
pub const FILE_HEADER_VERSION: u8 = 1;

/// Packed wire size of the header fields (no inter-field padding).
/// Computed from field types so adding a field updates this automatically.
pub const FILE_HEADER_WIRE_LEN: usize = size_of::<[u8; 4]>()  // magic
    + size_of::<u8>()                                           // version
    + size_of::<u64>()                                          // object_id
    + size_of::<u64>()                                          // len
    + size_of::<u32>(); // crc32c

// Static assert: wire header fits within the page.
const _: () = assert!(FILE_HEADER_WIRE_LEN <= FILE_HEADER_SIZE as usize);

/// On-disk file header for NVMe object files.
/// Data starts at offset FILE_HEADER_SIZE (4096) for O_DIRECT alignment.
///
/// The struct's in-memory layout does NOT match the on-disk wire format —
/// the compiler inserts padding for natural field alignment. Serialization
/// is handled by `to_page` (sequential writes) and `from_page` (sequential
/// reads with validation). Do not attempt to byte-cast this struct.
pub struct FileHeader {
    pub magic: [u8; 4],
    pub version: u8,
    pub object_id: u64,
    pub len: u64,
    pub crc32c: u32,
}

impl FileHeader {
    pub fn new(object_id: ObjectId, len: u64, crc32c: u32) -> Self {
        Self {
            magic: *FILE_HEADER_MAGIC,
            version: FILE_HEADER_VERSION,
            object_id: object_id.0,
            len,
            crc32c,
        }
    }

    /// Serialize into a 4096-byte page (header bytes + zero padding).
    pub fn to_page(&self) -> Vec<u8> {
        let mut page = Vec::with_capacity(FILE_HEADER_SIZE as usize);
        page.extend_from_slice(&self.magic);
        page.push(self.version);
        page.extend_from_slice(&self.object_id.to_le_bytes());
        page.extend_from_slice(&self.len.to_le_bytes());
        page.extend_from_slice(&self.crc32c.to_le_bytes());
        debug_assert_eq!(page.len(), FILE_HEADER_WIRE_LEN);
        page.resize(FILE_HEADER_SIZE as usize, 0);
        page
    }

    /// Deserialize from a page. Panics on invalid magic, version, or truncated page
    /// (these indicate corrupt on-disk data). Fields are read sequentially via cursor.
    pub fn from_page(page: &[u8]) -> Self {
        if page.len() < FILE_HEADER_WIRE_LEN {
            panic!(
                "largeobj: file header too short ({} bytes, need {})",
                page.len(),
                FILE_HEADER_WIRE_LEN
            );
        }
        let mut cur = 0;
        let magic: [u8; 4] = page[cur..cur + 4]
            .try_into()
            .expect("file header magic slice");
        cur += 4;
        if &magic != FILE_HEADER_MAGIC {
            panic!(
                "largeobj: file header invalid magic {:?} (expected {:?})",
                magic, FILE_HEADER_MAGIC
            );
        }
        let version = page[cur];
        cur += 1;
        if version != FILE_HEADER_VERSION {
            panic!(
                "largeobj: file header unsupported version {} (expected {})",
                version, FILE_HEADER_VERSION
            );
        }
        let object_id = u64::from_le_bytes(
            page[cur..cur + 8]
                .try_into()
                .expect("file header object_id slice"),
        );
        cur += 8;
        let len = u64::from_le_bytes(
            page[cur..cur + 8]
                .try_into()
                .expect("file header len slice"),
        );
        cur += 8;
        let crc32c = u32::from_le_bytes(
            page[cur..cur + 4]
                .try_into()
                .expect("file header crc32c slice"),
        );
        Self {
            magic,
            version,
            object_id,
            len,
            crc32c,
        }
    }
}

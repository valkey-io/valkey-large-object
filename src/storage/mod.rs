//! Storage Layer — DRAMPool + NVMePool + io_uring I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crate::data_type::ObjectId;

pub mod context;
pub mod dram_pool;
pub mod fd_pool;
pub mod nvme_pool;
pub mod segment;
pub mod segment_pool;
/// Linux-only. Tiered mode is refused at module load on non-Linux.
#[cfg(target_os = "linux")]
pub mod uring;

// Re-exports for convenience.
pub use context::{ObjectContext, SegmentBuffer, StreamingContext};

/// O_DIRECT / io_uring alignment requirement (XFS default block size).
/// Both buffer address and I/O length must be multiples of this.
pub const IO_ALIGN: usize = 4096;

/// Round up to IO_ALIGN boundary. Used by the allocator (buffer size)
/// and the uring layer (I/O length) to satisfy O_DIRECT requirements.
pub fn align_up(n: usize) -> usize {
    (n + IO_ALIGN - 1) & !(IO_ALIGN - 1)
}

/// `O_DIRECT` on Linux, where the object files live. Every open that ORs this in belongs to the
/// NVMe path, which Tiered mode owns and non-Linux builds never reach, so 0 keeps those opens
/// compiling without changing behaviour anywhere it matters.
#[cfg(target_os = "linux")]
pub const DIRECT_IO_FLAG: libc::c_int = libc::O_DIRECT;
#[cfg(not(target_os = "linux"))]
pub const DIRECT_IO_FLAG: libc::c_int = 0;
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

/// Global iovec registry. Segments append here at creation time.
/// Array position = iovec_index used by io_uring ReadFixed/WriteFixed.
/// register_buffers() passes this directly to the kernel — no reordering.
/// Stored as (ptr, len) pairs because libc::iovec contains raw pointers (not Send).
static IOVECS: Mutex<Vec<(usize, usize)>> = Mutex::new(Vec::new());

/// Called by SegmentPool::new() when creating each segment.
/// Returns the assigned iovec_index (= current array length before push).
pub fn append_iovec(iov: libc::iovec) -> u16 {
    let mut iovecs = IOVECS.lock().expect("IOVECS lock unavailable");
    let idx = u16::try_from(iovecs.len()).expect("iovec index overflow (>65535)");
    iovecs.push((iov.iov_base as usize, iov.iov_len));
    idx
}

/// Snapshot the registered iovec array, in registration order, for io_uring buffer registration.
fn iovec_snapshot() -> Vec<libc::iovec> {
    IOVECS
        .lock()
        .expect("IOVECS lock unavailable")
        .iter()
        .map(|&(ptr, len)| libc::iovec {
            iov_base: ptr as *mut libc::c_void,
            iov_len: len,
        })
        .collect()
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

/// Initialize storage layer: validate config, create pools, spawn io_uring poller (Tiered only).
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
    let nvme_segments: usize = if mode == crate::OperatingMode::Tiered {
        1
    } else {
        0
    };
    let total_segments = dram_segment_count + nvme_segments;
    if total_segments > u16::MAX as usize + 1 {
        return Err(format!(
            "too many segments ({} DRAM + {} NVMe = {}). \
             Max {} (io_uring iovec_index is u16). \
             Increase dram-segment-size or decrease dram-maxmemory",
            dram_segment_count,
            nvme_segments,
            total_segments,
            u16::MAX as usize + 1,
        ));
    }

    // ── Create all resources as locals (no OnceLock yet) ──

    // NVMePool + FdPool: only needed in Tiered mode.
    let nvme_pool = if mode == crate::OperatingMode::Tiered {
        Some(NVMePool::new(1, nvme_staging))
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
    let nvme_engine = crate::tiered::prepare_engine(mode, iovec_snapshot()).inspect_err(|_| {
        // Engine failed — clear IOVECS so a retry starts fresh.
        IOVECS.lock().expect("IOVECS lock unavailable").clear();
    })?;

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
    crate::tiered::commit_engine(nvme_engine);

    Ok(format!(
        "mode={:?} nvme_dir={} dram_segments={}x{}MB nvme_staging={}MB",
        mode,
        nvme_dir,
        dram_segment_count,
        dram_seg_size / (1024 * 1024),
        nvme_staging / (1024 * 1024),
    ))
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
        for seg in nvme_pool.segments() {
            slices.push(unsafe { std::slice::from_raw_parts(seg.base, seg.size) });
        }
    }
    for seg in get_dram_pool().segments() {
        slices.push(unsafe { std::slice::from_raw_parts(seg.base, seg.size) });
    }
    slices
}

/// Delete an object's NVMe file. Called from free callback.
pub fn delete_file(object_id: ObjectId) {
    let dir = crate::nvme_dir();
    let path = object_id.file_path(&dir);
    let _ = std::fs::remove_file(&path);
}

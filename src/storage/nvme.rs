//! NVMe Storage Semantics — files, headers, capacity accounting.
//!
//! Separated from `uring.rs` (io_uring transport mechanics) so NVMe storage
//! concerns live in one place. The uring module handles ring, SQE/CQE,
//! channels, and completion streams; this module handles everything that
//! knows what an NVMe object file *is*.

use std::mem::size_of;
use std::os::unix::io::{FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};

use super::context::SegmentBuffer;
use super::uring;
use super::Crc;
use super::NVMePool;
use crate::data_type::ObjectId;

// ─── NVMe Disk Usage Tracking ────────────────────────────────────────────────

/// Tracks total NVMe disk usage in bytes. Incremented on file creation, decremented on deletion.
static NVME_DISK_USAGE: AtomicU64 = AtomicU64::new(0);

/// Bytes of victim files eviction has claimed and not yet unlinked. They stay in `NVME_DISK_USAGE`
/// until the unlink, but are spoken for: reservations may overshoot the cap by this much, so that
/// writes overlapping an eviction do not each evict for the same shortfall.
static NVME_PENDING_FREE: AtomicU64 = AtomicU64::new(0);

/// Increment NVMe disk usage after a file is created.
pub fn increase_nvme_disk_usage(bytes: u64) {
    NVME_DISK_USAGE.fetch_add(bytes, Ordering::Relaxed);
}

/// Decrement NVMe disk usage after a file is deleted.
///
/// FATAL on underflow: freeing more than is tracked means corrupt accounting, which
/// must be accurate for capacity checks, so assert on the issue.
pub fn decrease_nvme_disk_usage(bytes: u64) {
    if let Err(tracked) = NVME_DISK_USAGE.try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        cur.checked_sub(bytes)
    }) {
        panic!(
            "NVMe disk-usage underflow: tried to free {bytes} B but only {tracked} B tracked \
             — accounting is corrupt (double-free or size mismatch)"
        );
    }
}

/// Eviction claimed a victim of `bytes`: its file is to be unlinked, which credits the ledger.
pub fn add_pending_free(bytes: u64) {
    NVME_PENDING_FREE.fetch_add(bytes, Ordering::Relaxed);
}

/// A claimed victim is dealt with. Call it BEFORE crediting the ledger for the unlink: readers load
/// the usage first and this second, so a race can only overstate the usage, never admit a write the
/// disk has no room for.
pub fn finish_pending_free(bytes: u64) {
    if NVME_PENDING_FREE
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            cur.checked_sub(bytes)
        })
        .is_err()
    {
        panic!("NVMe pending-free underflow: finished {bytes} B more than was claimed");
    }
}

/// Bytes of claimed victims whose files are not yet unlinked.
pub fn nvme_pending_free() -> u64 {
    NVME_PENDING_FREE.load(Ordering::Relaxed)
}

/// Atomically reserve `bytes` of NVMe disk budget if it fits within nvme-maxmemory plus the bytes
/// of victims already claimed. Returns true and increments the counter on success; returns false
/// and leaves the counter unchanged if the reservation would exceed that (or overflow).
/// Returns true if nvme-maxmemory is 0 (unlimited).
pub fn try_reserve_nvme_disk_usage(bytes: u64) -> bool {
    let max = crate::nvme_maxmemory();
    NVME_DISK_USAGE
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            let next = cur.checked_add(bytes)?;
            // Loaded after `cur`: see `finish_pending_free`.
            let room = max.saturating_add(nvme_pending_free());
            if max == 0 || next <= room {
                Some(next)
            } else {
                None
            }
        })
        .is_ok()
}

/// How far reserving `bytes` would take the ledger past `nvme-maxmemory` once the claimed victims
/// are gone. Zero means it fits (or the budget is unlimited).
pub fn nvme_shortfall(bytes: u64) -> u64 {
    let max = crate::nvme_maxmemory();
    if max == 0 {
        return 0;
    }
    let used = nvme_disk_usage();
    used.saturating_add(bytes)
        .saturating_sub(max.saturating_add(nvme_pending_free()))
}

/// Current tracked NVMe disk usage in bytes.
pub fn nvme_disk_usage() -> u64 {
    NVME_DISK_USAGE.load(Ordering::Relaxed)
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
    pub crc32c: Crc,
}

impl FileHeader {
    pub fn new(object_id: ObjectId, len: u64, crc32c: Crc) -> Self {
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
        assert_eq!(page.len(), FILE_HEADER_WIRE_LEN);
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

// ─── Disk Length ──────────────────────────────────────────────────────────────

/// O_DIRECT-aligned on-disk size of an object file, computed by iterating
/// through the ChunkIterator and summing each chunk's aligned I/O length.
/// This matches what the uring poller actually writes (`align_up(op.len)` per
/// SQE).
///
/// Resets the iterator cursor to 0 before and after iteration so the caller
/// can continue using it normally.
pub fn object_disk_len(chunk_iter: &mut super::ChunkIterator) -> u64 {
    chunk_iter.reset_cursor();
    let mut chunk_bytes: u64 = 0;
    while let Some(chunk) = chunk_iter.next_chunk() {
        chunk_bytes += super::align_up(chunk.user_data_len) as u64;
    }
    chunk_iter.reset_cursor();
    FILE_HEADER_SIZE + chunk_bytes
}

// ─── NVMe File I/O Helpers ───────────────────────────────────────────────────

/// Read FileHeader from offset 0 of an NVMe file into a pool buffer, parse and
/// validate against expected values. Checks magic, version, object_id, len, and CRC.
/// Panics on corrupt headers (unrecoverable on-disk corruption). Panics on CRC
/// mismatch (serving corrupt data is worse than crashing). Panics on poller failure
/// (RecvError means the io_uring poller is dead).
/// `pool_buffer_ptr` passed as usize for Send safety (raw pointer is not Send).
/// chunk-size config enforces min 4096, so every pool buffer can hold a full
/// FileHeader page.
#[allow(clippy::too_many_arguments)]
pub async fn read_and_verify_file_header(
    fd: RawFd,
    pool_id: uring::PoolType,
    iovec_index: u16,
    pool_buffer_ptr: usize,
    use_fixed: bool,
    expected_object_id: ObjectId,
    expected_len: u64,
    crc32c_expected: Crc,
) {
    assert!(
        pool_buffer_ptr != 0,
        "read_and_verify_file_header: null buffer pointer"
    );
    let hdr_op = uring::UringOp {
        iovec_index,
        buf_ptr: pool_buffer_ptr as *mut u8,
        file_offset: 0,
        len: FILE_HEADER_SIZE,
        use_fixed,
    };
    let hdr_rx = uring::submit_read(pool_id, fd, hdr_op);
    // RecvError: this pool's io_uring poller dropped the oneshot sender without
    // calling send(). This only happens if that poller thread panicked or exited
    // — it owns all senders in its pending HashMap. Each pool has its own
    // long-lived poller, so losing the one for `pool_id` is permanent: no future
    // I/O on that ring can complete. Abort.
    match hdr_rx.await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            panic!(
                "largeobj: file header read I/O error for object {:?}: {}",
                expected_object_id, e
            );
        }
        Err(_) => {
            panic!(
                "largeobj: io_uring poller dropped oneshot sender — poller is dead, \
                 all NVMe I/O is unrecoverable"
            );
        }
    }
    let hdr_slice = unsafe {
        std::slice::from_raw_parts(pool_buffer_ptr as *const u8, FILE_HEADER_SIZE as usize)
    };
    // from_page panics on corrupt magic/version (unrecoverable on-disk corruption).
    let header = FileHeader::from_page(hdr_slice);
    if header.object_id != expected_object_id.0 || header.len != expected_len {
        panic!(
            "largeobj: file header mismatch for object {:?}: \
             header_oid={} expected_oid={}, header_len={} expected_len={}",
            expected_object_id, header.object_id, expected_object_id.0, header.len, expected_len
        );
    }
    if header.crc32c != crc32c_expected {
        panic!(
            "largeobj: CRC mismatch for object {:?}: file={} expected={}",
            expected_object_id, header.crc32c, crc32c_expected
        );
    }
}

/// Open an NVMe file for writing with O_CREAT|O_TRUNC and optionally O_DIRECT.
/// Returns an `OwnedFd` that closes the file descriptor on drop.
pub fn open_nvme_file_for_write(file_path: &str) -> Result<OwnedFd, super::StorageError> {
    let c_path = std::ffi::CString::new(file_path).expect("file_path null");
    let mut flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        flags |= libc::O_DIRECT;
    }
    let fd = unsafe { libc::open(c_path.as_ptr(), flags, 0o644) };
    if fd < 0 {
        Err(super::StorageError::IoError {
            code: std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO),
        })
    } else {
        // SAFETY: fd is a valid, newly opened file descriptor owned exclusively by us.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

/// Write FileHeader to offset 0 of an NVMe file using the given NVMePool buffer.
/// chunk-size config enforces min 4096, so every pool buffer can hold a full
/// FileHeader page.
pub async fn write_file_header(
    fd: RawFd,
    object_id: ObjectId,
    obj_len: u64,
    crc: Crc,
    buf: &SegmentBuffer,
    nvme_pool: &NVMePool,
) -> Result<(), super::StorageError> {
    assert!(
        buf.len as u64 >= FILE_HEADER_SIZE,
        "NVMePool buffer too small for FileHeader"
    );
    let header = FileHeader::new(object_id, obj_len, crc);
    let header_page = header.to_page();
    let hdr_ptr = nvme_pool.buffer_ptr(buf);
    unsafe {
        std::ptr::copy_nonoverlapping(header_page.as_ptr(), hdr_ptr, header_page.len());
    }
    let hdr_op = uring::UringOp {
        iovec_index: nvme_pool.iovec_index_for_buf(buf),
        buf_ptr: hdr_ptr,
        file_offset: 0,
        len: FILE_HEADER_SIZE,
        use_fixed: nvme_pool.is_buf_io_uring_registered(buf),
    };
    let hdr_rx = uring::submit_write(uring::PoolType::Nvme, fd, hdr_op);
    match hdr_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            panic!(
                "largeobj: io_uring poller dropped oneshot sender — poller is dead, \
                 all NVMe I/O is unrecoverable"
            );
        }
    }
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

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // NVME_DISK_USAGE is a process-global static shared by every test in this
    // binary, and cargo runs tests in parallel. Serialize the accounting tests so
    // their reads/writes don't interleave. Recover from a poisoned lock (the
    // underflow test panics by design) so one panicking test can't wedge the rest.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    // increase/decrease are exact inverses: an equal amount added and removed must
    // leave the counter where it started. Asserted as a delta against a fresh
    // baseline so a corrupt absolute value from another test can't affect it.
    #[test]
    fn test_increase_decrease_symmetry() {
        let _g = lock();
        let base = nvme_disk_usage();
        increase_nvme_disk_usage(4096);
        assert_eq!(nvme_disk_usage(), base + 4096);
        increase_nvme_disk_usage(8192);
        assert_eq!(nvme_disk_usage(), base + 12288);
        decrease_nvme_disk_usage(8192);
        assert_eq!(nvme_disk_usage(), base + 4096);
        decrease_nvme_disk_usage(4096);
        assert_eq!(nvme_disk_usage(), base, "counter must return to baseline");
    }

    // Freeing exactly what was reserved must return to baseline — the same
    // reserve-then-free balance the SET path relies on for aligned disk_len.
    #[test]
    fn test_reserve_then_free_returns_to_zero() {
        let _g = lock();
        let base = nvme_disk_usage();
        // chunk_size=4096 (aligned), so each object is one or more chunks.
        for len in [1u64, 4095, 4096, 4097, 1_048_576] {
            let mut iter = super::super::ChunkIterator::new(len, 4096, 256, None);
            let disk_len = object_disk_len(&mut iter);
            increase_nvme_disk_usage(disk_len);
            decrease_nvme_disk_usage(disk_len);
        }
        assert_eq!(nvme_disk_usage(), base);
    }

    // Decrementing more than is tracked is a corrupt-accounting bug and MUST abort,
    // not silently wrap the counter (which would poison every capacity check).
    #[test]
    #[should_panic(expected = "underflow")]
    fn test_decrease_underflow_is_fatal() {
        let _g = lock();
        // Subtracting u64::MAX underflows from any real baseline, triggering the
        // fatal assert regardless of what the counter currently holds.
        decrease_nvme_disk_usage(u64::MAX);
    }

    // ─── object_disk_len ─────────────────────────────────────────────────

    #[test]
    fn test_object_disk_len_single_chunk() {
        // 1 byte → 1 chunk, align_up(1) = 4096. Total = 4096 header + 4096 data.
        let mut iter = super::super::ChunkIterator::new(1, 4096, 1, None);
        assert_eq!(object_disk_len(&mut iter), 4096 + 4096);
    }

    #[test]
    fn test_object_disk_len_exact_multiple_aligned_chunk_size() {
        // 8192 bytes, chunk_size=4096 → 2 full chunks. Each align_up(4096)=4096.
        let mut iter = super::super::ChunkIterator::new(8192, 4096, 2, None);
        assert_eq!(object_disk_len(&mut iter), 4096 + 4096 + 4096);
    }

    #[test]
    fn test_object_disk_len_partial_last_chunk_aligned_chunk_size() {
        // 10000 bytes, chunk_size=4096 → chunks: 4096, 4096, 1808.
        // align_up: 4096, 4096, 4096. Data total = 12288.
        let mut iter = super::super::ChunkIterator::new(10000, 4096, 3, None);
        assert_eq!(object_disk_len(&mut iter), 4096 + 12288);
    }

    #[test]
    fn test_object_disk_len_unaligned_chunk_size() {
        // 10000 bytes, chunk_size=5000 → chunks: 5000, 5000.
        // align_up(5000) = 8192 each. Data total = 16384.
        let mut iter = super::super::ChunkIterator::new(10000, 5000, 2, None);
        assert_eq!(object_disk_len(&mut iter), 4096 + 8192 + 8192);
    }

    #[test]
    fn test_object_disk_len_unaligned_chunk_size_with_remainder() {
        // 8193 bytes, chunk_size=5000 → chunks: 5000, 3193.
        // align_up(5000)=8192, align_up(3193)=4096. Data total = 12288.
        let mut iter = super::super::ChunkIterator::new(8193, 5000, 2, None);
        assert_eq!(object_disk_len(&mut iter), 4096 + 8192 + 4096);
    }
}

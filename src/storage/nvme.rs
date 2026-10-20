//! NVMe Storage Semantics — files, headers, capacity accounting.
//!
//! Separated from `uring.rs` (io_uring transport mechanics) so NVMe storage
//! concerns live in one place. The uring module handles ring, SQE/CQE,
//! channels, and completion streams; this module handles everything that
//! knows what an NVMe object file *is*.

use super::fd_placement::ObjectFd;
use std::mem::size_of;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::context::SegmentBuffer;
use super::uring;
use super::Crc;
use super::NVMePool;
use crate::data_type::ObjectId;

// ─── NVMe Disk Usage Tracking ────────────────────────────────────────────────

/// Tracks total NVMe disk usage in bytes. Incremented on file creation, decremented on deletion.
static NVME_DISK_USAGE: AtomicU64 = AtomicU64::new(0);

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

/// Reserve `bytes` of NVMe disk budget, evicting other objects to make room if it
/// does not fit within disk-maxmemory. Returns true and increments the counter on
/// success; returns false and leaves the counter unchanged otherwise.
/// Returns true if disk-maxmemory is 0 (unlimited).
pub fn try_reserve_nvme_disk_usage(bytes: u64) -> bool {
    reserve(bytes as i64) || evict_and_reserve(bytes)
}

/// Atomically add `delta` to the usage if the result fits within disk-maxmemory;
/// false on overflow. Negative when an eviction freed more than it reserves. FATAL
/// below 0.
fn reserve(delta: i64) -> bool {
    let max = crate::nvme_maxmemory();
    NVME_DISK_USAGE
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            let Some(next) = cur.checked_add_signed(delta) else {
                assert!(
                    delta > 0,
                    "NVMe disk-usage underflow: {cur} B tracked, delta {delta} B"
                );
                return None;
            };
            (max == 0 || next <= max).then_some(next)
        })
        .is_ok()
}

/// Current tracked NVMe disk usage in bytes.
pub fn nvme_disk_usage() -> u64 {
    NVME_DISK_USAGE.load(Ordering::Relaxed)
}

// ─── NVMe Eviction ───────────────────────────────────────────────────────────

/// Candidates picked per eviction.
const EVICT_BATCH: usize = 64;

/// Run `f` under the server lock: COPY already holds it on main; off main (a SET's
/// tokio task) take it. Never call off main while holding it: it would deadlock.
fn with_server_lock<R>(f: impl FnOnce(&valkey_module::Context) -> R) -> R {
    if crate::is_main_thread() {
        f(&valkey_module::Context::dummy())
    } else {
        f(&valkey_module::ThreadSafeContext::new().lock())
    }
}

/// Evict objects to make room for `bytes`, then reserve them. True if reserved.
/// A refused write keeps its victims evicted: a claim is never undone.
fn evict_and_reserve(bytes: u64) -> bool {
    let max = crate::nvme_maxmemory();
    // Eviction cannot make room for more than the cap.
    if bytes > max {
        return false;
    }
    if !with_server_lock(crate::tiered_eviction_allowed) {
        return false;
    }
    // Take the headroom first, so no other write can use it while we evict.
    let prev = NVME_DISK_USAGE.update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        cur + bytes.min(max.saturating_sub(cur))
    });
    let taken = bytes.min(max.saturating_sub(prev));
    let need = bytes - taken;
    // Claim, then stat: a claimed file cannot change under us.
    let mut victims = Vec::new();
    let mut covered = 0;
    for oid in super::eviction_candidates::pick(EVICT_BATCH, |_| true) {
        if covered >= need || victims.len() == super::cache_policy::RECLAIM_MAX_VICTIMS {
            break;
        }
        // Server lock -> UNEVICTABLE -> RECLAIM_LIST.
        let Some(unevictable) = with_server_lock(|_| super::object_file::claim_victim(oid)) else {
            continue;
        };
        match stat(oid) {
            Ok(Some(len)) => {
                covered += len;
                victims.push((unevictable, len));
            }
            // Torn down between the pick and the claim, so its key is gone.
            Ok(None) => {
                super::reclaim::RECLAIM_LIST.remove(&oid);
            }
            // Evicted, but orphaned with its size unknown: the retry stats and frees it.
            Err(e) => {
                valkey_module::logging::log_warning(format!(
                    "largeobj: failed to stat object file {} during eviction, will retry: {e}",
                    oid.file_path(&crate::disk_dir())
                ));
                crate::info::EVICTIONS.fetch_add(1, Ordering::Relaxed);
                ORPHANED_FILES.push(unevictable, 0);
            }
        }
    }
    // No lock.
    let freed = evict(victims);
    // One update, so a competing reservation cannot take the freed bytes.
    let reserved = reserve(bytes as i64 - taken as i64 - freed as i64);
    if !reserved {
        // Release the headroom; the victims stay evicted.
        decrease_nvme_disk_usage(taken + freed);
    }
    reserved
}

/// Unlink claimed `victims` and remove them from the unevictable list,
/// except orphans, which stay in it until the retry. Returns the bytes freed, not counting
/// orphans. Victims stay in the reclaim list: their keys read as missing until the cron
/// deletes them, and their teardown then only removes them from it.
fn evict(victims: Vec<(Arc<super::UnevictableOID>, u64)>) -> u64 {
    let mut freed = 0;
    for (unevictable, size) in victims {
        let oid = unevictable.oid();
        if let Some(pool) = super::FD_POOL.get() {
            pool.remove(oid);
        }
        super::get_dram_pool().remove_object(&oid);
        crate::info::EVICTIONS.fetch_add(1, Ordering::Relaxed);
        if try_unlink(unevictable, size, "eviction") {
            freed += size;
        }
    }
    freed
}

// ─── Orphaned Files ──────────────────────────────────────────────────────────

/// Files whose unlink failed transiently: still charged, and in the unevictable list
/// so no eviction takes them. The cron's retry unlinks them and credits their bytes.
/// Size 0: an eviction could not stat the file, so the retry does first.
pub struct OrphanedFiles {
    files: Mutex<Vec<(Arc<super::UnevictableOID>, u64)>>,
    pub count: AtomicU64,
    pub bytes: AtomicU64,
    retrying: AtomicBool,
}

pub static ORPHANED_FILES: OrphanedFiles = OrphanedFiles {
    files: Mutex::new(Vec::new()),
    count: AtomicU64::new(0),
    bytes: AtomicU64::new(0),
    retrying: AtomicBool::new(false),
};

impl OrphanedFiles {
    fn push(&self, unevictable: Arc<super::UnevictableOID>, size: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(size, Ordering::Relaxed);
        self.files
            .lock()
            .expect("ORPHANED_FILES lock unavailable")
            .push((unevictable, size));
    }

    /// Scaling cron: retry the unlinks on tokio, one retry at a time.
    pub fn retry_deletion(&'static self) {
        if self.count.load(Ordering::Relaxed) == 0 || self.retrying.swap(true, Ordering::Acquire) {
            return;
        }
        crate::runtime_handle().spawn(async {
            let files =
                std::mem::take(&mut *self.files.lock().expect("ORPHANED_FILES lock unavailable"));
            for (unevictable, mut size) in files {
                let oid = unevictable.oid();
                let sized = size > 0
                    || stat(oid).is_ok_and(|len| {
                        size = len.unwrap_or(0);
                        self.bytes.fetch_add(size, Ordering::Relaxed);
                        true
                    });
                if sized && unlink(oid).is_ok() {
                    decrease_nvme_disk_usage(size);
                    self.count.fetch_sub(1, Ordering::Relaxed);
                    self.bytes.fetch_sub(size, Ordering::Relaxed);
                } else {
                    let mut files = self.files.lock().expect("ORPHANED_FILES lock unavailable");
                    files.push((unevictable, size));
                }
            }
            self.retrying.store(false, Ordering::Release);
        });
    }
}

/// Unlink `oid`'s file.
/// - Ok: unlinked, or already gone (`ENOENT`).
/// - Err: a `transient` error. A retry may succeed.
/// - Any other error aborts the server.
fn unlink(oid: ObjectId) -> std::io::Result<()> {
    let path = oid.file_path(&crate::disk_dir());
    let Err(e) = std::fs::remove_file(&path) else {
        return Ok(());
    };
    match e.raw_os_error() {
        Some(libc::ENOENT) => Ok(()),
        _ if transient(&e) => Err(e),
        _ => panic!("largeobj: failed to unlink object file {path}: {e}"),
    }
}

/// Size of `oid`'s file; `None` if it is gone (`ENOENT`). A `transient` error is
/// returned; any other error aborts the server.
fn stat(oid: ObjectId) -> std::io::Result<Option<u64>> {
    let path = oid.file_path(&crate::disk_dir());
    match std::fs::metadata(&path) {
        Ok(meta) => Ok(Some(meta.len())),
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => Ok(None),
        Err(e) if transient(&e) => Err(e),
        Err(e) => panic!("largeobj: failed to stat object file {path}: {e}"),
    }
}

/// Transient errors, retried:
/// - `EINTR`: a signal interrupted the call.
/// - `EAGAIN`: the filesystem asked us to try again (network and FUSE filesystems).
/// - `EBUSY`: the file is in use by the system, e.g. an NFS client's stand-in for an
///   open, deleted file.
/// - `ENOMEM`: the kernel was briefly out of memory.
///
/// Critical errors abort the server. They include:
/// - `EIO`: the disk failed.
/// - `EROFS`: the filesystem went read-only.
/// - `EACCES`, `EPERM`: `disk-dir`'s permissions changed.
/// - `ENOTDIR`, `ELOOP`, `ENAMETOOLONG`: `disk-dir`'s path no longer resolves (part of it
///   was replaced, loops, or is too long).
/// - `EISDIR`: a directory sits where an object file should be.
/// - `EFAULT`: a bad pointer passed to the kernel, a bug.
fn transient(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EINTR | libc::EAGAIN | libc::EBUSY | libc::ENOMEM)
    )
}

/// Unlink the file of `size` charged bytes. True if it is gone: the caller credits
/// them. False if it is orphaned: the bytes stay charged until the retry credits them.
pub(crate) fn try_unlink(unevictable: Arc<super::UnevictableOID>, size: u64, during: &str) -> bool {
    let oid = unevictable.oid();
    let unlinked = if crate::test_fail_unlink() {
        Err(std::io::Error::from_raw_os_error(libc::EBUSY))
    } else {
        unlink(oid)
    };
    let Err(e) = unlinked else {
        return true;
    };
    valkey_module::logging::log_warning(format!(
        "largeobj: failed to unlink object file {} during {during}, will retry: {e}",
        oid.file_path(&crate::disk_dir())
    ));
    ORPHANED_FILES.push(unevictable, size);
    false
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
/// Returns an `ObjectFd` that closes the file descriptor on drop.
pub fn open_nvme_file_for_write(file_path: &str) -> Result<ObjectFd, super::StorageError> {
    let c_path = std::ffi::CString::new(file_path).expect("file_path null");
    let mut flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        flags |= libc::O_DIRECT;
    }
    super::fd_placement::open_object_file(&c_path, flags, 0o644).map_err(|e| {
        super::StorageError::IoError {
            code: e.raw_os_error().unwrap_or(libc::EIO),
        }
    })
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

/// Reset the NVMe object directory (Tiered mode only): delete it and everything
/// under it, then recreate it empty. `disk-dir` is a dedicated, module-owned
/// directory (see the `disk-dir` config docs), so wiping it is safe. A no-op
/// in Dram mode, which never touches disk.
///
/// Called both to reclaim a previous run's leftovers at startup and to clear
/// this instance's files at shutdown. Returns `Ok(())` once disk-dir exists and
/// is empty (or immediately, in Dram mode); `Err` if disk-dir is unset in Tiered
/// mode, or the directory could not be removed or recreated.
pub fn validate_and_clean_disk_dir(mode: crate::OperatingMode, dir: &str) -> std::io::Result<()> {
    if mode != crate::OperatingMode::Tiered {
        return Ok(());
    }
    if dir.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "disk-dir is required in Tiered operating mode",
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

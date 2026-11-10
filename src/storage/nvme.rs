//! NVMe Storage Semantics — files, headers, capacity accounting.
//!
//! Separated from `uring.rs` (io_uring transport mechanics) so NVMe storage
//! concerns live in one place. The uring module handles ring, SQE/CQE,
//! channels, and completion streams; this module handles everything that
//! knows what an NVMe object file *is*.

use super::fd_placement::ObjectFd;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::Crc;
use crate::data_type::ObjectId;

// ─── NVMe Disk Usage Tracking ────────────────────────────────────────────────

/// Tracks total NVMe disk usage in bytes. Incremented on file creation, decremented on deletion.
/// Callers charge each file its `allocated_disk_bytes`, not its length.
static NVME_DISK_USAGE: AtomicU64 = AtomicU64::new(0);

/// The unit disk-dir's filesystem allocates each file in, set once at startup by
/// `init_disk_geometry`. One `IO_ALIGN` block until then, and in Dram mode.
static DISK_ALLOC_UNIT: AtomicU64 = AtomicU64::new(super::IO_ALIGN as u64);

/// Disk space each file's inode takes, set once at startup by `init_disk_geometry`.
/// XFS allocates inodes from free space; other filesystems (ext4) set them aside at
/// mkfs, so they take none. 0 until then, and in Dram mode.
static DISK_INODE_SIZE: AtomicU64 = AtomicU64::new(0);

/// Bytes the filesystem allocates for a file of `len` bytes: its data, plus its inode.
/// On a striped XFS, a file of at least one stripe unit starts on a stripe-unit
/// boundary, so the rest of its last stripe unit can't hold another such file: charge
/// `len` rounded up to the unit. Charging only `len` lets the filesystem fill up before
/// disk-maxmemory is reached. XFS aligns only allocations of at least one stripe unit
/// (`xfs_bmapi_allocate`), and every object file is allocated in one `fallocate`
/// (`preallocate`), so a shorter file takes only its own blocks: charge `len`, already
/// `IO_ALIGN`-aligned.
pub fn allocated_disk_bytes(len: u64) -> u64 {
    let unit = DISK_ALLOC_UNIT.load(Ordering::Relaxed);
    let data = if len >= unit {
        len.next_multiple_of(unit)
    } else {
        len
    };
    data + DISK_INODE_SIZE.load(Ordering::Relaxed)
}

/// Blocks the filesystem allocated for the file `meta` describes beyond its data:
/// an xattr block if the header doesn't fit in the inode, or an extent-tree block if
/// the file is in too many extents to list in the inode. Charged on top of
/// `allocated_disk_bytes`. Every object file is allocated in full (`preallocate`) in
/// blocks that divide `IO_ALIGN`, so its data blocks are exactly its length.
pub fn metadata_disk_bytes(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    // st_blocks counts 512-byte units.
    (meta.blocks() * 512).saturating_sub(meta.len())
}

/// `_IOR('X', 100, struct xfs_fsop_geom_v1)`: fills the 112-byte geometry struct
/// from the kernel's `xfs_fs.h`.
const XFS_IOC_FSGEOMETRY_V1: u32 = 0x8070_5864;
/// u32 word index of `blocksize` (bytes 0..4): filesystem block size, bytes.
const XFS_GEOM_BLOCKSIZE: usize = 0;
/// u32 word index of `inodesize` (bytes 24..28): inode size, bytes.
const XFS_GEOM_INODESIZE: usize = 6;
/// u32 word index of `sunit` (bytes 80..84): stripe unit, filesystem blocks.
const XFS_GEOM_SUNIT: usize = 20;

/// Read disk-dir's filesystem geometry and check the module can write and charge
/// files on it exactly. Sets the unit `allocated_disk_bytes` rounds to (disk-dir's
/// stripe unit if it is on a striped XFS, else one `IO_ALIGN` block) and the inode
/// size it adds. Returns the unit.
pub fn init_disk_geometry(dir: &str) -> Result<u64, String> {
    use std::os::unix::io::AsRawFd;
    let dir_fd =
        std::fs::File::open(dir).map_err(|e| format!("cannot open disk-dir {dir}: {e}"))?;
    let fd = dir_fd.as_raw_fd();
    let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `fs` is the struct fstatfs writes.
    if unsafe { libc::fstatfs(fd, &mut fs) } != 0 {
        return Err(format!(
            "cannot statfs disk-dir {dir}: {}",
            std::io::Error::last_os_error()
        ));
    }
    let io_align = super::IO_ALIGN as u64;
    // Files are allocated and charged in IO_ALIGN units: exact only if blocks divide it.
    // O_DIRECT writes are IO_ALIGN-aligned, so the sector size must divide it too: on XFS
    // it does once the block size does (mkfs.xfs keeps the sector size <= the block size).
    let block = fs.f_bsize as u64;
    if !io_align.is_multiple_of(block) {
        return Err(format!(
            "disk-dir {dir} block size {block} B does not divide {io_align} B"
        ));
    }
    let mut stripe = 0;
    let mut inode = 0;
    if fs.f_type == libc::XFS_SUPER_MAGIC {
        let mut geometry = [0u32; 28];
        // SAFETY: `geometry` is the 112 bytes this ioctl writes.
        if unsafe { libc::ioctl(fd, XFS_IOC_FSGEOMETRY_V1 as _, geometry.as_mut_ptr()) } != 0 {
            return Err(format!(
                "cannot read disk-dir {dir} XFS geometry: {}",
                std::io::Error::last_os_error()
            ));
        }
        stripe = geometry[XFS_GEOM_SUNIT] as u64 * geometry[XFS_GEOM_BLOCKSIZE] as u64;
        inode = geometry[XFS_GEOM_INODESIZE] as u64;
        valkey_module::logging::log_notice(format!(
            "largeobj: disk-dir {dir} is on XFS, stripe unit {stripe} B (0 = not striped), \
             inode {inode} B"
        ));
    } else {
        valkey_module::logging::log_warning(format!(
            "largeobj: disk-dir {dir} is not on XFS; disk usage is charged in {io_align} B blocks"
        ));
    }
    let unit = stripe.max(io_align);
    DISK_ALLOC_UNIT.store(unit, Ordering::Relaxed);
    DISK_INODE_SIZE.store(inode, Ordering::Relaxed);
    Ok(unit)
}

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
            Some(meta) => {
                // What the file was charged: its ObjectFile's `disk_bytes`.
                let bytes = allocated_disk_bytes(meta.len()) + metadata_disk_bytes(&meta);
                covered += bytes;
                victims.push((unevictable, bytes));
            }
            // Torn down between the pick and the claim, so its key is gone.
            None => {
                super::reclaim::RECLAIM_LIST.remove(&oid);
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

/// Unlink claimed `victims` and remove them from the unevictable list. Returns the bytes
/// freed. Victims stay in the reclaim list: their keys read as missing until the cron
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
        unlink(oid);
        freed += size;
    }
    freed
}

/// Unlink `oid`'s file.
/// - Returns once it is unlinked, or already gone (`ENOENT`).
/// - A `transient` error is retried once.
/// - Any other error, or a second transient one, aborts the server.
pub(crate) fn unlink(oid: ObjectId) {
    let path = oid.file_path(&crate::disk_dir());
    let removed = std::fs::remove_file(&path).or_else(|e| {
        if transient(&e) {
            std::fs::remove_file(&path)
        } else {
            Err(e)
        }
    });
    match removed {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
        Err(e) => panic!("largeobj: failed to unlink object file {path}: {e}"),
    }
}

/// Metadata of `oid`'s file.
/// - `None` if it is gone (`ENOENT`).
/// - A `transient` error is retried once.
/// - Any other error, or a second transient one, aborts the server.
fn stat(oid: ObjectId) -> Option<std::fs::Metadata> {
    let path = oid.file_path(&crate::disk_dir());
    let meta = std::fs::metadata(&path).or_else(|e| {
        if transient(&e) {
            std::fs::metadata(&path)
        } else {
            Err(e)
        }
    });
    match meta {
        Ok(meta) => Some(meta),
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => None,
        Err(e) => panic!("largeobj: failed to stat object file {path}: {e}"),
    }
}

/// Transient errors, retried once, immediately. One that recurs is not transient and
/// aborts the server.
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

// ─── FileHeader ──────────────────────────────────────────────────────────────

/// Extended attribute that holds an object file's `FileHeader`. Keeping the header
/// out of the file data makes the file exactly its payload, so a payload that is a
/// multiple of the filesystem's stripe unit doesn't spill into one more stripe unit.
pub const FILE_HEADER_XATTR: &std::ffi::CStr = c"user.lobj";
pub const FILE_HEADER_VERSION: u8 = 1;

/// Packed wire size of the header: the version, then the `FileHeader` fields.
pub const FILE_HEADER_WIRE_LEN: usize = size_of::<u8>() // version
    + size_of::<u64>()                                    // object_id
    + size_of::<u64>()                                    // len
    + size_of::<Crc>(); // crc32c

/// On-disk file header for NVMe object files, stored in the `FILE_HEADER_XATTR`
/// xattr. The file itself holds only the payload, starting at offset 0.
///
/// The struct's in-memory layout does NOT match the on-disk wire format --
/// the compiler inserts padding for natural field alignment. Serialization
/// is handled by `to_bytes` and `from_bytes`, field by field in the order above.
/// Do not attempt to byte-cast this struct.
#[derive(Debug, PartialEq)]
pub struct FileHeader {
    pub object_id: u64,
    pub len: u64,
    pub crc32c: Crc,
}

impl FileHeader {
    pub fn new(object_id: ObjectId, len: u64, crc32c: Crc) -> Self {
        Self {
            object_id: object_id.0,
            len,
            crc32c,
        }
    }

    /// Serialize: the version, then the fields back to back, little-endian.
    pub fn to_bytes(&self) -> [u8; FILE_HEADER_WIRE_LEN] {
        let fields: [&[u8]; 4] = [
            &[FILE_HEADER_VERSION],
            &self.object_id.to_le_bytes(),
            &self.len.to_le_bytes(),
            &self.crc32c.to_le_bytes(),
        ];
        let mut bytes = [0u8; FILE_HEADER_WIRE_LEN];
        let mut at = 0;
        for field in fields {
            bytes[at..at + field.len()].copy_from_slice(field);
            at += field.len();
        }
        bytes
    }

    /// Deserialize. Panics on a wrong length or version (corrupt on-disk data).
    pub fn from_bytes(bytes: &[u8]) -> Self {
        if bytes.len() != FILE_HEADER_WIRE_LEN {
            panic!(
                "largeobj: file header is {} bytes, expected {FILE_HEADER_WIRE_LEN}",
                bytes.len()
            );
        }
        let (&version, mut rest) = bytes.split_first().expect("length checked");
        if version != FILE_HEADER_VERSION {
            panic!(
                "largeobj: file header unsupported version {version} \
                 (expected {FILE_HEADER_VERSION})"
            );
        }
        // The next N bytes; the length check above keeps every take in bounds.
        let mut take = |n: usize| {
            let (field, tail) = rest.split_at(n);
            rest = tail;
            field
        };
        Self {
            object_id: u64::from_le_bytes(take(8).try_into().expect("8 bytes")),
            len: u64::from_le_bytes(take(8).try_into().expect("8 bytes")),
            crc32c: Crc::from_le_bytes(take(4).try_into().expect("4 bytes")),
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
    chunk_bytes
}

// ─── NVMe File I/O Helpers ───────────────────────────────────────────────────

/// Read the FileHeader xattr of an NVMe file and validate it against expected
/// values. Checks version, length, object_id, len, and CRC.
/// Panics on a missing or corrupt header (unrecoverable on-disk corruption). Panics
/// on CRC mismatch (serving corrupt data is worse than crashing).
pub fn read_and_verify_file_header(
    fd: RawFd,
    expected_object_id: ObjectId,
    expected_len: u64,
    crc32c_expected: Crc,
) {
    let mut bytes = [0u8; FILE_HEADER_WIRE_LEN];
    // SAFETY: the name is NUL-terminated and `bytes` is valid for `bytes.len()`.
    let n = unsafe {
        libc::fgetxattr(
            fd,
            FILE_HEADER_XATTR.as_ptr(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
        )
    };
    if n < 0 {
        panic!(
            "largeobj: file header read error for object {:?}: {}",
            expected_object_id,
            std::io::Error::last_os_error()
        );
    }
    // from_bytes panics on a corrupt length or version (unrecoverable on-disk corruption).
    let header = FileHeader::from_bytes(&bytes[..n as usize]);
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
pub fn open_nvme_file_for_write(
    file_path: &str,
    disk_len: u64,
) -> Result<ObjectFd, super::StorageError> {
    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true).mode(0o644);
    if crate::direct_io() {
        opts.custom_flags(libc::O_DIRECT);
    }
    let io_err = |e: std::io::Error| super::StorageError::IoError {
        code: e.raw_os_error().unwrap_or(libc::EIO),
    };
    let fd = super::fd_placement::open_object_file(file_path, &opts).map_err(io_err)?;
    preallocate(fd.as_raw_fd(), disk_len).map_err(io_err)?;
    Ok(fd)
}

/// Allocate all `len` bytes of a new object file in one `fallocate`, so its data blocks
/// are exactly `len` (`allocated_disk_bytes`, `metadata_disk_bytes`) and every O_DIRECT
/// chunk write lands inside EOF. Extending writes make XFS take the inode IOLOCK
/// exclusive (serializing the object's parallel chunk writes), allocate extents inside
/// each write, and preallocate past EOF. EOPNOTSUPP keeps extending writes.
pub fn preallocate(fd: RawFd, len: u64) -> std::io::Result<()> {
    if unsafe { libc::fallocate(fd, 0, 0, len as libc::off_t) } != 0 {
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EOPNOTSUPP) {
            return Err(e);
        }
    }
    Ok(())
}

/// Write the FileHeader xattr of an NVMe object file. Callers write it after the
/// payload, so a file with a header is a complete one.
pub fn write_file_header(
    fd: RawFd,
    object_id: ObjectId,
    obj_len: u64,
    crc: Crc,
) -> std::io::Result<()> {
    let bytes = FileHeader::new(object_id, obj_len, crc).to_bytes();
    // SAFETY: the name is NUL-terminated and `bytes` is valid for `bytes.len()`.
    let rc = unsafe {
        libc::fsetxattr(
            fd,
            FILE_HEADER_XATTR.as_ptr(),
            bytes.as_ptr().cast(),
            bytes.len(),
            0,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
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

    // ─── allocated_disk_bytes ────────────────────────────────────────────

    // On a 1 MiB stripe unit, a file of at least 1 MiB takes whole stripe units (a
    // 1 MiB + 4 KiB file takes 2 MiB); a shorter file is not aligned and takes its
    // own length.
    #[test]
    fn test_allocated_disk_bytes_rounds_to_stripe_unit() {
        let _g = lock();
        let unit = DISK_ALLOC_UNIT.swap(1 << 20, Ordering::Relaxed);
        assert_eq!(allocated_disk_bytes(1 << 20), 1 << 20);
        assert_eq!(allocated_disk_bytes((1 << 20) + 4096), 2 << 20);
        assert_eq!(allocated_disk_bytes(8192), 8192);
        assert_eq!(allocated_disk_bytes((1 << 20) - 4096), (1 << 20) - 4096);
        DISK_ALLOC_UNIT.store(unit, Ordering::Relaxed);
    }

    #[test]
    fn test_allocated_disk_bytes_adds_the_inode() {
        let _g = lock();
        let inode = DISK_INODE_SIZE.swap(512, Ordering::Relaxed);
        assert_eq!(allocated_disk_bytes(8192), 8192 + 512);
        DISK_INODE_SIZE.store(inode, Ordering::Relaxed);
    }

    #[test]
    fn test_metadata_disk_bytes_counts_blocks_beyond_the_data() {
        // A directory of its own, so the test never touches another run's file.
        let dir = std::env::temp_dir().join(format!("lo-metadata-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("object");
        let file = std::fs::File::create(&path).unwrap();
        preallocate(file.as_raw_fd(), 1 << 20).unwrap();
        // A written object file: its data and its header. The header fits in the inode.
        write_file_header(file.as_raw_fd(), ObjectId(1), 1 << 20, 7).unwrap();
        let with_header = metadata_disk_bytes(&file.metadata().unwrap());
        // An xattr too big for any inode takes a block of its own (tmpfs keeps it in memory).
        let big = [0u8; 2048];
        // SAFETY: the name is NUL-terminated and `big` is valid for its length.
        let rc = unsafe {
            libc::fsetxattr(
                file.as_raw_fd(),
                c"user.big".as_ptr(),
                big.as_ptr().cast(),
                big.len(),
                0,
            )
        };
        let with_big = metadata_disk_bytes(&file.metadata().unwrap());
        let mut fs: libc::statfs = unsafe { std::mem::zeroed() };
        let is_tmpfs = unsafe { libc::fstatfs(file.as_raw_fd(), &mut fs) } == 0
            && fs.f_type == libc::TMPFS_MAGIC;
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(with_header, 0, "the header must fit in the inode");
        assert_eq!(rc, 0, "set a 2 KiB xattr");
        if !is_tmpfs {
            assert!(with_big > 0, "an xattr block is metadata");
        }
    }

    // ─── FileHeader ──────────────────────────────────────────────────────

    #[test]
    fn test_file_header_round_trips() {
        let header = FileHeader::new(ObjectId(0x0102_0304_0506_0708), 0x1112_1314, 0x2122_2324);
        let bytes = header.to_bytes();
        assert_eq!(bytes[0], FILE_HEADER_VERSION);
        assert_eq!(FileHeader::from_bytes(&bytes), header);
    }

    #[test]
    #[should_panic(expected = "expected 21")]
    fn test_file_header_of_the_wrong_length_is_rejected() {
        let bytes = FileHeader::new(ObjectId(1), 2, 3).to_bytes();
        FileHeader::from_bytes(&bytes[..FILE_HEADER_WIRE_LEN - 1]);
    }

    #[test]
    #[should_panic(expected = "unsupported version")]
    fn test_file_header_of_another_version_is_rejected() {
        let mut bytes = FileHeader::new(ObjectId(1), 2, 3).to_bytes();
        bytes[0] = FILE_HEADER_VERSION + 1;
        FileHeader::from_bytes(&bytes);
    }

    // ─── object_disk_len ─────────────────────────────────────────────────

    #[test]
    fn test_object_disk_len_single_chunk() {
        // 1 byte → 1 chunk, align_up(1) = 4096. Total = 4096.
        let mut iter = super::super::ChunkIterator::new(1, 4096, 1, None);
        assert_eq!(object_disk_len(&mut iter), 4096);
    }

    #[test]
    fn test_object_disk_len_exact_multiple_aligned_chunk_size() {
        // 8192 bytes, chunk_size=4096 → 2 full chunks. Each align_up(4096)=4096.
        let mut iter = super::super::ChunkIterator::new(8192, 4096, 2, None);
        assert_eq!(object_disk_len(&mut iter), 4096 + 4096);
    }

    #[test]
    fn test_object_disk_len_partial_last_chunk_aligned_chunk_size() {
        // 10000 bytes, chunk_size=4096 → chunks: 4096, 4096, 1808.
        // align_up: 4096, 4096, 4096. Data total = 12288.
        let mut iter = super::super::ChunkIterator::new(10000, 4096, 3, None);
        assert_eq!(object_disk_len(&mut iter), 12288);
    }

    #[test]
    fn test_object_disk_len_unaligned_value_multi_page_chunk_size() {
        // 20000 bytes, chunk_size=8192 → chunks: 8192, 8192, 3616.
        // align_up(3616) = 4096. Data total = 20480.
        let mut iter = super::super::ChunkIterator::new(20000, 8192, 3, None);
        assert_eq!(object_disk_len(&mut iter), 8192 + 8192 + 4096);
    }

    #[test]
    fn test_open_for_write_preallocates_disk_len() {
        use std::os::unix::fs::MetadataExt;
        // A directory of its own, so the test never touches another run's file.
        let dir = std::env::temp_dir().join(format!("lo-prealloc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        crate::data_type::create_disk_dir_shards(dir.to_str().unwrap()).unwrap();
        let path = ObjectId(1).file_path(dir.to_str().unwrap());
        let disk_len = 2 * 1024 * 1024;
        // Nothing is written, so the size and the allocated blocks come from fallocate alone.
        let meta = open_nvme_file_for_write(&path, disk_len).map(|_fd| std::fs::metadata(&path));
        std::fs::remove_dir_all(&dir).unwrap();
        let meta = meta.expect("open for write").expect("stat");
        assert_eq!(meta.len(), disk_len);
        assert!(
            meta.blocks() * 512 >= disk_len,
            "blocks are reserved at open"
        );
    }
}

//! NVMe Storage Semantics — files, headers, capacity accounting.
//!
//! Separated from `uring.rs` (io_uring transport mechanics) so NVMe storage
//! concerns live in one place. The uring module handles ring, SQE/CQE,
//! channels, and completion streams; this module handles everything that
//! knows what an NVMe object file *is*.

use super::fd_placement::ObjectFd;
use std::fs::OpenOptions;
use std::io::Read;
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

/// Disk space each file's inode takes, set once at startup by `init_disk_geometry`:
/// XFS allocates inodes from free space, ext4 reserves them at mkfs (0). 0 in Dram mode.
static DISK_INODE_SIZE: AtomicU64 = AtomicU64::new(0);

/// What a file of `disk_len` bytes is charged before it is written: its data and its
/// inode. On a striped XFS, a file of at least one stripe unit starts on a stripe-unit
/// boundary, so the rest of its last stripe unit can't hold another such file: its data
/// is charged rounded up to the unit. A shorter file takes only its own blocks.
pub fn allocated_disk_bytes(disk_len: u64) -> u64 {
    data_disk_bytes(disk_len) + DISK_INODE_SIZE.load(Ordering::Relaxed)
}

/// The data part of `allocated_disk_bytes`.
fn data_disk_bytes(disk_len: u64) -> u64 {
    let unit = DISK_ALLOC_UNIT.load(Ordering::Relaxed);
    if disk_len >= unit {
        disk_len.next_multiple_of(unit)
    } else {
        disk_len
    }
}

/// Bytes per `st_blocks` unit, on every filesystem (glibc's `S_BLKSIZE`).
pub const ST_BLOCKS_UNIT: u64 = 512;

/// What a written file of `disk_len` bytes takes beyond its data charge, from its
/// `st_blocks` (which never counts the inode). Usually 0. Nonzero for an xattr block
/// when the header does not fit in the inode, extent-tree blocks when `fallocate`
/// returns many extents (fragmented free space), or padding the filesystem adds past
/// the allocation unit (an extent-size hint, ext4 bigalloc).
pub fn metadata_disk_bytes(disk_len: u64, blocks: u64) -> u64 {
    (blocks * ST_BLOCKS_UNIT).saturating_sub(data_disk_bytes(disk_len))
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
            "disk-dir {dir} has a block size of {block} B, which does not divide the \
             {io_align} B I/O alignment; put disk-dir on a filesystem with a block size of \
             {io_align} B or less"
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

/// Increment NVMe disk usage, without the disk-maxmemory check: a file's metadata
/// blocks, already allocated (`ObjectFile::write_header`).
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

/// Victims claimed per server-lock hold, which this bounds. A write takes as many holds
/// as it needs. The DRAM cache keeps its own `RECLAIM_MAX_VICTIMS` per reclaim.
const VICTIMS_PER_LOCK_HOLD: usize = 1024;

/// Run `f` under the server lock: COPY already holds it on main; off main (a SET's
/// tokio task) take it. Never call off main while holding it: it would deadlock.
pub(crate) fn with_server_lock<R>(f: impl FnOnce(&valkey_module::Context) -> R) -> R {
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
    // Also evict for any usage already above the cap (metadata blocks are charged uncapped).
    let need = (prev + bytes).saturating_sub(max);
    // Server lock -> UNEVICTABLE -> eviction candidates, then RECLAIM_LIST. A batch per
    // hold, so other clients get the lock between batches. Each non-empty batch takes
    // candidates off a finite list, and an empty one means none evictable is left.
    let mut victims = Vec::new();
    let mut covered = 0;
    while covered < need {
        let batch = with_server_lock(|_| {
            super::object_file::claim_victims(need - covered, VICTIMS_PER_LOCK_HOLD)
        });
        if batch.is_empty() {
            break;
        }
        covered += batch.iter().map(|(_, disk_bytes)| disk_bytes).sum::<u64>();
        victims.extend(batch);
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
        // Before the unlink, which takes the header with the file.
        super::reclaim::RECLAIM_LIST.set_key_name(oid, read_key_hint(oid));
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
/// Only the module writes this name, so a value read from it is ours: no magic.
pub const FILE_HEADER_XATTR: &std::ffi::CStr = c"user.lobj";
pub const FILE_HEADER_VERSION: u8 = 1;

/// Packed size of the fixed fields (no inter-field padding), before the key bytes.
pub const FILE_HEADER_FIXED_LEN: usize = size_of::<u8>() // version
    + size_of::<u64>()                                    // object_id
    + size_of::<u64>()                                    // len
    + size_of::<u32>()                                    // crc32c
    + size_of::<u32>()                                    // db
    + size_of::<u8>(); // key_len

/// Longest key name the header records. Keeps the whole xattr inline in a default
/// 512-byte XFS inode: measured, the value stays inline up to 223 bytes next to a
/// 33-byte SELinux label, and this leaves room for a longer label, more extents and
/// later fields. A longer key is recorded as no key.
pub const KEY_HINT_MAX_LEN: usize = 128;

/// Largest header value: the fixed fields plus the longest key.
pub const FILE_HEADER_MAX_LEN: usize = FILE_HEADER_FIXED_LEN + KEY_HINT_MAX_LEN;

/// Where the object's key was when it was written, so the reaper can delete it by
/// name after an eviction instead of scanning the keyspace for it. Only a hint:
/// RENAME, MOVE and SWAPDB never rewrite it, so the reaper checks the key still holds
/// this object before deleting it, and leaves it to the scan if not. A COPY records
/// no key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyHint {
    pub db: u32,
    /// The key name, or empty if it was longer than `KEY_HINT_MAX_LEN`.
    pub key: Vec<u8>,
}

impl KeyHint {
    pub fn new(db: u32, key: &[u8]) -> Self {
        let key = if key.len() <= KEY_HINT_MAX_LEN {
            key.to_vec()
        } else {
            Vec::new()
        };
        Self { db, key }
    }
}

/// On-disk file header for NVMe object files, stored in the `FILE_HEADER_XATTR`
/// xattr. The file itself holds only the payload, starting at offset 0.
///
/// The struct's in-memory layout does NOT match the on-disk wire format —
/// the compiler inserts padding for natural field alignment. Serialization
/// is handled by `to_bytes` (sequential writes) and `from_bytes` (sequential
/// reads with validation). Do not attempt to byte-cast this struct.
#[derive(Debug, PartialEq)]
pub struct FileHeader {
    pub version: u8,
    pub object_id: ObjectId,
    pub len: u64,
    pub crc32c: Crc,
    pub hint: KeyHint,
}

impl FileHeader {
    /// Serialize a header's fields back to back: `FILE_HEADER_FIXED_LEN` bytes, then
    /// the key.
    pub fn to_bytes(object_id: ObjectId, len: u64, crc32c: Crc, hint: &KeyHint) -> Vec<u8> {
        let key = &hint.key;
        assert!(
            key.len() <= KEY_HINT_MAX_LEN,
            "key hint longer than the cap"
        );
        let mut page = Vec::with_capacity(FILE_HEADER_FIXED_LEN + key.len());
        page.push(FILE_HEADER_VERSION);
        page.extend_from_slice(&object_id.0.to_le_bytes());
        page.extend_from_slice(&len.to_le_bytes());
        page.extend_from_slice(&crc32c.to_le_bytes());
        page.extend_from_slice(&hint.db.to_le_bytes());
        page.push(key.len() as u8);
        page.extend_from_slice(key);
        page
    }

    /// Deserialize, field by field in the order `to_bytes` writes them. Errs on a short
    /// value, an unknown version, or a key whose length doesn't match `key_len`: a
    /// truncated or corrupt value. Never panics: every short read is an error.
    pub fn from_bytes(mut bytes: &[u8]) -> Result<Self, String> {
        let r = &mut bytes;
        let [version] = read_field::<1>(r)?;
        if version != FILE_HEADER_VERSION {
            return Err(format!(
                "file header unsupported version {version} (expected {FILE_HEADER_VERSION})"
            ));
        }
        let object_id = ObjectId(u64::from_le_bytes(read_field(r)?));
        let len = u64::from_le_bytes(read_field(r)?);
        let crc32c = Crc::from_le_bytes(read_field(r)?);
        let db = u32::from_le_bytes(read_field(r)?);
        let [key_len] = read_field::<1>(r)?;
        // What is left is the key.
        let key = *r;
        if key.len() != key_len as usize {
            return Err(format!(
                "file header key_len {key_len} but {} key bytes",
                key.len()
            ));
        }
        Ok(Self {
            version,
            object_id,
            len,
            crc32c,
            hint: KeyHint {
                db,
                key: key.to_vec(),
            },
        })
    }
}

/// The next N bytes of `r`, advancing past them; errs if it holds fewer.
fn read_field<const N: usize>(r: &mut &[u8]) -> Result<[u8; N], String> {
    let mut field = [0u8; N];
    r.read_exact(&mut field)
        .map_err(|_| "file header too short".to_string())?;
    Ok(field)
}

/// The key hint in `oid`'s file header, if it records a key. `None` means the reaper
/// finds the key by scan instead. Reads by path, so the caller needs no open fd.
/// - Expected, silent: no key was recorded (a COPY, or a key over `KEY_HINT_MAX_LEN`).
///   Called on a claimed victim, whose file is complete and only its eviction unlinks.
/// - Disk or kernel fault, logged: any other read error. The hint is only a shortcut,
///   so the key is left to the scan.
/// - Bug, panics: a corrupt header, or a header for another object. Disk-dir is wiped
///   at startup, so every header was written by this run, to its own object's file.
pub fn read_key_hint(oid: ObjectId) -> Option<KeyHint> {
    let path = oid.file_path(&crate::disk_dir());
    let c_path = std::ffi::CString::new(path.as_str()).expect("object file paths have no NUL");
    let mut bytes = [0u8; FILE_HEADER_MAX_LEN];
    // SAFETY: both names are NUL-terminated and `bytes` is valid for `bytes.len()`.
    let n = unsafe {
        libc::getxattr(
            c_path.as_ptr(),
            FILE_HEADER_XATTR.as_ptr(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
        )
    };
    if n < 0 {
        let e = std::io::Error::last_os_error();
        if !matches!(e.raw_os_error(), Some(libc::ENOENT | libc::ENODATA)) {
            valkey_module::logging::log_warning(format!(
                "largeobj: cannot read the key hint of {path}: {e}; its key is found by scan"
            ));
        }
        return None;
    }
    let header = FileHeader::from_bytes(&bytes[..n as usize])
        .unwrap_or_else(|e| panic!("largeobj: {path}: {e}"));
    assert_eq!(
        header.object_id, oid,
        "largeobj: {path} has another object's header"
    );
    (!header.hint.key.is_empty()).then_some(header.hint)
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
/// values. Checks version, object_id, len, and CRC; never the key hint, which a
/// RENAME or MOVE makes stale.
/// Panics on a missing or corrupt header (unrecoverable on-disk corruption). Panics
/// on CRC mismatch (serving corrupt data is worse than crashing).
pub fn read_and_verify_file_header(
    fd: RawFd,
    expected_object_id: ObjectId,
    expected_len: u64,
    crc32c_expected: Crc,
) {
    // Sized for the longest header: a smaller buffer fails fgetxattr with ERANGE.
    let mut bytes = [0u8; FILE_HEADER_MAX_LEN];
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
    // A corrupt version or length is unrecoverable on-disk corruption.
    let header = FileHeader::from_bytes(&bytes[..n as usize])
        .unwrap_or_else(|e| panic!("largeobj: object {expected_object_id:?}: {e}"));
    if header.object_id != expected_object_id || header.len != expected_len {
        panic!(
            "largeobj: file header mismatch for object {:?}: \
             header_oid={:?} header_len={} expected_len={}",
            expected_object_id, header.object_id, header.len, expected_len
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

/// Allocate all `disk_len` bytes of a new object file in one `fallocate`, so every
/// O_DIRECT chunk write lands inside EOF. Extending writes make XFS take the inode
/// IOLOCK exclusive (serializing the object's parallel chunk writes), allocate extents
/// inside each write, and preallocate past EOF. EOPNOTSUPP keeps extending writes.
pub fn preallocate(fd: RawFd, disk_len: u64) -> std::io::Result<()> {
    if unsafe { libc::fallocate(fd, 0, 0, disk_len as libc::off_t) } != 0 {
        let e = std::io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EOPNOTSUPP) {
            return Err(e);
        }
    }
    Ok(())
}

/// Write the FileHeader xattr of an NVMe object file (`ObjectFile::write_header`).
/// Callers write it after the payload, so a file with a header is a complete one.
pub fn write_file_header(
    fd: RawFd,
    object_id: ObjectId,
    obj_len: u64,
    crc: Crc,
    hint: &KeyHint,
) -> std::io::Result<()> {
    let bytes = FileHeader::to_bytes(object_id, obj_len, crc, hint);
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
    fn test_metadata_disk_bytes_charges_data_padding_once() {
        let _g = lock();
        let unit = DISK_ALLOC_UNIT.swap(1 << 20, Ordering::Relaxed);
        let blocks = |bytes: u64| bytes / ST_BLOCKS_UNIT;
        let disk_len = (1 << 20) + 4096;
        // An extent-size hint padded the allocation to the unit `allocated_disk_bytes`
        // already charged: nothing more.
        assert_eq!(metadata_disk_bytes(disk_len, blocks(2 << 20)), 0);
        // Padded to two units: the second is charged once, here.
        assert_eq!(metadata_disk_bytes(disk_len, blocks(3 << 20)), 1 << 20);
        // Below one unit nothing is padded: an attribute block is charged.
        assert_eq!(metadata_disk_bytes(8192, blocks(8192 + 4096)), 4096);
        DISK_ALLOC_UNIT.store(unit, Ordering::Relaxed);
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

    /// COPY sizes its file as `align_up(obj_len)`: what a SET writes for any valid chunk size.
    #[test]
    fn test_object_disk_len_is_the_aligned_length() {
        let io = super::super::IO_ALIGN;
        for chunk_size in [io, 64 * io] {
            for obj_len in [1, chunk_size + 1, 3 * chunk_size + 7] {
                let mut iter =
                    super::super::ChunkIterator::new(obj_len as u64, chunk_size, 1, None);
                assert_eq!(
                    object_disk_len(&mut iter),
                    super::super::align_up(obj_len) as u64
                );
            }
        }
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
            meta.blocks() * ST_BLOCKS_UNIT >= disk_len,
            "blocks are reserved at open"
        );
    }

    // ─── FileHeader ──────────────────────────────────────────────────────

    fn header(key: &[u8]) -> FileHeader {
        FileHeader {
            version: FILE_HEADER_VERSION,
            object_id: ObjectId(0x0102_0304_0506_0708),
            len: 0x1112_1314_1516_1718,
            crc32c: 0x2122_2324,
            hint: KeyHint::new(0x3132_3334, key),
        }
    }

    fn to_bytes(h: &FileHeader) -> Vec<u8> {
        FileHeader::to_bytes(h.object_id, h.len, h.crc32c, &h.hint)
    }

    // Every field round-trips, at its fixed offset, and the key follows the fixed part.
    #[test]
    fn test_file_header_round_trips_with_its_key_hint() {
        let key = b"model@00000003@0@abcdef";
        let bytes = to_bytes(&header(key));
        assert_eq!(bytes.len(), FILE_HEADER_FIXED_LEN + key.len());
        assert_eq!(bytes[0], FILE_HEADER_VERSION);
        assert_eq!(bytes[FILE_HEADER_FIXED_LEN - 1] as usize, key.len());
        assert_eq!(&bytes[FILE_HEADER_FIXED_LEN..], key);
        assert_eq!(FileHeader::from_bytes(&bytes), Ok(header(key)));
    }

    // A key at the cap is kept; one byte over records no key, so the reaper scans for it.
    #[test]
    fn test_file_header_records_a_key_over_the_cap_as_no_key() {
        let at_cap = vec![b'k'; KEY_HINT_MAX_LEN];
        assert_eq!(KeyHint::new(7, &at_cap).key, at_cap);
        let over = vec![b'k'; KEY_HINT_MAX_LEN + 1];
        let hint = KeyHint::new(7, &over);
        assert!(hint.key.is_empty());
        let bytes = to_bytes(&header(&over));
        assert_eq!(bytes.len(), FILE_HEADER_FIXED_LEN);
        let parsed = FileHeader::from_bytes(&bytes).expect("a header with no key parses");
        assert_eq!(parsed.hint.key, b"");
        assert_eq!(to_bytes(&header(&at_cap)).len(), FILE_HEADER_MAX_LEN);
    }

    // A truncated or padded value, or another version, is corrupt.
    #[test]
    fn test_file_header_of_the_wrong_length_or_version_is_rejected() {
        let bytes = to_bytes(&header(b"key"));
        assert!(FileHeader::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        assert!(FileHeader::from_bytes(&[bytes.as_slice(), b"x"].concat()).is_err());
        assert!(FileHeader::from_bytes(&bytes[..FILE_HEADER_FIXED_LEN - 1]).is_err());
        let mut other_version = bytes.clone();
        other_version[0] = FILE_HEADER_VERSION + 1;
        assert!(FileHeader::from_bytes(&other_version).is_err());
    }

    // Every length up to past the largest header, with every key_len byte: never a panic,
    // and parsed exactly when the length is the fixed part plus key_len.
    #[test]
    fn test_file_header_from_bytes_never_panics() {
        for len in 0..=FILE_HEADER_MAX_LEN + 8 {
            for key_len in 0..=u8::MAX {
                let mut bytes = vec![0u8; len];
                if let Some(version) = bytes.first_mut() {
                    *version = FILE_HEADER_VERSION;
                }
                if let Some(slot) = bytes.get_mut(FILE_HEADER_FIXED_LEN - 1) {
                    *slot = key_len;
                }
                let parsed = FileHeader::from_bytes(&bytes).is_ok();
                assert_eq!(
                    parsed,
                    len == FILE_HEADER_FIXED_LEN + key_len as usize,
                    "len {len} key_len {key_len}"
                );
            }
        }
    }
}

//! ObjectFile — the reference-counted existence handle owned by `LoValue`.
//!
//! An `ObjectFile` represents one Tiered-mode object's *on-disk existence*: its
//! identity (`ObjectId`, from which the file path is derived) and the NVMe bytes it
//! accounts for. Objects are immutable and versioned: each write or copy creates a
//! new `ObjectFile` with a monotonically-increasing `ObjectId` (a new version),
//! and `LoValue` always references the latest one. The keyspace `LoValue` holds an
//! `Arc<ObjectFile>`, and so does every in-flight request that resolved the key. When
//! the last reference drops (`ObjectFile::Drop`) that version is gone: we deregister
//! its read fd from the `FdPool` and unlink the NVMe file.
//!
//! Safety of deletion under a concurrent read rests on two facts:
//!   1. Reference counting on two independent Arcs — a reader pins `Arc<ObjectFile>`
//!      (existence) AND holds an `Arc<OwnedFd>` clone (the open fd). Neither the file
//!      nor its fd can be reclaimed while that reader is alive, no matter which thread
//!      drops the last ObjectFile ref (a tokio worker, a lazyfree BIO thread, or the
//!      main thread on a synchronous free).
//!   2. The keyspace lookup and removal are both serialized on the main event-loop
//!      thread, so a reader either resolves the key *before* it is unlinked — taking
//!      pins that outlive the delete — or *after*, seeing it already gone. This holds
//!      for async/lazyfree deletes too: Valkey unlinks the key on the main thread and
//!      frees the value object off-thread afterward.
//!
//! Teardown (pool deregister + unlink) runs inline on whichever thread dropped the
//! last ref, except on the main event-loop thread, where blocking would stall the
//! server, so it is handed to the tokio worker pool (see `crate::is_main_thread`).
//!
//! `disk_len` is the handle's charge against `nvme-maxmemory`, credited back by whoever unlinks the
//! file: this `Drop`, or eviction, which unlinks by id without a handle (`Evicted`, off the main
//! thread). If the key is freed while eviction has the file claimed (`leave_to_eviction`), this
//! `Drop` leaves the file alone.
//!
//! `ObjectFile` is Tiered-mode-only (DRAM-only mode has no NVMe file). It has no
//! serialized form; on load a handle is reconstructed for the existing file and its
//! fd opens lazily on the first GET.

use std::collections::HashSet;
use std::os::unix::io::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use super::fd_pool::FdPool;
use super::Crc;
use crate::data_type::ObjectId;

// ─── ObjectFile ────────────────────────────────────────────────────────────────

/// Existence handle for one version of an object's NVMe file. Always held behind an
/// `Arc`; its
/// `Drop` deregisters the read fd from the pool and unlinks the file once, when the
/// last reference goes away.
#[derive(Debug)]
pub struct ObjectFile {
    /// Identity; the file path is *derived* (`ObjectId::file_path`), never stored.
    object_id: ObjectId,
    /// True on-disk size, used for NVMe utilization accounting.
    disk_len: u64,
    /// Eviction had claimed this file when its key was freed (set by `lo_free`): eviction owns the
    /// unlink and the credit, so this handle's `Drop` must do neither.
    owned_by_eviction: AtomicBool,
}

impl ObjectFile {
    /// Construct the handle for a newly committed object version whose file already
    /// exists on NVMe. No read fd is open yet — it opens lazily on the first GET via
    /// `ensure_open`. `disk_len` is the true on-disk size; `Drop` releases exactly
    /// that many bytes.
    pub fn new(object_id: ObjectId, disk_len: u64) -> Self {
        Self {
            object_id,
            disk_len,
            owned_by_eviction: AtomicBool::new(false),
        }
    }

    /// Leave this file's unlink and credit to eviction. Called when the key is freed while eviction
    /// has the file claimed.
    pub fn leave_to_eviction(&self) {
        self.owned_by_eviction.store(true, Ordering::Relaxed);
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Returns a cloned `Arc<OwnedFd>`. Calls into the `FdPool`, which owns the fd and
    /// caches it for reuse. The returned reference should be used to protect the
    /// fd from being closed while there are inflight read requests.
    pub fn ensure_open(&self, pool: &FdPool, dir: &str) -> Option<Arc<OwnedFd>> {
        pool.get_or_open(self.object_id, dir)
    }

    /// Copy this file into the new object version `reservation` pays for: writes a
    /// header carrying the new OID with this object's `len`/`crc32c`, then copies the
    /// payload past the header byte-for-byte. `fsync`s before returning so the file is
    /// durable before it is exposed to O_DIRECT reads via io_uring.
    ///
    /// The returned handle's `Drop` releases the reserved bytes. Returns `None` if any I/O
    /// fails (COPY then fails the command rather than aborting the node), leaving no partial
    /// file behind.
    pub fn copy(&self, reservation: DiskReservation, len: u64, crc32c: Crc) -> Option<ObjectFile> {
        let new_oid = reservation.object_id();
        let dir = crate::nvme_dir();
        let warn = |e: std::io::Error| {
            valkey_module::logging::log_warning(format!(
                "largeobj: Tiered COPY {:?} -> {new_oid:?} failed: {e}",
                self.object_id
            ));
        };
        let mut src = std::fs::File::open(self.object_id.file_path(&dir))
            .map_err(&warn)
            .ok()?;
        let mut dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(new_oid.file_path(&dir))
            .map_err(&warn)
            .ok()?;
        // The file exists from here, so the handle owns its cleanup: dropping it on failure
        // unlinks the file and credits the bytes, as for any other version.
        let file = reservation.into_object_file();
        match Self::copy_payload(&mut src, &mut dst, new_oid, len, crc32c) {
            Ok(()) => Some(file),
            Err(e) => {
                warn(e);
                None
            }
        }
    }

    /// Header write + payload copy for `copy`. Buffered I/O on purpose: this runs off
    /// the io_uring path, and the destination is not yet visible to any reader.
    fn copy_payload(
        src: &mut std::fs::File,
        dst: &mut std::fs::File,
        new_oid: ObjectId,
        len: u64,
        crc32c: Crc,
    ) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        dst.write_all(&super::FileHeader::new(new_oid, len, crc32c).to_page())?;
        src.seek(SeekFrom::Start(super::FILE_HEADER_SIZE))?;
        std::io::copy(src, dst)?;
        dst.sync_all()
    }
}

impl Drop for ObjectFile {
    fn drop(&mut self) {
        // Runs once, when the last ref drops: a completed deletion with no
        // remaining ObjectFile refs. Means "object gone" — deregister the fd and
        // unlink the file.
        let object_id = self.object_id;
        let disk_len = self.disk_len;
        // Last reference: `lo_free`'s store is visible through the `Arc`'s own synchronization.
        let owned_by_eviction = *self.owned_by_eviction.get_mut();

        // Deregistering drops the pool's Arc<OwnedFd>; if no in-flight reader
        // holds a clone, the fd's OwnedFd closes at this time.
        let teardown = move || {
            if let Some(pool) = super::FD_POOL.get() {
                pool.remove(object_id);
            }
            // Eviction owns this file's unlink and credit, so the bytes reach only the write that
            // evicted it. If that is done already, nothing is left to keep eviction away from.
            if owned_by_eviction {
                if !std::path::Path::new(&object_id.file_path(&crate::nvme_dir())).exists() {
                    KEYLESS_FILES.lock().remove(&object_id);
                }
                return;
            }
            let path = object_id.file_path(&crate::nvme_dir());
            // Once the file is gone there is nothing left to keep eviction away from.
            match remove_object_file(&path) {
                Ok(()) => {
                    KEYLESS_FILES.lock().remove(&object_id);
                    // Release exactly what create added — no stat, so it can't drift.
                    crate::storage::nvme::decrease_nvme_disk_usage(disk_len);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    KEYLESS_FILES.lock().remove(&object_id);
                }
                // The file stays on disk and stays keyless, so eviction never lists it (no key
                // leads to it); the bytes are credited anyway.
                Err(e) => {
                    super::warn_failed_unlink("teardown", &path, &e);
                    crate::storage::nvme::decrease_nvme_disk_usage(disk_len);
                }
            }
        };

        // The main event-loop thread must be kept syscall-free. Hand the operation
        // off to the tokio pool if the drop() is invoked from the main thread.
        if crate::is_main_thread() {
            crate::runtime_handle().spawn(async move { teardown() });
        } else {
            teardown();
        }
    }
}

/// Unlink an object file. Test hook: `test-fail-unlink` makes it fail without touching the file.
fn remove_object_file(path: &str) -> std::io::Result<()> {
    if crate::test_fail_unlink() {
        return Err(std::io::Error::from_raw_os_error(libc::EIO));
    }
    std::fs::remove_file(path)
}

// ─── DiskReservation ───────────────────────────────────────────────────────────

/// A Tiered SET or COPY's claim on the `nvme-maxmemory` budget, made on the main thread before the
/// write, so the write cannot fail for capacity. It also lists the new file as keyless, so eviction leaves it
/// alone, until the key commits or the file is gone.
pub struct DiskReservation {
    object_id: ObjectId,
    disk_len: u64,
    /// The new `ObjectFile` owes the credit and the release of the id.
    handed_off: bool,
    /// The victims whose files are to go to make room for this one.
    evicted: Evicted,
}

impl DiskReservation {
    /// `disk_len` is charged already, against the room `evicted` will make.
    pub fn charged(object_id: ObjectId, disk_len: u64, evicted: Evicted) -> Self {
        KEYLESS_FILES.lock().insert(object_id);
        Self {
            object_id,
            disk_len,
            handed_off: false,
            evicted,
        }
    }

    /// The victims' files to unlink. Left in place, dropping the reservation hands them to the
    /// background.
    pub fn take_evicted(&mut self) -> Evicted {
        std::mem::take(&mut self.evicted)
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Hand the charged bytes to the new version's handle, which now owes the release.
    pub fn into_object_file(mut self) -> ObjectFile {
        self.handed_off = true;
        ObjectFile::new(self.object_id, self.disk_len)
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        // The write never produced an `ObjectFile`: give the budget back.
        if !self.handed_off {
            super::nvme::decrease_nvme_disk_usage(self.disk_len);
            // No file was ever created for it.
            KEYLESS_FILES.lock().remove(&self.object_id);
        }
    }
}

// ─── Evicted ───────────────────────────────────────────────────────────────────

/// Victim files eviction has listed whose unlink is still owed. Until it is done their bytes stay
/// in the ledger, counted as pending (`nvme::add_pending_free`) so reservations may use them.
/// The main thread must not make the syscall (it can stall on the filesystem journal), so whoever
/// holds this unlinks off it: the SET's write task, or `Drop` in the background.
#[derive(Default)]
pub struct Evicted(Vec<(ObjectId, u64)>);

impl Evicted {
    pub fn push(&mut self, id: ObjectId, size: u64) {
        self.0.push((id, size));
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Unlink the files on this thread.
    pub fn unlink(mut self) {
        unlink_victims(std::mem::take(&mut self.0));
    }
}

impl Drop for Evicted {
    fn drop(&mut self) {
        if self.0.is_empty() {
            return;
        }
        let files = std::mem::take(&mut self.0);
        if crate::is_main_thread() {
            crate::runtime_handle().spawn_blocking(move || unlink_victims(files));
        } else {
            unlink_victims(files);
        }
    }
}

fn unlink_victims(files: Vec<(ObjectId, u64)>) {
    use super::nvme::{decrease_nvme_disk_usage, finish_pending_free};
    let dir = crate::nvme_dir();
    for (id, size) in files {
        let path = id.file_path(&dir);
        match remove_object_file(&path) {
            // Gone some other way: its bytes are free all the same.
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                super::warn_failed_unlink("eviction", &path, &e);
                // Still listed: its key lives, the object serves again, and its handle credits the
                // file when the key goes. Not listed: the key was freed meanwhile and left the
                // unlink to us, so nobody will ever remove the file.
                if !super::reclaim::RECLAIM_LIST.remove(&id) {
                    crate::eviction::DISK_LEAKED_FILES_TOTAL.fetch_add(1, Ordering::Relaxed);
                    valkey_module::logging::log_warning(format!(
                        "largeobj: leaked {path}: its key is gone and eviction could not unlink it"
                    ));
                }
                finish_pending_free(size);
                continue;
            }
        }
        finish_pending_free(size);
        decrease_nvme_disk_usage(size);
        // A key freed meanwhile left the file keyless to keep eviction off it until now.
        KEYLESS_FILES.lock().remove(&id);
    }
}

// ─── Keyless files ─────────────────────────────────────────────────────────────

/// Files with no live key behind them. Eviction picks victims from the NVMe directory, so it sees
/// files, not keys, and must not claim one of these:
/// - a SET or COPY's new file, from its reservation until its key commits: claiming it destroys a
///   write in flight;
/// - a freed key's file, until it is unlinked (by its teardown, or by the write that evicted it):
///   the file lives on for its last reader, and a claim would list an id no key leads to, so the
///   cron could never delete it, and the unlinks would both credit it.
///
/// One set serves both, since they never overlap: a key can only be freed after its write
/// committed, and a commit holds the GIL.
///
/// Lock order: `RECLAIM_LIST`, then this set.
pub static KEYLESS_FILES: LazyLock<KeylessFiles> = LazyLock::new(KeylessFiles::default);

#[derive(Default)]
pub struct KeylessFiles(Mutex<HashSet<ObjectId>>);

impl KeylessFiles {
    pub fn lock(&self) -> MutexGuard<'_, HashSet<ObjectId>> {
        self.0.lock().expect("KEYLESS_FILES lock unavailable")
    }

    /// Drop from `ids` every keyless file and return how many there were.
    pub fn filter_out(&self, ids: &mut Vec<ObjectId>) -> usize {
        let keyless = self.lock();
        let before = ids.len();
        ids.retain(|id| !keyless.contains(id));
        before - ids.len()
    }
}

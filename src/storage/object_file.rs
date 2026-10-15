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
//! `ObjectFile` is Tiered-mode-only (DRAM-only mode has no NVMe file). It has no
//! serialized form; on load a handle is reconstructed for the existing file and its
//! fd opens lazily on the first GET.

use std::collections::HashMap;
use std::os::unix::io::OwnedFd;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use super::fd_pool::FdPool;
use super::Crc;
use crate::data_type::ObjectId;

/// Unevictable list (Tiered): oids whose files are in use by a GET, SET, COPY,
/// teardown or eviction, as oid -> the `Arc<UnevictableOID>` its holders share.
/// Eviction never claims an oid whose `Weak` is live.
static UNEVICTABLE: LazyLock<Mutex<HashMap<ObjectId, Weak<UnevictableOID>>>> =
    LazyLock::new(Mutex::default);

/// Keeps its oid in the unevictable list while any `Arc` of it is alive. Every holder
/// of one oid shares the same `Arc`. Never call `new` or drop an `Arc` under the
/// `UNEVICTABLE` or `RECLAIM_LIST` lock.
pub struct UnevictableOID(ObjectId);

impl UnevictableOID {
    #[must_use]
    pub fn new(oid: ObjectId) -> Arc<Self> {
        let mut unevictable = UNEVICTABLE.lock().expect("UNEVICTABLE lock unavailable");
        match unevictable.get(&oid).and_then(Weak::upgrade) {
            Some(shared) => shared,
            None => Self::insert(&mut unevictable, oid),
        }
    }

    pub fn oid(&self) -> ObjectId {
        self.0
    }

    fn insert(unevictable: &mut HashMap<ObjectId, Weak<Self>>, oid: ObjectId) -> Arc<Self> {
        let shared = Arc::new(Self(oid));
        unevictable.insert(oid, Arc::downgrade(&shared));
        shared
    }
}

/// The last `Arc` dropped: remove the oid from the unevictable list.
impl Drop for UnevictableOID {
    fn drop(&mut self) {
        let mut unevictable = UNEVICTABLE.lock().expect("UNEVICTABLE lock unavailable");
        // A new holder may have replaced the entry before this took the lock.
        if unevictable
            .get(&self.0)
            .is_some_and(|w| w.strong_count() == 0)
        {
            unevictable.remove(&self.0);
        }
    }
}

/// Oids in the unevictable list (INFO).
pub fn unevictable_count() -> usize {
    UNEVICTABLE
        .lock()
        .expect("UNEVICTABLE lock unavailable")
        .values()
        .filter(|w| w.strong_count() > 0)
        .count()
}

/// Claim `oid` for eviction: add it to the reclaim list and the unevictable list,
/// unless it is in either already. One `UNEVICTABLE` critical section, so a
/// teardown either adds `oid` to the unevictable list first (the claim fails) or
/// finds it in the reclaim list. Call under the server lock.
#[must_use]
pub fn claim_victim(oid: ObjectId) -> Option<Arc<UnevictableOID>> {
    let mut unevictable = UNEVICTABLE.lock().expect("UNEVICTABLE lock unavailable");
    if unevictable.get(&oid).is_some_and(|w| w.strong_count() > 0)
        || !super::reclaim::RECLAIM_LIST.insert(oid)
    {
        return None;
    }
    Some(UnevictableOID::insert(&mut unevictable, oid))
}

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
        }
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

    /// Copy this file into a new object version: allocates a fresh `ObjectId`, writes a
    /// header carrying the new OID with this object's `len`/`crc32c`, then copies the
    /// payload past the header byte-for-byte. `fsync`s before returning so the file is
    /// durable before it is exposed to O_DIRECT reads via io_uring.
    ///
    /// Reserves `disk_len` against disk-maxmemory up front; the returned handle's `Drop`
    /// releases it. Returns `None` if the reservation or any I/O fails (COPY then fails
    /// the command rather than aborting the node), leaving no partial file behind.
    pub fn copy(&self, len: u64, crc32c: Crc) -> Option<ObjectFile> {
        let dir = crate::nvme_dir();
        let disk_len = self.disk_len;
        // We do not allow the eviction of the source or the new file mid-copy.
        let _source = UnevictableOID::new(self.object_id);
        if !super::nvme::try_reserve_nvme_disk_usage(disk_len) {
            return None;
        }
        let new_oid = ObjectId::next();
        let copy = UnevictableOID::new(new_oid);
        let dst_path = new_oid.file_path(&dir);
        match self.copy_file(&dst_path, new_oid, len, crc32c) {
            Ok(()) => Some(ObjectFile::new(new_oid, disk_len)),
            Err(e) => {
                if super::nvme::try_unlink(copy, disk_len, "copy") {
                    super::nvme::decrease_nvme_disk_usage(disk_len);
                }
                valkey_module::logging::log_warning(format!(
                    "largeobj: Tiered COPY {:?} -> {new_oid:?} failed: {e}",
                    self.object_id
                ));
                None
            }
        }
    }

    /// Header write + payload copy for `copy`. Buffered I/O on purpose: this runs off
    /// the io_uring path, and the destination is not yet visible to any reader.
    fn copy_file(
        &self,
        dst_path: &str,
        new_oid: ObjectId,
        len: u64,
        crc32c: Crc,
    ) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let mut src = std::fs::File::open(self.object_id.file_path(&crate::nvme_dir()))?;
        let mut dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dst_path)?;
        dst.write_all(&super::FileHeader::new(new_oid, len, crc32c).to_page())?;
        src.seek(SeekFrom::Start(super::FILE_HEADER_SIZE))?;
        std::io::copy(&mut src, &mut dst)?;
        dst.sync_all()
    }
}

impl Drop for ObjectFile {
    fn drop(&mut self) {
        // Runs once, when the last ref drops: a completed deletion with no
        // remaining ObjectFile refs. Means "object gone" — deregister the fd and,
        // unless an eviction owns it, unlink the file.
        let object_id = self.object_id;
        let disk_len = self.disk_len;
        // We do not allow the eviction of a file that is being torn down.
        let unevictable = UnevictableOID::new(object_id);

        // Deregistering drops the pool's Arc<OwnedFd>; if no in-flight reader
        // holds a clone, the fd's OwnedFd closes at this time.
        let teardown = move || {
            if let Some(pool) = super::FD_POOL.get() {
                pool.remove(object_id);
            }
            // An eviction may have claimed this file. If so, it owns the unlink and the
            // ledger credit (the ledger can't detect a second one); we only take the oid
            // off the reclaim list, which only this teardown can do now that the key is
            // gone. Otherwise we unlink and credit.
            let claimed_by_eviction = super::reclaim::RECLAIM_LIST.remove(&object_id);
            if claimed_by_eviction {
                // The eviction unlinks the file and credits its bytes.
            } else if super::nvme::try_unlink(unevictable, disk_len, "teardown") {
                // Release exactly what create added — no stat, so it can't drift.
                crate::storage::nvme::decrease_nvme_disk_usage(disk_len);
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

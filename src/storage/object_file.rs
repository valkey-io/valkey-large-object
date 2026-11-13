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
//!      (existence) AND holds an `Arc<ObjectFd>` clone (the open fd). Neither the file
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
use std::sync::{Arc, LazyLock, Mutex, Weak};

use super::fd_placement::ObjectFd;
use super::fd_pool::FdPool;
use super::Crc;
use crate::data_type::ObjectId;
use std::os::unix::io::RawFd;

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

/// Claim the oldest evictable candidates whose `disk_bytes` cover `need`, at most
/// `max`: take them off the eviction candidates and add them to the
/// reclaim list by name, and to the unevictable list. Returns each claim and
/// the `disk_bytes` its eviction credits. One `UNEVICTABLE` critical section, so a
/// teardown either takes an oid off the candidates first (it is not claimed) or finds it
/// in the reclaim list, and concurrent claims take disjoint victims. Call under the
/// server lock.
#[must_use]
pub fn claim_victims(need: u64, max: usize) -> Vec<(Arc<UnevictableOID>, u64)> {
    let mut unevictable = UNEVICTABLE.lock().expect("UNEVICTABLE lock unavailable");
    let taken = super::eviction_candidates::take(need, max, |oid| {
        !unevictable.get(&oid).is_some_and(|w| w.strong_count() > 0)
    });
    taken
        .into_iter()
        .map(|(oid, disk_bytes)| {
            // Only a claim lists a file's oid, and it takes the oid off the candidates first.
            let listed = super::reclaim::RECLAIM_LIST.insert_awaiting_key_name(oid);
            assert!(
                listed,
                "largeobj: eviction candidate {oid:?} is already on the reclaim list"
            );
            (UnevictableOID::insert(&mut unevictable, oid), disk_bytes)
        })
        .collect()
}

// ─── ObjectFile ────────────────────────────────────────────────────────────────

/// Existence handle for one version of an object's NVMe file. Always held behind an
/// `Arc`; its
/// `Drop` deregisters the read fd from the pool and unlinks the file once, when the
/// last reference goes away. The disk usage charged for the file is its
/// eviction-candidate entry's `disk_bytes`, from `new` until `Drop` or the eviction
/// that claims it.
#[derive(Debug)]
pub struct ObjectFile {
    /// Identity; the file path is *derived* (`ObjectId::file_path`), never stored.
    object_id: ObjectId,
}

impl ObjectFile {
    /// Construct the handle for a newly committed object version whose file already
    /// exists on NVMe. No read fd is open yet — it opens lazily on the first GET via
    /// `ensure_open`. `disk_len` is the file length: it becomes an eviction candidate
    /// charged the `allocated_disk_bytes` reserved for it. The caller holds it
    /// unevictable until `write_header`.
    pub fn new(object_id: ObjectId, disk_len: u64) -> Self {
        super::eviction_candidates::insert(object_id, super::nvme::allocated_disk_bytes(disk_len));
        Self { object_id }
    }

    /// Write the file's header through `fd`, its last write once the payload is
    /// written, then charge what the file took beyond what `new` reserved.
    /// - `disk_len`: the file's length, the payload with each chunk padded to
    ///   `IO_ALIGN` (`object_disk_len`), which the caller reserved and preallocated.
    /// - `obj_len`, `crc32c`, `hint`: the object's length, CRC and key, which the header
    ///   records.
    ///
    /// The reservation already covers the data and the inode (`allocated_disk_bytes`).
    /// The `fstat` measures what only exists once the file is written: blocks beyond
    /// the data, such as an xattr block if the header does not fit in the inode
    /// (`metadata_disk_bytes`). Charged without a disk-maxmemory check: the blocks are
    /// already allocated, and the next reservation evicts for them. On error, `Drop`
    /// still unlinks the file and releases what was charged.
    pub fn write_header(
        &self,
        fd: RawFd,
        disk_len: u64,
        obj_len: u64,
        crc32c: Crc,
        hint: &super::KeyHint,
    ) -> std::io::Result<()> {
        super::write_file_header(fd, self.object_id, obj_len, crc32c, hint)?;
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: `st` is the struct fstat writes.
        if unsafe { libc::fstat(fd, &mut st) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        assert_eq!(
            st.st_size as u64, disk_len,
            "NVMe accounting: object {:?} on disk is {} B but we reserved {} B — write \
             path and accounting have diverged",
            self.object_id, st.st_size, disk_len
        );
        let bytes = super::nvme::metadata_disk_bytes(disk_len, st.st_blocks as u64);
        if bytes > 0 {
            crate::info::DISK_METADATA_SPILLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        super::nvme::increase_nvme_disk_usage(bytes);
        // Unevictable since `new`, so no claim has taken its entry.
        let recorded = super::eviction_candidates::add_disk_bytes(self.object_id, bytes);
        assert!(
            recorded,
            "largeobj: object {:?} lost its eviction-candidate entry before its header",
            self.object_id
        );
        Ok(())
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Returns a cloned `Arc<ObjectFd>`. Calls into the `FdPool`, which owns the fd and
    /// caches it for reuse. The returned reference should be used to protect the
    /// fd from being closed while there are inflight read requests.
    pub fn ensure_open(&self, pool: &FdPool, dir: &str) -> Option<Arc<ObjectFd>> {
        pool.get_or_open(self.object_id, dir)
    }

    /// Copy this file into a new object version: allocates a fresh `ObjectId`, copies the
    /// payload byte-for-byte, then writes a header carrying the new OID with this
    /// object's `len`/`crc32c`. `fsync`s the payload before the header, so the file is
    /// on disk before it is exposed to O_DIRECT reads via io_uring.
    ///
    /// Reserves the source's length against disk-maxmemory up front; the returned
    /// handle's `Drop` releases it. Returns `None` if the reservation or any I/O fails
    /// (COPY then fails the command rather than aborting the node), leaving no partial
    /// file behind.
    pub fn copy(&self, obj_len: u64, crc32c: Crc) -> Option<ObjectFile> {
        // We do not allow the eviction of the source or the new file mid-copy.
        let _source = UnevictableOID::new(self.object_id);
        // The source's file length: chunk-size is a multiple of IO_ALIGN, so a SET's file
        // is `align_up(obj_len)`.
        let disk_len = super::align_up(obj_len as usize) as u64;
        if !super::nvme::try_reserve_nvme_disk_usage(super::nvme::allocated_disk_bytes(disk_len)) {
            return None;
        }
        let new_oid = ObjectId::next();
        let _copy = UnevictableOID::new(new_oid);
        // Owns cleanup from here: Drop removes the file and releases disk budget.
        let copy = ObjectFile::new(new_oid, disk_len);
        match self.copy_file(&copy, disk_len, obj_len, crc32c) {
            Ok(()) => Some(copy),
            Err(e) => {
                valkey_module::logging::log_warning(format!(
                    "largeobj: Tiered COPY {:?} -> {new_oid:?} failed: {e}",
                    self.object_id
                ));
                None
            }
        }
    }

    /// Payload copy + header write for `copy`. Buffered I/O on purpose: this runs off
    /// the io_uring path, and the destination is not yet visible to any reader.
    fn copy_file(
        &self,
        copy: &ObjectFile,
        disk_len: u64,
        obj_len: u64,
        crc32c: Crc,
    ) -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let dir = crate::storage::disk_dir();
        let mut src = std::fs::File::open(self.object_id.file_path(dir))?;
        let mut dst = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(copy.object_id.file_path(dir))?;
        super::nvme::preallocate(dst.as_raw_fd(), disk_len)?;
        std::io::copy(&mut src, &mut dst)?;
        dst.sync_all()?;
        // No key hint: the copy callback doesn't know the destination db, so an evicted
        // copy's key is found by scan.
        copy.write_header(
            dst.as_raw_fd(),
            disk_len,
            obj_len,
            crc32c,
            &super::KeyHint::default(),
        )
    }
}

impl Drop for ObjectFile {
    fn drop(&mut self) {
        // Runs once, when the last ref drops: a completed deletion with no
        // remaining ObjectFile refs. Means "object gone" — deregister the fd and,
        // unless an eviction owns it, unlink the file.
        let object_id = self.object_id;
        // Off the candidates, so no eviction can claim a file being torn down. `None`:
        // an eviction claimed it first, and owns its unlink and credit.
        let disk_bytes = super::eviction_candidates::remove(object_id);
        // We do not allow the eviction of a file that is being torn down.
        let unevictable = UnevictableOID::new(object_id);

        // Deregistering drops the pool's Arc<ObjectFd>; if no in-flight reader
        // holds a clone, the fd closes at this time.
        let teardown = move || {
            // Moved in so it outlives drop(): a spawned teardown unlinks later.
            let _unevictable = unevictable;
            if let Some(pool) = super::FD_POOL.get() {
                pool.remove(object_id);
            }
            // A claim takes the candidate entry and then lists the oid, so the entry and
            // the listing exclude each other. We take the oid off the reclaim list, which
            // only this teardown can do now that the key is gone.
            let claimed_by_eviction = super::reclaim::RECLAIM_LIST.remove(&object_id);
            match (disk_bytes, claimed_by_eviction) {
                // The eviction unlinks the file and credits its bytes.
                (None, true) => {}
                (Some(disk_bytes), false) => {
                    super::nvme::unlink(object_id);
                    // Release exactly what was charged — no stat, so it can't drift.
                    crate::storage::nvme::decrease_nvme_disk_usage(disk_bytes);
                }
                (disk_bytes, claimed) => panic!(
                    "largeobj: object {object_id:?} torn down with candidate charge \
                     {disk_bytes:?} and claimed_by_eviction={claimed}"
                ),
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

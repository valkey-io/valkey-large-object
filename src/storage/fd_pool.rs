//! File Descriptor Pool — owns and caches open read fds as `Arc<OwnedFd>`, keyed by `ObjectId`.
//!
//! A read fd is opened lazily on the first GET (via `ObjectFile::ensure_open` →
//! `get_or_open`) and cached for reuse; every later GET and in-flight reader gets a clone.
//! Being an `Arc<OwnedFd>`, it closes itself (RAII) once the last ref — the cache entry
//! plus any reader clones — is gone. An fd is transient metadata, never persisted on the
//! `LoValue` data type; it lives only in this pool, decoupled from object lifetime.
//!
//! The pool earns its keep for three jobs:
//!   1. **Reuse** — avoid a fresh `open()` on every GET.
//!   2. **Serialize the lazy first-open** — the write lock stops two concurrent first-GETs
//!      on a cold object from both `open()`-ing and leaking an fd.
//!   3. **Own the fd independently of `ObjectFile`** — demotion drops the pool's ref to
//!      reclaim a cold fd without disturbing in-flight readers that still hold one.
//!
//! Cap and demotion (`docs/CACHE_POLICY_DESIGN.md` §4.8): with `max-open-fds` set, a
//! full pool demotes the lowest-scoring fd not held by a reader. If every sampled fd is
//! held, the new fd is handed out uncached and closes when the reader finishes.
//!
//! `remove` drops the pool's ref; `ObjectFile::Drop` (on delete) calls it. There is no
//! `Drop for FdPool` — it is a process-lifetime static, so any fds still cached at
//! teardown are reclaimed by process exit.

use std::os::unix::io::{FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::cache_policy::{demote_one, now_minutes, AccessStats, IndexedMap};
use crate::data_type::ObjectId;

/// One cached fd plus its LFU score.
struct FdEntry {
    fd: Arc<OwnedFd>,
    stats: AccessStats,
}

pub struct FdPool {
    fds: RwLock<IndexedMap<FdEntry>>,
    /// Cached fds dropped to stay under `max-open-fds`.
    pub demotions: AtomicU64,
}

impl Default for FdPool {
    fn default() -> Self {
        Self::new()
    }
}

impl FdPool {
    pub fn new() -> Self {
        Self {
            fds: RwLock::new(IndexedMap::new()),
            demotions: AtomicU64::new(0),
        }
    }

    /// Return the cached read fd for `object_id`, opening + caching it if not present.
    /// A clone of `Arc<OwnedFd>` is returned to the caller. The reference can be
    /// used to prevent the underlying fd from being closed during inflight read requests.
    /// Returns `None` only on a genuine `open()` failure.
    pub fn get_or_open(&self, object_id: ObjectId, dir: &str) -> Option<Arc<OwnedFd>> {
        self.get_or_open_with(object_id, dir, crate::max_open_fds())
    }

    /// `get_or_open` with an explicit `max-open-fds` cap (0 = unlimited), so
    /// parallel unit tests do not share the global config.
    pub(crate) fn get_or_open_with(
        &self,
        object_id: ObjectId,
        dir: &str,
        cap: usize,
    ) -> Option<Arc<OwnedFd>> {
        let now_min = now_minutes();
        let decay_time = crate::tiered_decay_time();
        let hit = |entry: &FdEntry| {
            entry.stats.touch(now_min, decay_time);
            Arc::clone(&entry.fd)
        };
        // Fast path: shared read lock, touch the score and clone the cached handle.
        {
            let fds = self.fds.read().expect("FdPool.fds lock unavailable");
            if let Some(entry) = fds.get(&object_id) {
                return Some(hit(entry));
            }
        }

        // Slow path: serialize opens through the write lock.
        let mut fds = self.fds.write().expect("FdPool.fds lock unavailable");

        // Re-check under the lock: another caller may have opened it meanwhile.
        if let Some(entry) = fds.get(&object_id) {
            return Some(hit(entry));
        }

        // Make room before opening, so the cap counts the new entry. An entry is
        // demotable only when the map holds its sole Arc; cloning needs the read
        // lock, so a count of 1 seen here stays 1. Victims close after unlock.
        let mut victims = Vec::new();
        if cap > 0 && fds.len() >= cap {
            let samples = crate::demote_sample_size();
            for _ in 0..fds.len() + 1 - cap {
                let Some((_, entry)) = demote_one(&mut fds, samples, |e| {
                    (Arc::strong_count(&e.fd) == 1)
                        .then(|| e.stats.decayed_counter(now_min, decay_time))
                }) else {
                    break;
                };
                victims.push(entry.fd);
            }
            self.demotions
                .fetch_add(victims.len() as u64, Ordering::Relaxed);
        }
        let cache_it = cap == 0 || fds.len() < cap;

        let path = object_id.file_path(dir);
        let c_path = std::ffi::CString::new(path).expect("file_path null");
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= libc::O_DIRECT;
        }
        // SAFETY: c_path is a valid NUL-terminated path; open returns a fd or -1.
        let raw = unsafe { libc::open(c_path.as_ptr(), flags) };
        if raw < 0 {
            drop(fds);
            drop(victims);
            return None;
        }
        let fd = Arc::new(unsafe { OwnedFd::from_raw_fd(raw) });
        if cache_it {
            fds.insert(
                object_id,
                FdEntry {
                    fd: Arc::clone(&fd),
                    stats: AccessStats::new(now_min),
                },
            );
        }
        drop(fds);
        // Closing the victims happens here, after the lock is released.
        drop(victims);
        Some(fd)
    }

    /// Drop the pool's ref to `object_id`'s fd. The fd closes once this ref and all
    /// in-flight reader clones are gone. Called from `ObjectFile::Drop` (on delete).
    pub fn remove(&self, object_id: ObjectId) {
        // Only hold the write lock for remove(). Once the lock is released we can drop
        // the reference which may trigger the drop().
        let removed = self
            .fds
            .write()
            .expect("FdPool.fds lock unavailable")
            .swap_remove(&object_id);
        drop(removed);
    }

    /// Number of cached fds. Test/introspection helper.
    pub fn len(&self) -> usize {
        self.fds.read().expect("FdPool.fds lock unavailable").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `object_id`'s fd is cached right now. Test helper.
    #[cfg(test)]
    fn contains(&self, object_id: ObjectId) -> bool {
        self.fds
            .read()
            .expect("FdPool.fds lock unavailable")
            .contains_key(&object_id)
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ObjectId;
    use std::os::unix::io::AsRawFd;

    // get_or_open uses O_DIRECT when direct_io() is set and can fail on some
    // filesystems (e.g. tmpfs). When the open fails we skip the fd-dependent
    // assertions — the Python integration tests cover the real NVMe path.

    /// Files for `n` objects in a fresh temp dir. Removed on drop.
    struct Files {
        dir: String,
        oids: Vec<ObjectId>,
    }

    impl Files {
        fn new(tag: u64, n: usize) -> Self {
            let dir = std::env::temp_dir().join(format!("lo_fdpool_{tag}_{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let dir = dir.to_str().unwrap().to_string();
            let oids: Vec<ObjectId> = (0..n as u64).map(|i| ObjectId(tag * 1000 + i)).collect();
            for oid in &oids {
                std::fs::write(oid.file_path(&dir), b"x").unwrap();
            }
            Self { dir, oids }
        }
    }

    impl Drop for Files {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn test_remove_is_idempotent() {
        let pool = FdPool::new();
        // Removing an unregistered object_id is a harmless no-op.
        pool.remove(ObjectId(999));
        assert!(pool.is_empty());
    }

    #[test]
    fn test_get_or_open_caches_and_reuses() {
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let object_id = ObjectId(0x5151);
        let path = object_id.file_path(dir);
        std::fs::write(&path, b"hello").unwrap();

        let pool = FdPool::new();
        if let Some(fd1) = pool.get_or_open(object_id, dir) {
            assert_eq!(pool.len(), 1, "open registers exactly one cached fd");
            // Second call reuses the cached handle (a clone of the same Arc).
            let fd2 = pool.get_or_open(object_id, dir).expect("cached fd");
            assert_eq!(fd1.as_raw_fd(), fd2.as_raw_fd());
            assert!(
                Arc::ptr_eq(&fd1, &fd2),
                "reuse returns clones of the same Arc"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reader_clone_keeps_fd_open_across_remove() {
        // The honor rule at the refcount level: a reader holding an Arc<OwnedFd> clone
        // keeps the fd open even after the pool drops its own ref (as ObjectFile::Drop
        // does on delete); the fd closes only when the last clone goes away.
        let dir = std::env::temp_dir();
        let dir = dir.to_str().unwrap();
        let object_id = ObjectId(0x6262);
        let path = object_id.file_path(dir);
        std::fs::write(&path, b"world").unwrap();

        let pool = FdPool::new();
        if let Some(reader) = pool.get_or_open(object_id, dir) {
            let weak = Arc::downgrade(&reader);
            assert_eq!(pool.len(), 1);

            // Pool drops its ref; the reader clone is still alive, so fd stays open.
            pool.remove(object_id);
            assert_eq!(pool.len(), 0);
            assert!(
                weak.upgrade().is_some(),
                "fd must stay open while a reader clone is alive"
            );

            // Last clone drops -> OwnedFd::drop closes the fd.
            drop(reader);
            assert!(
                weak.upgrade().is_none(),
                "fd must be closed once the last clone drops"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    // ─── Cap and demotion ───

    #[test]
    fn cap_demotes_lowest_score_and_keeps_hot() {
        let files = Files::new(1, 4);
        let pool = FdPool::new();
        let k = 3;

        // Fill to the cap; skip if this filesystem refuses the open.
        let Some(_) = pool.get_or_open_with(files.oids[0], &files.dir, k) else {
            return;
        };
        for oid in &files.oids[1..3] {
            pool.get_or_open_with(*oid, &files.dir, k).unwrap();
        }
        // Touch oid[1] and oid[2]. The first hit on a fresh entry always
        // increments, so the untouched oid[0] is the unique minimum.
        for oid in &files.oids[1..3] {
            pool.get_or_open_with(*oid, &files.dir, k).unwrap();
        }

        // A fourth open must demote exactly one entry: the cold oid[0].
        pool.get_or_open_with(files.oids[3], &files.dir, k).unwrap();
        assert_eq!(pool.len(), 3, "cap respected");
        assert!(!pool.contains(files.oids[0]), "cold fd demoted");
        assert!(files.oids[1..].iter().all(|o| pool.contains(*o)));
        assert_eq!(pool.demotions.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn all_pinned_hands_out_uncached_fd() {
        let files = Files::new(3, 3);
        let pool = FdPool::new();
        let k = 2;

        let Some(r0) = pool.get_or_open_with(files.oids[0], &files.dir, k) else {
            return;
        };
        let r1 = pool.get_or_open_with(files.oids[1], &files.dir, k).unwrap();

        // Pool full, both pinned: the third open succeeds but is not cached.
        let extra = pool.get_or_open_with(files.oids[2], &files.dir, k).unwrap();
        assert_eq!(pool.len(), 2, "cap holds");
        assert!(!pool.contains(files.oids[2]));
        assert_eq!(pool.demotions.load(Ordering::Relaxed), 0);
        drop(extra);

        // Once a reader releases, the next open caches again.
        drop(r0);
        pool.get_or_open_with(files.oids[2], &files.dir, k).unwrap();
        assert!(pool.contains(files.oids[2]));
        assert!(!pool.contains(files.oids[0]));
        assert_eq!(pool.len(), 2);
        drop(r1);
    }
}

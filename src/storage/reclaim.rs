//! Reclaim list: oids of ghost keys, whose payload is gone or going (a Tiered
//! eviction unlinks the file after claiming it) while the key still exists. In
//! Tiered mode, in the list means the eviction owns the file's unlink and credit.
//!
//! Commands check it upfront and treat a key in the list as missing; the scaling
//! cron deletes the keys. An oid leaves the list:
//! - Dram: when its object is removed (`DRAMPool::remove_object`, reached from
//!   `lo_free` once the key is deleted).
//! - Tiered: when its file is torn down (`ObjectFile::drop`), or when an NVMe
//!   eviction finds its file already gone (a stale directory entry).
//!
//! The reaper (the scaling cron) deletes each victim's key one of two ways:
//! - By name (Tiered): an eviction lists the victim awaiting its key name, then reads it
//!   (its key's name and db when it was written, from the file header) before the
//!   unlink (`set_key_name`).
//! - By scan: the fallback for ghost keys with no usable hint (Dram, renamed, copied, a
//!   key over the hint cap). The scan runs only while such a ghost key is listed.
//!
//! A deleted key's oid stays listed until its value is freed (`lo_free`, on a BIO
//! thread), marked `KeyState::Deleted` so neither way spends work on it.
//!
//! Lock order: `RECLAIM_LIST` before `DRAMPool.objects` when both are held.
//!
//! Also the previous run's disk-dir files, reclaimed at startup (below).

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, ReadDir};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use valkey_module::{raw, Context, KeysCursor, ValkeyString};

use super::nvme::KeyHint;
use crate::data_type::{LoValue, ObjectId, LO_TYPE};

/// INFO: evicted keys the reaper deleted by name, and by scan.
pub static EVICTED_KEYS_BY_NAME: AtomicU64 = AtomicU64::new(0);
pub static EVICTED_KEYS_BY_SCAN: AtomicU64 = AtomicU64::new(0);

pub static RECLAIM_LIST: LazyLock<ReclaimList> = LazyLock::new(ReclaimList::default);

/// How the reaper deletes a listed oid's key.
#[derive(Clone, Copy, Debug, PartialEq)]
enum KeyState {
    /// Queued on `by_name` (an eviction queues it once it has read its key name), which
    /// the reaper empties before it scans: never the scan's.
    ByName,
    /// Found by scan: no hint, or its key no longer held it when looked up by name.
    ByScan,
    /// Key deleted; listed until its value is freed.
    Deleted,
}

#[derive(Default)]
struct GhostKeys {
    states: HashMap<ObjectId, KeyState>,
    /// The `ByName` ghost keys, oldest first, with their hints. A popped entry whose oid
    /// is no longer `ByName` is skipped. Empty whenever `states` is.
    by_name: VecDeque<(ObjectId, KeyHint)>,
    /// How many of `states` are `ByScan`.
    num_by_scan: usize,
}

impl GhostKeys {
    /// List `oid`: by name if `hint` is given, else by scan. False if already listed.
    fn insert(&mut self, oid: ObjectId, hint: Option<KeyHint>) -> bool {
        if self.states.contains_key(&oid) {
            return false;
        }
        let state = match hint {
            Some(hint) => {
                self.by_name.push_back((oid, hint));
                KeyState::ByName
            }
            None => {
                self.num_by_scan += 1;
                KeyState::ByScan
            }
        };
        self.states.insert(oid, state);
        true
    }

    /// List `oid` before its key name is known: `ByName`, but not queued until
    /// `set_key_name`. False if already listed.
    fn insert_awaiting_key_name(&mut self, oid: ObjectId) -> bool {
        if self.states.contains_key(&oid) {
            return false;
        }
        self.states.insert(oid, KeyState::ByName);
        true
    }

    /// Give an `insert_awaiting_key_name` oid its key name: queued to be deleted by name,
    /// or, with none, left to the scan. No-op if it left the list: its key was deleted and
    /// its value freed.
    fn set_key_name(&mut self, oid: ObjectId, key_name: Option<KeyHint>) {
        if self.states.get(&oid) != Some(&KeyState::ByName) {
            return;
        }
        match key_name {
            Some(key_name) => self.by_name.push_back((oid, key_name)),
            None => self.set_state(oid, KeyState::ByScan),
        }
    }

    /// Move a listed `oid` to `to`. No-op if it is not listed.
    fn set_state(&mut self, oid: ObjectId, to: KeyState) {
        let Some(state) = self.states.get_mut(&oid) else {
            return;
        };
        let was = std::mem::replace(state, to);
        if was == KeyState::ByScan {
            self.num_by_scan -= 1;
        }
        if to == KeyState::ByScan {
            self.num_by_scan += 1;
        }
    }

    /// Unlist `oid`; false if it was not listed.
    fn remove(&mut self, oid: &ObjectId) -> bool {
        let Some(state) = self.states.remove(oid) else {
            return false;
        };
        if state == KeyState::ByScan {
            self.num_by_scan -= 1;
        }
        if self.states.is_empty() {
            // Every queued entry left is stale.
            self.by_name.clear();
        }
        true
    }
}

#[derive(Default)]
pub struct ReclaimList {
    ghost_keys: Mutex<GhostKeys>,
    /// `ghost_keys.states.len()`. Store only under the lock, or it can read 0 for a non-empty list.
    len: AtomicUsize,
}

impl ReclaimList {
    /// Private so every change goes through the methods below, which update `len`.
    fn lock(&self) -> MutexGuard<'_, GhostKeys> {
        self.ghost_keys
            .lock()
            .expect("RECLAIM_LIST lock unavailable")
    }

    /// Adds the oids returned by `f` to the reclaim list, found by scan. The list stays
    /// locked while `f` runs. Call with the server lock held (main thread, or
    /// `ThreadSafeContext::lock()`).
    pub fn add_with<I: IntoIterator<Item = ObjectId>>(&self, f: impl FnOnce() -> I) {
        let mut ghost_keys = self.lock();
        for oid in f() {
            ghost_keys.insert(oid, None);
        }
        self.len.store(ghost_keys.states.len(), Ordering::Release);
    }

    /// Add `oid` to the reclaim list: deleted by name if `hint` is given, else by scan.
    /// False if it was already there.
    pub fn insert(&self, oid: ObjectId, hint: Option<KeyHint>) -> bool {
        let mut ghost_keys = self.lock();
        let inserted = ghost_keys.insert(oid, hint);
        self.len.store(ghost_keys.states.len(), Ordering::Release);
        inserted
    }

    /// Add `oid` to the reclaim list before its key name is known (an eviction's claim):
    /// neither the by-name pass nor the scan takes it until `set_key_name`. False if it
    /// was already there.
    pub fn insert_awaiting_key_name(&self, oid: ObjectId) -> bool {
        let mut ghost_keys = self.lock();
        let inserted = ghost_keys.insert_awaiting_key_name(oid);
        self.len.store(ghost_keys.states.len(), Ordering::Release);
        inserted
    }

    /// Give an `insert_awaiting_key_name` oid its key name: deleted by name, or, with
    /// none, by scan.
    pub fn set_key_name(&self, oid: ObjectId, key_name: Option<KeyHint>) {
        self.lock().set_key_name(oid, key_name);
    }

    pub fn contains(&self, oid: &ObjectId) -> bool {
        !self.is_empty() && self.lock().states.contains_key(oid)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove `oid` from the list; false if it was not there. Always locks:
    /// `remove_object` must wait for an eviction that is adding this oid.
    pub fn remove(&self, oid: &ObjectId) -> bool {
        let mut ghost_keys = self.lock();
        let removed = ghost_keys.remove(oid);
        self.len.store(ghost_keys.states.len(), Ordering::Release);
        removed
    }

    /// Move a listed `oid` to `to`. No-op if it already left the list.
    fn set_state(&self, oid: ObjectId, to: KeyState) {
        self.lock().set_state(oid, to);
    }

    /// Ghost keys left to the scan.
    fn num_by_scan(&self) -> usize {
        self.lock().num_by_scan
    }

    /// The oldest ghost key still to delete by name, and its hint. Skips stale entries.
    /// None when none is left, or `deadline` has passed.
    fn pop_by_name(&self, deadline: Instant) -> Option<(ObjectId, KeyHint)> {
        let mut ghost_keys = self.lock();
        while Instant::now() < deadline {
            let (oid, hint) = ghost_keys.by_name.pop_front()?;
            if ghost_keys.states.get(&oid) == Some(&KeyState::ByName) {
                return Some((oid, hint));
            }
        }
        None
    }
}

/// INFO: ghost keys queued for deletion by name.
pub fn pending_evictions_by_name() -> usize {
    RECLAIM_LIST.lock().by_name.len()
}

/// Select db `db` on `ctx`. False past the last db.
fn select_db(ctx: &Context, db: i32) -> bool {
    unsafe { raw::RedisModule_SelectDb.unwrap()(ctx.get_raw(), db) == raw::REDISMODULE_OK as i32 }
}

/// Where the reclaim scan resumes on the next tick.
struct ReclaimScanCursor {
    db: i32,
    cursor: KeysCursor,
}

thread_local! {
    // Scaling cron only, which always runs on the main event-loop thread.
    static RECLAIM_SCAN_CURSOR: RefCell<ReclaimScanCursor> = RefCell::new(ReclaimScanCursor { db: 0, cursor: KeysCursor::new() });
}

/// Delete keys whose oids are on the reclaim list, spending at most
/// `reaper-scan-budget-us` of main-thread time per tick: first the ghost keys with a
/// key hint, by name, then the rest by scan.
///
/// Cross-slot deletion is safe here: a timer callback runs outside command
/// execution (`server.current_client` is NULL), so key lookups hash each key's
/// own slot instead of reusing a command's cached slot.
pub fn delete_reclaimed_keys(ctx: &Context) {
    if RECLAIM_LIST.is_empty() {
        return;
    }
    let deadline = Instant::now() + Duration::from_micros(crate::reaper_scan_budget_us());
    delete_keys_by_name(ctx, deadline);
    delete_keys_by_scan(ctx, deadline);
}

/// Delete, by scan, the `ByScan` ghost keys, until `deadline`. Goes db by db,
/// resuming across ticks, while one is listed: a ghost key stops being `ByScan` once its
/// key is deleted, here, by name, or by any command.
fn delete_keys_by_scan(ctx: &Context, deadline: Instant) {
    RECLAIM_SCAN_CURSOR.with_borrow_mut(|scan| {
        while RECLAIM_LIST.num_by_scan() > 0 && Instant::now() < deadline {
            // Past the last db: one full pass done. At most one pass per tick
            // so a key moved behind the cursor can't spin us.
            if !select_db(ctx, scan.db) {
                scan.db = 0;
                break;
            }
            let (listed_keys, more) = listed_keys_in_next_bucket(ctx, &scan.cursor);
            for (name, oid) in listed_keys {
                // Unlink fails only on a key not opened for writing (VM_UnlinkKey).
                ctx.open_key_writable(&name)
                    .unlink()
                    .expect("a key opened for writing unlinks");
                RECLAIM_LIST.set_state(oid, KeyState::Deleted);
                EVICTED_KEYS_BY_SCAN.fetch_add(1, Ordering::Relaxed);
                // Same keyspace event core fires for its own evictions.
                ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
            }
            if !more {
                scan.cursor.restart();
                scan.db += 1;
            }
        }
    });
}

/// Delete, by name, the `ByName` ghost keys, until `deadline`.
/// A key is deleted only if it still holds its victim: oids are never reused, so a key
/// that was renamed, moved, swapped to another db, overwritten or replaced since holds
/// another oid, or is gone. Such a victim is left to the scan. Runs on the main thread,
/// like every keyspace change, so nothing moves the key between the check and the delete.
fn delete_keys_by_name(ctx: &Context, deadline: Instant) {
    while let Some((oid, hint)) = RECLAIM_LIST.pop_by_name(deadline) {
        // Fails only for a db outside 0..databases (selectDb). The hint holds a client's
        // selected db from this run, and databases cannot change at runtime.
        assert!(
            select_db(ctx, hint.db as i32),
            "largeobj: key hint of object {oid:?} names db {}, which does not exist",
            hint.db
        );
        let name = ValkeyString::create_from_slice(ctx.get_raw(), &hint.key);
        let key = ctx.open_key_writable(&name);
        // Gone, another type, or another object: the key moved on since the write (a
        // rename, overwrite or delete), so it is not the victim's. Left to the scan.
        let holds_victim = matches!(
            key.get_value::<LoValue>(&LO_TYPE),
            Ok(Some(lo)) if lo.object_id == oid
        );
        if !holds_victim {
            RECLAIM_LIST.set_state(oid, KeyState::ByScan);
            continue;
        }
        // Unlink fails only on a key not opened for writing (VM_UnlinkKey).
        key.unlink().expect("a key opened for writing unlinks");
        RECLAIM_LIST.set_state(oid, KeyState::Deleted);
        EVICTED_KEYS_BY_NAME.fetch_add(1, Ordering::Relaxed);
        // Same keyspace event core fires for its own evictions.
        ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
    }
}

/// Scan the next hashtable bucket of the selected db. Returns the bucket's keys
/// whose oids are `ByScan`, and whether the db has more buckets.
///
/// Only collects, never deletes: core reads each scanned value after the scan
/// callback returns (`moduleCloseKey`), so unlinking a key inside the callback
/// hands BIO an object core still dereferences.
///
/// Holds the reclaim list lock for the whole bucket, not per key. Nothing in the
/// callback can re-enter it: core opens each scanned key read-only with its value
/// in hand (`moduleScanCallback`), so no lookup expires or frees a key mid-scan.
fn listed_keys_in_next_bucket(
    ctx: &Context,
    cursor: &KeysCursor,
) -> (Vec<(ValkeyString, ObjectId)>, bool) {
    let listed = RefCell::new(Vec::new());
    let ghost_keys = RECLAIM_LIST.lock();
    // scan() calls this once per key in the bucket.
    let more = cursor.scan(ctx, &|_ctx, name, key| {
        let Some(Ok(Some(lo))) = key.map(|k| k.get_value::<LoValue>(&LO_TYPE)) else {
            return;
        };
        if ghost_keys.states.get(&lo.object_id) == Some(&KeyState::ByScan) {
            listed.borrow_mut().push((name, lo.object_id));
        }
    });
    (listed.into_inner(), more)
}

// ─── Previous-run cleanup ────────────────────────────────────────────────────
//
// Tiered: deleting disk-dir takes time proportional to what is in it, and OIDs restart
// at 1 every run, so a new object would reuse an old object's path. So startup renames
// everything in disk-dir into `disk-dir/previous-run-<nanos>/` (one rename per shard),
// charges the space it holds against disk-maxmemory, and deletes it on a background
// thread. A reservation that does not fit deletes some of it inline before evicting
// live objects (`nvme::try_reserve_nvme_disk_usage`).

/// Space still held by the previous run's files: what `statvfs` reports used beyond
/// this run's charges, at most disk-maxmemory, and only ever lowered
/// (`refresh_previous_run_bytes`). It includes what the filesystem holds for itself
/// (XFS reserves metadata space per allocation group and reports it used), so only the
/// end of the walk sets it to 0, and 0 means done.
static PREVIOUS_RUN_BYTES: AtomicU64 = AtomicU64::new(0);

/// disk-dir, for `statvfs`. Set once the previous run is charged.
static PREVIOUS_RUN_DISK_DIR: std::sync::OnceLock<std::ffi::CString> = std::sync::OnceLock::new();

/// Directories still being deleted, depth-first, each with its open listing.
static PREVIOUS_RUN_DIRS: Mutex<Vec<(PathBuf, ReadDir)>> = Mutex::new(Vec::new());

/// Space still held by the previous run's files, charged against disk-maxmemory
/// (INFO `disk_previous_run_remaining_bytes`).
pub fn previous_run_remaining_bytes() -> u64 {
    PREVIOUS_RUN_BYTES.load(Ordering::Relaxed)
}

/// Tiered mode: create disk-dir, move what a previous run left in it (including a
/// `previous-run-*` dir a crash left half-deleted) out of the way, charge it, and start
/// the thread that deletes it. Returns the bytes charged. A no-op in Dram mode.
pub fn clean_previous_run(mode: crate::OperatingMode, dir: &str) -> io::Result<u64> {
    if mode != crate::OperatingMode::Tiered {
        return Ok(0);
    }
    if dir.is_empty() {
        let msg = "disk-dir is required in Tiered operating mode";
        return Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
    }
    fs::create_dir_all(dir)?;
    let entries = fs::read_dir(dir)?.collect::<io::Result<Vec<_>>>()?;
    if entries.is_empty() {
        return Ok(0);
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let dest = Path::new(dir).join(format!("previous-run-{}", nanos.as_nanos()));
    fs::create_dir(&dest)?;
    for entry in entries {
        fs::rename(entry.path(), dest.join(entry.file_name()))?;
    }
    let listing = fs::read_dir(&dest)?;
    PREVIOUS_RUN_DIRS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((dest.clone(), listing));
    let path =
        std::ffi::CString::new(dir).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    // Nothing of this run is on disk yet, so every used byte is the previous run's (or
    // the filesystem's own).
    let bytes = previous_run_bytes_on_disk(&path)?.max(1);
    let _ = PREVIOUS_RUN_DISK_DIR.set(path);
    PREVIOUS_RUN_BYTES.store(bytes, Ordering::Relaxed);
    std::thread::Builder::new()
        .name("lo-prev-cleanup".into())
        .spawn(move || delete_previous_run(&dest))?;
    Ok(bytes)
}

/// What `statvfs` reports used beyond this run's charges, at most disk-maxmemory: the
/// previous run never charged more than that.
fn previous_run_bytes_on_disk(dir: &std::ffi::CStr) -> io::Result<u64> {
    let bytes = fs_used_bytes(dir)?.saturating_sub(super::nvme::nvme_disk_usage());
    let max = crate::nvme_maxmemory();
    Ok(if max > 0 { bytes.min(max) } else { bytes })
}

/// Lower the charge to what is still on disk. It never rises: space a writer or an
/// eviction released but the filesystem has not yet freed would otherwise count twice.
pub fn refresh_previous_run_bytes() {
    let Some(dir) = PREVIOUS_RUN_DISK_DIR.get() else {
        return;
    };
    let Ok(on_disk) = previous_run_bytes_on_disk(dir) else {
        return;
    };
    let _ = PREVIOUS_RUN_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |b| {
        (b > 0).then(|| b.min(on_disk.max(1)))
    });
}

/// The cleanup thread, at `SCHED_IDLE`. One is enough: unlink throughput is bound by
/// the filesystem's inode inactivation, not by the number of callers.
fn delete_previous_run(dest: &Path) {
    let param = libc::sched_param { sched_priority: 0 };
    // SAFETY: pid 0 = the calling thread; param is a valid sched_param.
    unsafe { libc::sched_setscheduler(0, libc::SCHED_IDLE, &param) };
    let start = Instant::now();
    while delete_previous_run_file() {}
    // A directory whose last file a writer was still deleting stays behind. Anything that
    // cannot be deleted stays on disk uncharged, so say so.
    let left = fs::remove_dir_all(dest).err();
    let took = start.elapsed();
    let msg = match left.filter(|e| e.kind() != io::ErrorKind::NotFound) {
        None => format!("largeobj: previous run cleanup done in {took:?}"),
        Some(e) => format!(
            "largeobj: previous run cleanup done in {took:?}, but {} could not be fully deleted: {e}",
            dest.display()
        ),
    };
    #[cfg(not(test))]
    valkey_module::logging::log_notice(&msg);

    #[cfg(test)]
    let _ = msg;
}

/// Delete the previous run's next file; false once none is left. Safe from any
/// thread: each file is handed out once, under `PREVIOUS_RUN_DIRS`. The charge is
/// refreshed after each unlink, and set to 0 when the last file is handed out (dropping
/// what the filesystem holds for itself).
pub fn delete_previous_run_file() -> bool {
    if previous_run_remaining_bytes() == 0 {
        return false;
    }
    let path = {
        let mut dirs = PREVIOUS_RUN_DIRS.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            let Some(next) = dirs.last_mut().map(|(_, listing)| listing.next()) else {
                PREVIOUS_RUN_BYTES.store(0, Ordering::Relaxed);
                return false;
            };
            match next {
                Some(Ok(entry)) if entry.file_type().is_ok_and(|t| t.is_dir()) => {
                    if let Ok(listing) = fs::read_dir(entry.path()) {
                        dirs.push((entry.path(), listing));
                    }
                }
                Some(Ok(entry)) => break entry.path(),
                Some(Err(_)) => {}
                None => {
                    let (done, _) = dirs.pop().expect("last_mut was Some");
                    let _ = fs::remove_dir(done);
                }
            }
        }
    };
    let _ = fs::remove_file(&path);
    refresh_previous_run_bytes();
    true
}

fn fs_used_bytes(dir: &std::ffi::CStr) -> io::Result<u64> {
    // SAFETY: statvfs fills a zeroed POD struct from a valid NUL-terminated path.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(dir.as_ptr(), &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((st.f_blocks as u64).saturating_sub(st.f_bfree as u64) * st.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    fn popped(list: &ReclaimList) -> Option<ObjectId> {
        list.pop_by_name(later()).map(|(oid, _)| oid)
    }

    #[test]
    fn test_only_ghost_keys_left_to_scan_need_the_scan() {
        let list = ReclaimList::default();
        let (hinted, renamed, no_hint) = (ObjectId(1), ObjectId(2), ObjectId(3));
        let hint = || Some(KeyHint::new(0, b"k"));
        assert!(list.insert(hinted, hint()));
        assert!(list.insert(renamed, hint()));
        assert!(list.insert(no_hint, None));
        assert!(!list.insert(no_hint, hint()), "already listed");
        assert_eq!(list.num_by_scan(), 1);

        // Deleted by name: listed until its value is freed, but nobody's work.
        assert_eq!(popped(&list), Some(hinted));
        list.set_state(hinted, KeyState::Deleted);
        // Not found by name: the scan's.
        assert_eq!(popped(&list), Some(renamed));
        list.set_state(renamed, KeyState::ByScan);
        assert_eq!(list.num_by_scan(), 2);
        assert!(list.contains(&hinted));

        list.set_state(renamed, KeyState::Deleted);
        list.set_state(no_hint, KeyState::Deleted);
        assert_eq!(list.num_by_scan(), 0);
        assert_eq!(list.len(), 3);
        for oid in [hinted, renamed, no_hint] {
            assert!(list.remove(&oid));
        }
        assert!(list.is_empty());
    }

    #[test]
    fn test_add_with_keeps_an_oid_that_is_already_listed() {
        let list = ReclaimList::default();
        list.insert(ObjectId(1), Some(KeyHint::new(0, b"k")));
        list.add_with(|| [ObjectId(1), ObjectId(2)]);
        assert_eq!(list.len(), 2);
        assert_eq!(list.num_by_scan(), 1, "only oid 2 is the scan's");
        assert_eq!(popped(&list), Some(ObjectId(1)));
        list.remove(&ObjectId(1));
        list.remove(&ObjectId(2));
        assert_eq!(list.num_by_scan(), 0);
    }

    #[test]
    fn test_a_stale_queued_entry_is_skipped_and_cleared() {
        let list = ReclaimList::default();
        for oid in 1..=3 {
            list.insert(ObjectId(oid), Some(KeyHint::new(0, b"k")));
        }
        // Its key was deleted by a command and its value freed before the reaper ran.
        list.remove(&ObjectId(1));
        // A passed deadline pops nothing and keeps the entries.
        assert_eq!(list.pop_by_name(Instant::now()), None);
        assert_eq!(popped(&list), Some(ObjectId(2)));
        list.remove(&ObjectId(2));
        // The last unlisting clears whatever is still queued.
        list.remove(&ObjectId(3));
        assert_eq!(list.lock().by_name.len(), 0);
    }

    #[test]
    fn test_an_entry_awaiting_its_key_name_is_neither_ways_until_set() {
        let list = ReclaimList::default();
        let (named, unnamed, gone) = (ObjectId(1), ObjectId(2), ObjectId(3));
        for oid in [named, unnamed, gone] {
            assert!(list.insert_awaiting_key_name(oid));
        }
        assert!(!list.insert_awaiting_key_name(named), "already listed");
        // Listed, so commands see the keys as missing, but the reaper skips them.
        assert!(list.contains(&named));
        assert_eq!(list.num_by_scan(), 0);
        assert_eq!(popped(&list), None);

        list.set_key_name(named, Some(KeyHint::new(0, b"k")));
        list.set_key_name(unnamed, None);
        // Its value was freed first: the late key name does nothing.
        list.remove(&gone);
        list.set_key_name(gone, Some(KeyHint::new(0, b"k")));
        assert!(!list.contains(&gone));
        assert_eq!(popped(&list), Some(named));
        assert_eq!(popped(&list), None);
        assert_eq!(list.num_by_scan(), 1);
        // Once the scan's, a key name does not take it back.
        list.set_key_name(unnamed, Some(KeyHint::new(0, b"k")));
        assert_eq!(popped(&list), None);
        list.remove(&named);
        list.remove(&unnamed);
        assert_eq!(list.num_by_scan(), 0);
    }

    fn names(p: &Path) -> Vec<String> {
        let entries = fs::read_dir(p).unwrap();
        entries
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    // Every shard and stray file (plus a half-deleted dir from a crash) is moved out of
    // the way and deleted, and the charge drops to 0; a fresh disk-dir charges nothing.
    #[test]
    fn test_clean_previous_run() {
        // A disk-dir with a space and non-ASCII characters, holding file names that are
        // not valid UTF-8 as well.
        let root = std::env::temp_dir().join(format!("lo-prev-run é 漢 {}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        for f in [
            "0000/a.dat",
            "0001/b.dat",
            "stray.dat",
            "previous-run-1/0002/c.dat",
        ] {
            let path = root.join(f);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, [0u8; 4096]).unwrap();
        }
        let raw = |b: &[u8]| {
            <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(b.to_vec())
        };
        fs::write(root.join(raw(b"\xff\xfe.dat")), [0u8; 4096]).unwrap();
        fs::write(root.join("0000").join(raw(b"\xc3.dat")), [0u8; 4096]).unwrap();
        let dir = root.to_str().unwrap();
        assert!(clean_previous_run(crate::OperatingMode::Tiered, dir).unwrap() > 0);
        let deadline = Instant::now() + Duration::from_secs(10);
        while previous_run_remaining_bytes() > 0 || !names(&root).is_empty() {
            assert!(Instant::now() < deadline, "cleanup did not finish");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            clean_previous_run(crate::OperatingMode::Tiered, dir).unwrap(),
            0
        );
        assert!(
            names(&root).is_empty(),
            "nothing created on a fresh disk-dir"
        );
        assert_eq!(
            clean_previous_run(crate::OperatingMode::Dram, "").unwrap(),
            0
        );
        assert!(clean_previous_run(crate::OperatingMode::Tiered, "").is_err());
        fs::remove_dir_all(&root).unwrap();
    }
}

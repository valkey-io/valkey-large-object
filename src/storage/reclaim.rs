//! Reclaim list: oids of ghost keys, whose payload is gone or going (a Tiered
//! eviction unlinks the file after claiming it) while the key still exists. In
//! Tiered mode, in the list means the eviction owns the file's unlink and credit.
//!
//! Commands check it upfront and treat a key in the list as missing; the scaling
//! cron deletes the keys. An oid leaves the list:
//! - Dram: when its object is removed (`DRAMPool::remove_object`, reached from
//!   `lo_free` once the key is deleted), or when the cron deletes its key.
//! - Tiered: when its file is torn down (`ObjectFile::drop`), or when an NVMe
//!   eviction finds its file already gone (a stale directory entry).
//!
//! Tiered: the reaper (the scaling cron) deletes each victim's key one of two ways:
//! - By name: an eviction reads the victim's key hint (its key's name and db when it
//!   was written, from the file header) and queues it on `DELETE_BY_NAME_QUEUE`.
//! - By scan: the fallback for victims with no usable hint (renamed, copied, a key
//!   over the hint cap). The scan finds every key whose oid is in `RECLAIM_LIST`.
//!
//! Lock order: `RECLAIM_LIST` before `DRAMPool.objects` when both are held.

use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use valkey_module::{raw, Context, KeysCursor, ValkeyString};

use super::nvme::KeyHint;
use crate::data_type::{LoValue, ObjectId, LO_TYPE};

/// INFO: evicted keys the reaper deleted by name, and by scan.
pub static EVICTED_KEYS_BY_NAME: AtomicU64 = AtomicU64::new(0);
pub static EVICTED_KEYS_BY_SCAN: AtomicU64 = AtomicU64::new(0);

/// Tiered victims whose keys the reaper deletes by name, oldest first: each victim's
/// oid and key hint. A shortcut into `RECLAIM_LIST`, never the only record of a victim:
/// - Pushed by an eviction, in the same server-lock hold as its claim, so a reaper
///   tick (which empties this queue before it scans) never scans for a queued victim.
/// - Popped by the reaper. An entry whose oid already left `RECLAIM_LIST` (its key was
///   deleted another way) is skipped; one whose key no longer holds the victim is left
///   to the scan.
/// - Cleared when `RECLAIM_LIST` empties: every entry left then is stale.
static DELETE_BY_NAME_QUEUE: LazyLock<Mutex<VecDeque<(ObjectId, KeyHint)>>> =
    LazyLock::new(Mutex::default);

fn delete_by_name_queue() -> MutexGuard<'static, VecDeque<(ObjectId, KeyHint)>> {
    DELETE_BY_NAME_QUEUE
        .lock()
        .expect("DELETE_BY_NAME_QUEUE lock unavailable")
}

/// Queue a claimed victim's key for deletion by name. Call under the claim's server lock.
pub fn queue_delete_by_name(oid: ObjectId, hint: KeyHint) {
    delete_by_name_queue().push_back((oid, hint));
}

/// INFO: victims queued for deletion by name.
pub fn pending_evictions_by_name() -> usize {
    delete_by_name_queue().len()
}

pub static RECLAIM_LIST: LazyLock<ReclaimList> = LazyLock::new(ReclaimList::default);

#[derive(Default)]
pub struct ReclaimList {
    oids: Mutex<HashSet<ObjectId>>,
    /// `oids.len()`. Store only under the `oids` lock, or it can read 0 for a non-empty set.
    len: AtomicUsize,
}

impl ReclaimList {
    /// Private so every change goes through `add_with` / `insert` / `remove`, which update `len`.
    fn lock(&self) -> MutexGuard<'_, HashSet<ObjectId>> {
        self.oids.lock().expect("RECLAIM_LIST lock unavailable")
    }

    /// Adds the oids returned by `f` to the reclaim set, which stays locked while
    /// `f` runs. Call with the server lock held (main thread, or
    /// `ThreadSafeContext::lock()`).
    pub fn add_with<I: IntoIterator<Item = ObjectId>>(&self, f: impl FnOnce() -> I) {
        let mut oids = self.lock();
        oids.extend(f());
        self.len.store(oids.len(), Ordering::Release);
    }

    /// Add `oid` to the reclaim list. False if it was already there.
    pub fn insert(&self, oid: ObjectId) -> bool {
        let mut oids = self.lock();
        let inserted = oids.insert(oid);
        self.len.store(oids.len(), Ordering::Release);
        inserted
    }

    pub fn contains(&self, oid: &ObjectId) -> bool {
        !self.is_empty() && self.lock().contains(oid)
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
        let mut oids = self.lock();
        let removed = oids.remove(oid);
        if removed {
            self.len.store(oids.len(), Ordering::Release);
        }
        removed
    }
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
/// `reaper-scan-budget-us` of main-thread time per tick: first the victims with a
/// key hint, by name, then the rest by scan.
///
/// Cross-slot deletion is safe here: a timer callback runs outside command
/// execution (`server.current_client` is NULL), so key lookups hash each key's
/// own slot instead of reusing a command's cached slot.
pub fn delete_reclaimed_keys(ctx: &Context) {
    if RECLAIM_LIST.is_empty() {
        // Every victim's key is gone, so every queued entry is stale. An eviction
        // queues only under the server lock, which this tick holds.
        delete_by_name_queue().clear();
        return;
    }
    let deadline = Instant::now() + Duration::from_micros(crate::reaper_scan_budget_us());
    delete_keys_by_name(ctx, deadline);
    delete_keys_by_scan(ctx, deadline);
}

/// Delete, by scan, the keys whose oids are on the reclaim list, until `deadline`.
/// Goes db by db, resuming across ticks, until the reclaim list is empty. Dram: an
/// oid leaves the list when its key is deleted, here or via `lo_free`. Tiered: when
/// its `ObjectFile` drops. Either way the scan cannot outlive its keys.
fn delete_keys_by_scan(ctx: &Context, deadline: Instant) {
    RECLAIM_SCAN_CURSOR.with_borrow_mut(|scan| {
        while !RECLAIM_LIST.is_empty() && Instant::now() < deadline {
            // Past the last db: one full pass done. At most one pass per tick
            // so a key moved behind the cursor can't spin us.
            if !select_db(ctx, scan.db) {
                scan.db = 0;
                break;
            }
            let (listed_keys, more) = listed_keys_in_next_bucket(ctx, &scan.cursor);
            for (name, oid) in listed_keys {
                if ctx.open_key_writable(&name).unlink().is_ok() {
                    EVICTED_KEYS_BY_SCAN.fetch_add(1, Ordering::Relaxed);
                    // Same keyspace event core fires for its own evictions.
                    ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
                }
                // Tiered Mode: Do not touch the RECLAIM_LIST. ObjectFile's drop uses
                // presence in the list to determine if the file was claimed by eviction.
                // This is necessary in order to not double-free evicted items.
                // DRAM Mode: Calls to remove the oid from the RECLAIM_LIST happen both
                // here and in lo_free (initiated by the unlink call above). This removal
                // is redundant, but removing it here allows the main thread to exit the
                // reclaim loop instead of waiting for the bio thread or deadline timeout.
                if crate::operating_mode() == crate::OperatingMode::Dram {
                    RECLAIM_LIST.remove(&oid);
                }
            }
            if !more {
                scan.cursor.restart();
                scan.db += 1;
            }
        }
    });
}

/// Delete, by name, the keys of victims in `DELETE_BY_NAME_QUEUE`, until `deadline`.
/// A key is deleted only if it still holds its victim: oids are never reused, so a key
/// that was renamed, moved, swapped to another db, overwritten or replaced since holds
/// another oid, or is gone. Such a victim is left to the scan. Runs on the main thread,
/// like every keyspace change, so nothing moves the key between the check and the delete.
fn delete_keys_by_name(ctx: &Context, deadline: Instant) {
    while Instant::now() < deadline {
        let Some((oid, hint)) = delete_by_name_queue().pop_front() else {
            return;
        };
        // Torn down since the eviction: its key is gone already.
        if !RECLAIM_LIST.contains(&oid) {
            continue;
        }
        if !select_db(ctx, hint.db as i32) {
            continue;
        }
        let name = ValkeyString::create_from_slice(ctx.get_raw(), &hint.key);
        let key = ctx.open_key_writable(&name);
        let holds_victim = matches!(
            key.get_value::<LoValue>(&LO_TYPE),
            Ok(Some(lo)) if lo.object_id == oid
        );
        if holds_victim && key.unlink().is_ok() {
            EVICTED_KEYS_BY_NAME.fetch_add(1, Ordering::Relaxed);
            // Same keyspace event core fires for its own evictions.
            ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
        }
    }
}

/// Scan the next hashtable bucket of the selected db. Returns the bucket's keys
/// whose oids are on the reclaim list, and whether the db has more buckets.
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
    let oids = RECLAIM_LIST.lock();
    // scan() calls this once per key in the bucket.
    let more = cursor.scan(ctx, &|_ctx, name, key| {
        let Some(Ok(Some(lo))) = key.map(|k| k.get_value::<LoValue>(&LO_TYPE)) else {
            return;
        };
        if oids.contains(&lo.object_id) {
            listed.borrow_mut().push((name, lo.object_id));
        }
    });
    (listed.into_inner(), more)
}

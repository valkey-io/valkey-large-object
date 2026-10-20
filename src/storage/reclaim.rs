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
//! Lock order: `RECLAIM_LIST` before `DRAMPool.objects` when both are held.

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use valkey_module::{raw, Context, KeysCursor, ValkeyString};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};

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
/// `reaper-scan-budget-us` of main-thread time per tick. Scans db by db,
/// resuming across ticks, until the reclaim list is empty. Dram: an oid leaves the
/// list when its key is deleted, here or via `lo_free`. Tiered: when its `ObjectFile`
/// drops. Either way the scan cannot outlive its keys.
///
/// Cross-slot deletion is safe here: a timer callback runs outside command
/// execution (`server.current_client` is NULL), so key lookups hash each key's
/// own slot instead of reusing a command's cached slot.
pub fn delete_reclaimed_keys(ctx: &Context) {
    if RECLAIM_LIST.is_empty() {
        return;
    }
    let deadline = Instant::now() + Duration::from_micros(crate::reaper_scan_budget_us());
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
                let _ = ctx.open_key_writable(&name).unlink();
                // Same keyspace event core fires for its own evictions.
                ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
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

/// Scan the next hashtable bucket of the selected db. Returns the bucket's keys
/// whose oids are on the reclaim list, and whether the db has more buckets.
///
/// Only collects, never deletes: core reads each scanned value after the scan
/// callback returns (`moduleCloseKey`), so unlinking a key inside the callback
/// hands BIO an object core still dereferences.
fn listed_keys_in_next_bucket(
    ctx: &Context,
    cursor: &KeysCursor,
) -> (Vec<(ValkeyString, ObjectId)>, bool) {
    let listed = RefCell::new(Vec::new());
    // scan() calls this once per key in the bucket.
    let more = cursor.scan(ctx, &|_ctx, name, key| {
        let Some(Ok(Some(lo))) = key.map(|k| k.get_value::<LoValue>(&LO_TYPE)) else {
            return;
        };
        if RECLAIM_LIST.contains(&lo.object_id) {
            listed.borrow_mut().push((name, lo.object_id));
        }
    });
    (listed.into_inner(), more)
}

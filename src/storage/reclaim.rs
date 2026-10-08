//! Reclaim list: objects whose memory a background job (e.g. Dram-mode shrink)
//! already freed while their keys still exist.
//!
//! Commands check it upfront and treat a listed key as missing; the scaling
//! cron deletes the keys. An oid leaves the list when its object is removed
//! (`DRAMPool::remove_object`, reached from `lo_free` once the key is deleted).
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
    /// `oids.len()`, written under the lock: readers skip it when empty.
    len: AtomicUsize,
}

impl ReclaimList {
    /// Private so every change goes through `add_with` / `remove`, which update `len`.
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

    pub fn contains(&self, oid: &ObjectId) -> bool {
        !self.is_empty() && self.lock().contains(oid)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Take `oid` off the list. Always locks: `remove_object` must wait for a
    /// shrink that is listing this oid. Returns whether it was listed.
    pub fn remove(&self, oid: &ObjectId) -> bool {
        self.remove_or_else(oid, || {})
    }

    /// `remove`, running `otherwise` with the list still locked if `oid` was not listed.
    pub fn remove_or_else(&self, oid: &ObjectId, otherwise: impl FnOnce()) -> bool {
        let mut oids = self.lock();
        let listed = oids.remove(oid);
        if listed {
            self.len.store(oids.len(), Ordering::Release);
        } else {
            otherwise();
        }
        listed
    }

    /// List `oid` unless it is listed already or `refuse`, which runs with the list locked, says
    /// no. Returns whether it was listed. Call with the server lock held, as for `add_with`.
    pub fn add_unless(&self, oid: ObjectId, refuse: impl FnOnce() -> bool) -> bool {
        let mut oids = self.lock();
        if oids.contains(&oid) || refuse() {
            return false;
        }
        oids.insert(oid);
        self.len.store(oids.len(), Ordering::Release);
        true
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
/// `reclaim-scan-budget-us` of main-thread time per tick. Scans db by db,
/// resuming across ticks, until the reclaim list is empty. An oid leaves the
/// list when its key is deleted, here or by any other path (`lo_free` ->
/// `remove_object`), so the scan cannot outlive its keys. Only a delete made
/// here counts as a reclaim; a normal delete (user DEL, overwrite, expiry ->
/// `lo_free`) just takes the oid off the list.
///
/// Cross-slot deletion is safe here: a timer callback runs outside command
/// execution (`server.current_client` is NULL), so key lookups hash each key's
/// own slot instead of reusing a command's cached slot.
pub fn delete_reclaimed_keys(ctx: &Context) {
    if RECLAIM_LIST.is_empty() {
        return;
    }
    let deadline = Instant::now() + Duration::from_micros(crate::reclaim_scan_budget_us());
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
                // Same keyspace event and count core makes for its own evictions.
                ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
                super::get_dram_pool()
                    .reclaims
                    .fetch_add(1, Ordering::Relaxed);
                // lo_free runs later on the BIO thread; clear now so the scan
                // stops once every listed key is gone. Tiered leaves it to `lo_free`, which must
                // find the entry to know eviction owns the file's unlink.
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

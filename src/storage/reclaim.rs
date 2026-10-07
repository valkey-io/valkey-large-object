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

use valkey_module::{raw, Context, KeysCursor};

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

    /// Take `oid` off the list, counting the reclaim. Always locks: `remove_object`
    /// must wait for a shrink that is listing this oid.
    pub fn remove(&self, oid: &ObjectId) {
        let mut oids = self.lock();
        if oids.remove(oid) {
            self.len.store(oids.len(), Ordering::Release);
            super::get_dram_pool()
                .reclaims
                .fetch_add(1, Ordering::Relaxed);
        }
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
/// `remove_object`), so the scan cannot outlive its keys.
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
            // VM_Scan allows deleting the current key from its callback.
            let more = scan.cursor.scan(ctx, &|ctx, name, key| {
                let Some(Ok(Some(lo))) = key.map(|k| k.get_value::<LoValue>(&LO_TYPE)) else {
                    return;
                };
                let oid = lo.object_id;
                if RECLAIM_LIST.contains(&oid) {
                    let _ = ctx.open_key_writable(&name).unlink();
                    // Same keyspace event core fires for its own evictions.
                    ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
                    // lo_free runs later on the BIO thread; clear now so the
                    // scan stops once every listed key is gone.
                    RECLAIM_LIST.remove(&oid);
                }
            });
            if !more {
                scan.cursor.restart();
                scan.db += 1;
            }
        }
    });
}

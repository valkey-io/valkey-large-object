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
use std::sync::atomic::Ordering;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use valkey_module::{raw, Context, KeysCursor};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};

pub static RECLAIM_LIST: LazyLock<ReclaimList> = LazyLock::new(ReclaimList::default);

#[derive(Default)]
pub struct ReclaimList {
    oids: Mutex<HashSet<ObjectId>>,
}

impl ReclaimList {
    /// Hold the list across another lock (shrink lists oids while it holds
    /// `DRAMPool.objects`).
    pub fn lock(&self) -> MutexGuard<'_, HashSet<ObjectId>> {
        self.oids.lock().expect("RECLAIM_LIST lock unavailable")
    }

    pub fn contains(&self, oid: &ObjectId) -> bool {
        self.lock().contains(oid)
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Copy of the listed oids, so the caller can scan without holding the lock.
    pub fn snapshot(&self) -> HashSet<ObjectId> {
        self.lock().clone()
    }

    /// Take `oid` off the list, counting the completed reclaim on the DRAMPool.
    pub fn remove(&self, oid: &ObjectId) {
        if self.lock().remove(oid) {
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

/// Number of keys in the selected db.
fn db_size(ctx: &Context) -> u64 {
    unsafe { raw::RedisModule_DbSize.unwrap()(ctx.get_raw()) }
}

/// Where the reclaim scan resumes on the next tick.
struct ReclaimScan {
    db: i32,
    cursor: KeysCursor,
}

thread_local! {
    // Scaling cron only, which always runs on the main event-loop thread.
    static RECLAIM_SCAN: RefCell<ReclaimScan> = RefCell::new(ReclaimScan { db: 0, cursor: KeysCursor::new() });
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
    // Shrink, the only writer that adds oids, also runs on this thread, so the
    // snapshot misses nothing. Oids other threads remove meanwhile just never match.
    let reclaim = RECLAIM_LIST.snapshot();
    RECLAIM_SCAN.with_borrow_mut(|scan| {
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
                if reclaim.contains(&oid) {
                    let _ = ctx.open_key_writable(&name).unlink();
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

/// Whether the keyspace holds any non-LargeObject keys: total keys across all
/// dbs exceeds the LargeObject count. Dram shrink deletes LargeObject keys to
/// free memory for other data types, so with none of those there is nothing
/// to make room for. `num_objects` decrements when `lo_free` drops the value (async), so
/// right after a delete it can read high and delay a shrink by one tick.
pub fn has_non_lo_keys(ctx: &Context) -> bool {
    let mut total_keys = 0;
    let mut db = 0;
    while select_db(ctx, db) {
        total_keys += db_size(ctx);
        db += 1;
    }
    total_keys > crate::info::LARGE_OBJECT_COUNT.load(Ordering::Relaxed)
}

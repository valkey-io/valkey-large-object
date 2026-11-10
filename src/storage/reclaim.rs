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
//! Tiered: an eviction reads the victim's key hint (its key's name and db when it was
//! written, from the file header) and hands it to the cron, which deletes that key by
//! name if it still holds the victim. Victims without a usable hint are found by scan.
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

/// INFO: victims' keys the cron deleted by their key hint, and by scan.
pub static REAPED_BY_NAME: AtomicU64 = AtomicU64::new(0);
pub static REAPED_BY_SCAN: AtomicU64 = AtomicU64::new(0);

/// Tiered victims between their claim and the cron.
#[derive(Default)]
struct KeyHints {
    /// Every victim whose key hint is being read or waits in `ready`. The scan skips
    /// these, so it never races the delete by name. An oid leaves when the cron takes
    /// its hint, or when the eviction finds none and leaves the key to the scan.
    hinted: HashSet<ObjectId>,
    /// Hints read, oldest first, for the cron.
    ready: VecDeque<(ObjectId, KeyHint)>,
}

static KEY_HINTS: LazyLock<Mutex<KeyHints>> = LazyLock::new(Mutex::default);

fn key_hints() -> MutexGuard<'static, KeyHints> {
    KEY_HINTS.lock().expect("KEY_HINTS lock unavailable")
}

/// Tiered: keys only the scan can delete (a victim with no key hint or a stale one, or
/// a placeholder from a load). The
/// Tiered scan runs only while this is above 0, so deleting by name costs no scan.
/// It can overcount (such a key deleted by a client instead), which only costs scan
/// time, never a missed key; it resets once the reclaim list empties.
static SCAN_OWED: AtomicUsize = AtomicUsize::new(0);

/// A claimed victim's key hint is on its way: hide the victim from the scan.
pub fn expect_key_hint(oid: ObjectId) {
    key_hints().hinted.insert(oid);
}

/// The victim's key hint, or `None` to leave its key to the scan.
pub fn provide_key_hint(oid: ObjectId, hint: Option<KeyHint>) {
    let mut hints = key_hints();
    match hint {
        Some(hint) => hints.ready.push_back((oid, hint)),
        None => {
            hints.hinted.remove(&oid);
            SCAN_OWED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Put `oid` on the reclaim list with no key hint, so the scan deletes its key (a blob
/// placeholder from a load).
pub fn reclaim_by_scan(oid: ObjectId) {
    RECLAIM_LIST.insert(oid);
    SCAN_OWED.fetch_add(1, Ordering::Relaxed);
}

/// The claimed victim was torn down before its eviction, so its key is gone.
pub fn forget_key_hint(oid: ObjectId) {
    key_hints().hinted.remove(&oid);
}

/// The oldest hint read, taking its victim off the hinted set.
fn take_key_hint() -> Option<(ObjectId, KeyHint)> {
    let mut hints = key_hints();
    let (oid, hint) = hints.ready.pop_front()?;
    hints.hinted.remove(&oid);
    Some((oid, hint))
}

/// INFO: victims whose key hint waits for the cron, or is still being read.
pub fn pending_key_hints() -> usize {
    key_hints().hinted.len()
}

// ─── db tokens ───────────────────────────────────────────────────────────────

/// A key hint names its db by token, not number: SWAPDB swaps two dbs' tokens, so a
/// hint written before a SWAPDB still names the db its key moved to. Tokens start
/// equal to db numbers; only SWAPDB moves them. Main thread only (commands, COPY,
/// SWAPDB, the cron).
#[derive(Default)]
struct DbTokens {
    /// `token_of[db]`; a db past the end has token == db.
    token_of: Vec<u32>,
    /// `db_of[token]`, the inverse.
    db_of: Vec<i32>,
}

impl DbTokens {
    fn token_of(&self, db: i32) -> u32 {
        self.token_of.get(db as usize).copied().unwrap_or(db as u32)
    }

    fn db_of(&self, token: u32) -> i32 {
        self.db_of
            .get(token as usize)
            .copied()
            .unwrap_or(token as i32)
    }

    fn swap(&mut self, a: i32, b: i32) {
        let len = a.max(b) as usize + 1;
        while self.token_of.len() < len {
            let n = self.token_of.len();
            self.token_of.push(n as u32);
            self.db_of.push(n as i32);
        }
        self.token_of.swap(a as usize, b as usize);
        self.db_of[self.token_of[a as usize] as usize] = a;
        self.db_of[self.token_of[b as usize] as usize] = b;
    }
}

thread_local! {
    static DB_TOKENS: RefCell<DbTokens> = RefCell::default();
}

/// The token a key hint records for `db`. Main thread only.
pub fn db_token(db: i32) -> u32 {
    DB_TOKENS.with_borrow(|t| t.token_of(db))
}

/// `SWAPDB`'s server event payload (`RedisModuleSwapDbInfoV1`); crosses FFI.
#[repr(C)]
struct SwapDbInfo {
    _version: u64,
    dbnum_first: i32,
    dbnum_second: i32,
}

/// Subscribe to SWAPDB, so the db tokens follow every swap. The crate's SWAPDB hook
/// drops the payload, so this subscribes directly.
pub fn subscribe_swapdb(ctx: &Context) -> Result<(), String> {
    unsafe extern "C" fn on_swapdb(
        _ctx: *mut raw::RedisModuleCtx,
        _eid: raw::RedisModuleEvent,
        _subevent: u64,
        data: *mut std::ffi::c_void,
    ) {
        // SAFETY: core passes a RedisModuleSwapDbInfo for this event.
        let info = unsafe { &*(data as *const SwapDbInfo) };
        DB_TOKENS.with_borrow_mut(|t| t.swap(info.dbnum_first, info.dbnum_second));
    }
    let event = raw::RedisModuleEvent {
        id: raw::REDISMODULE_EVENT_SWAPDB,
        dataver: 1,
    };
    // SAFETY: `ctx` is the live load context.
    let rc = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(ctx.get_raw(), event, Some(on_swapdb))
    };
    if rc == raw::REDISMODULE_OK as i32 {
        Ok(())
    } else {
        Err("failed to subscribe to SWAPDB".to_string())
    }
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
/// key hint, by name, then the rest by scan. The scan goes db by db, resuming across
/// ticks, until the reclaim list is empty. Dram: an oid leaves the list when its key
/// is deleted, here or via `lo_free`. Tiered: when its `ObjectFile` drops. Either way
/// the scan cannot outlive its keys.
///
/// Cross-slot deletion is safe here: a timer callback runs outside command
/// execution (`server.current_client` is NULL), so key lookups hash each key's
/// own slot instead of reusing a command's cached slot.
pub fn delete_reclaimed_keys(ctx: &Context) {
    if RECLAIM_LIST.is_empty() {
        // No victims left, so none is owed to the scan.
        SCAN_OWED.store(0, Ordering::Relaxed);
        return;
    }
    let deadline = Instant::now() + Duration::from_micros(crate::reaper_scan_budget_us());
    delete_hinted_keys(ctx, deadline);
    RECLAIM_SCAN_CURSOR.with_borrow_mut(|scan| {
        // Dram has no key hints, so every victim is the scan's.
        while !RECLAIM_LIST.is_empty()
            && (crate::operating_mode() == crate::OperatingMode::Dram
                || SCAN_OWED.load(Ordering::Relaxed) > 0)
            && Instant::now() < deadline
        {
            // Past the last db: one full pass done. At most one pass per tick
            // so a key moved behind the cursor can't spin us.
            if !select_db(ctx, scan.db) {
                scan.db = 0;
                break;
            }
            let (mut listed_keys, more) = listed_keys_in_next_bucket(ctx, &scan.cursor);
            // Victims with a key hint coming, or waiting, are deleted by name.
            {
                let hints = key_hints();
                listed_keys.retain(|(_, oid)| !hints.hinted.contains(oid));
            }
            for (name, oid) in listed_keys {
                let _ = ctx.open_key_writable(&name).unlink();
                REAPED_BY_SCAN.fetch_add(1, Ordering::Relaxed);
                let _ = SCAN_OWED
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
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

/// Delete, by name, the keys of victims whose key hint has arrived, until `deadline`.
/// A key is deleted only if it still holds its victim: oids are never reused, so a
/// key that was renamed, moved, overwritten or replaced since holds another oid, or
/// is gone. Such a victim is left to the scan. Runs on the main thread, like every
/// keyspace change, so nothing moves the key between the check and the delete.
fn delete_hinted_keys(ctx: &Context, deadline: Instant) {
    while Instant::now() < deadline {
        let Some((oid, hint)) = take_key_hint() else {
            return;
        };
        // Torn down since the eviction: its key is gone already.
        if !RECLAIM_LIST.contains(&oid) {
            continue;
        }
        let db = DB_TOKENS.with_borrow(|t| t.db_of(hint.db_token));
        let name = ValkeyString::create_from_slice(ctx.get_raw(), &hint.key);
        let deleted = select_db(ctx, db) && {
            let key = ctx.open_key_writable(&name);
            let holds_victim = matches!(
                key.get_value::<LoValue>(&LO_TYPE),
                Ok(Some(lo)) if lo.object_id == oid
            );
            holds_victim && key.unlink().is_ok()
        };
        if deleted {
            REAPED_BY_NAME.fetch_add(1, Ordering::Relaxed);
            // Same keyspace event core fires for its own evictions.
            ctx.notify_keyspace_event(raw::NotifyEvent::EVICTED, "evicted", &name);
        } else {
            SCAN_OWED.fetch_add(1, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

    // Tokens follow the data through any sequence of swaps: the token a db had when a
    // key was written still maps to wherever that data is now.
    #[test]
    fn db_tokens_follow_every_swapdb() {
        let mut tokens = DbTokens::default();
        assert_eq!((tokens.token_of(5), tokens.db_of(5)), (5, 5));
        let written_in_0 = tokens.token_of(0);
        let written_in_3 = tokens.token_of(3);
        tokens.swap(0, 3);
        assert_eq!(tokens.db_of(written_in_0), 3);
        assert_eq!(tokens.db_of(written_in_3), 0);
        tokens.swap(3, 7);
        assert_eq!(tokens.db_of(written_in_0), 7);
        assert_eq!(tokens.db_of(written_in_3), 0);
        tokens.swap(0, 7);
        assert_eq!(tokens.db_of(written_in_0), 0);
        assert_eq!(tokens.db_of(written_in_3), 7);
        for db in 0..10 {
            assert_eq!(tokens.db_of(tokens.token_of(db)), db, "a permutation");
        }
    }
}

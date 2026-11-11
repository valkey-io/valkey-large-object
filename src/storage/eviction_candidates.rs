//! The eviction candidates: which objects exist, which of them have a TTL, and in what order
//! eviction takes them. One global set serves Dram and Tiered mode alike. In Dram mode
//! it holds the objects in the DRAMPool; in Tiered mode, the objects with an NVMe file.
//! Eviction picks its victims from it and never scans a directory or a segment.
//!
//! It is the one place that knows an eviction order, and it has one `EvictionCandidates`
//! variant per order. A new order is a new variant, and each function below gains one arm:
//! - `Lru`: every object and the objects with a TTL, each in least-recently-used order.
//!   Every `maxmemory-policy` uses it for now.
//!
//! An evicted object leaves the candidates at once (`remove`), so no later eviction picks it,
//! even while its key lingers until the reclaim cron deletes it. The teardown that
//! follows calls `remove` again, which does nothing.
//!
//! The `maxmemory-policy` also lives here: the module API has no getter for it, so it is read
//! at load and again when a `CONFIG SET` changes it.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};

use hashlink::{LinkedHashMap, LinkedHashSet};
use valkey_module::{Context, NotifyEvent};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};

/// Exact least-recently-used order: the least recently used oid is first in each.
/// `all` maps every oid to its `disk_bytes` (`insert`); `ttl` holds the oids of `all`
/// whose key has a TTL.
#[derive(Default)]
struct Lru {
    all: LinkedHashMap<ObjectId, u64>,
    ttl: LinkedHashSet<ObjectId>,
}

impl Lru {
    fn touch(&mut self, oid: ObjectId) {
        if self.all.to_back(&oid).is_some() {
            self.ttl.to_back(&oid);
        }
    }

    /// A TTL change is a use, as in core (EXPIRE and PERSIST touch the key).
    fn set_ttl(&mut self, oid: ObjectId, has_ttl: bool) {
        if self.all.to_back(&oid).is_none() {
            // Evicted: the key lingers until the reclaim cron deletes it.
            return;
        }
        if has_ttl {
            self.ttl.insert(oid);
        } else {
            self.ttl.remove(&oid);
        }
    }

    fn remove(&mut self, oid: ObjectId) -> Option<u64> {
        self.ttl.remove(&oid);
        self.all.remove(&oid)
    }

    fn oldest(
        &self,
        volatile: bool,
        n: usize,
        mut take: impl FnMut(ObjectId) -> bool,
    ) -> Vec<ObjectId> {
        let mut first = |oids: &mut dyn Iterator<Item = &ObjectId>| -> Vec<ObjectId> {
            oids.copied().filter(|&oid| take(oid)).take(n).collect()
        };
        if volatile {
            first(&mut self.ttl.iter())
        } else {
            first(&mut self.all.keys())
        }
    }

    fn take(
        &mut self,
        volatile: bool,
        need: u64,
        max: usize,
        mut accept: impl FnMut(ObjectId) -> bool,
    ) -> Vec<(ObjectId, u64)> {
        let mut taken = Vec::new();
        let mut covered = 0;
        let oids: Box<dyn Iterator<Item = &ObjectId>> = if volatile {
            Box::new(self.ttl.iter())
        } else {
            Box::new(self.all.keys())
        };
        for &oid in oids {
            if covered >= need || taken.len() == max {
                break;
            }
            if accept(oid) {
                let disk_bytes = self.all[&oid];
                covered += disk_bytes;
                taken.push((oid, disk_bytes));
            }
        }
        for &(oid, _) in &taken {
            self.remove(oid);
        }
        taken
    }
}

enum EvictionCandidates {
    Lru(Lru),
}

static CANDIDATES: LazyLock<Mutex<EvictionCandidates>> =
    LazyLock::new(|| Mutex::new(EvictionCandidates::Lru(Lru::default())));

fn candidates() -> MutexGuard<'static, EvictionCandidates> {
    CANDIDATES
        .lock()
        .expect("eviction candidates lock unavailable")
}

/// A new object exists: Dram, its buffers are in the DRAMPool; Tiered, its file is on NVMe.
/// `disk_bytes`: Tiered, the disk usage charged for its file, which its eviction credits
/// back; Dram, 0 (eviction frees segments, not disk).
pub fn insert(oid: ObjectId, disk_bytes: u64) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => {
            lru.all.insert(oid, disk_bytes);
        }
    }
}

/// Record a GET.
pub fn touch(oid: ObjectId) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.touch(oid),
    }
}

/// The object is evicted or gone: no eviction may pick it again. Returns its `disk_bytes`,
/// or `None` if it was not a candidate.
pub fn remove(oid: ObjectId) -> Option<u64> {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.remove(oid),
    }
}

/// Add `bytes` to a candidate's `disk_bytes`, keeping its place in the eviction order.
/// False if `oid` is not a candidate.
pub fn add_disk_bytes(oid: ObjectId, bytes: u64) -> bool {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.all.get_mut(&oid).map(|b| *b += bytes).is_some(),
    }
}

/// Up to `n` eviction candidates `take` accepts, in eviction order, among the objects
/// the policy allows: under `volatile-*`, those whose key has a TTL. The candidates lock
/// is held while `take` runs, so it must not touch the candidates.
pub fn pick(n: usize, take: impl FnMut(ObjectId) -> bool) -> Vec<ObjectId> {
    match &*candidates() {
        EvictionCandidates::Lru(lru) => {
            lru.oldest(POLICY.load(Ordering::Relaxed) == VOLATILE, n, take)
        }
    }
}

/// Take candidates `accept` accepts, in eviction order among the objects the policy
/// allows, until their `disk_bytes` cover `need` or `max` are taken. They leave the
/// candidates in the same lock hold, so concurrent evictions take disjoint victims.
/// Returns each oid with its `disk_bytes`. The candidates lock is held while `accept`
/// runs, so it must not touch the candidates.
pub fn take(need: u64, max: usize, accept: impl FnMut(ObjectId) -> bool) -> Vec<(ObjectId, u64)> {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.take(
            POLICY.load(Ordering::Relaxed) == VOLATILE,
            need,
            max,
            accept,
        ),
    }
}

/// A COPY's object has a TTL exactly when its source has one, so it inherits the source's.
/// Call it once the copy is a candidate (`insert`).
pub fn inherit_ttl(from: ObjectId, to: ObjectId) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => {
            if lru.ttl.contains(&from) {
                lru.set_ttl(to, true);
            }
        }
    }
}

/// Keyspace events that change a key's TTL without changing its oid: `expire` (EXPIRE,
/// PEXPIRE, EXPIREAT, PEXPIREAT, GETEX) fires only when a TTL was set, and `persist` only when
/// one was removed. An overwrite (BLOB.TCP_SET / BLOB.RDMA_SET) needs none: it mints a new oid,
/// and Valkey drops the key's TTL. RENAME and MOVE need none: the object and its TTL move
/// together. COPY is handled by `inherit_ttl`.
pub fn on_keyspace_event(ctx: &Context, _: NotifyEvent, event: &str, key: &[u8]) {
    let has_ttl = match event {
        "expire" => true,
        "persist" => false,
        _ => return,
    };
    let key = valkey_module::ValkeyString::create_from_slice(ctx.get_raw(), key);
    let oid = match ctx.open_key(&key).get_value::<LoValue>(&LO_TYPE) {
        Ok(Some(lo)) => lo.object_id,
        _ => return,
    };
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.set_ttl(oid, has_ttl),
    }
}

// ─── maxmemory-policy ────────────────────────────────────────────────────────

/// What `maxmemory-policy` lets eviction take: any key, only keys with a TTL, or nothing.
const ALLKEYS: u8 = 0;
const VOLATILE: u8 = 1;
const NOEVICTION: u8 = 2;
static POLICY: AtomicU8 = AtomicU8::new(ALLKEYS);

pub fn noeviction() -> bool {
    POLICY.load(Ordering::Relaxed) == NOEVICTION
}

/// Read `maxmemory-policy` from the server and cache it, warning if it is served in another
/// order than its own. Call at load and after a `CONFIG SET` of the policy (`configs::on_server_config_changed`).
pub fn refresh_policy(ctx: &Context) {
    let info = ctx.server_info("memory");
    // `field_c` borrows from `info`. `field` leaks its string: the crate retains the string
    // the server just created, then releases it once.
    let name = info.field_c("maxmemory_policy").unwrap_or_default();
    let volatile = name.starts_with("volatile-");
    if name.ends_with("-lfu") || name.ends_with("-random") || name == "volatile-ttl" {
        valkey_module::logging::log_warning(format!(
            "largeobj: maxmemory-policy {name} evicts in {}-lru order: module eviction has \
             no LFU, random or TTL-ordered eviction",
            if volatile { "volatile" } else { "allkeys" }
        ));
    }
    let policy = if name == "noeviction" {
        NOEVICTION
    } else if volatile {
        VOLATILE
    } else {
        ALLKEYS
    };
    POLICY.store(policy, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lru(oids: std::ops::Range<u64>) -> Lru {
        let mut lru = Lru::default();
        for oid in oids {
            lru.all.insert(ObjectId(oid), 0);
        }
        lru
    }

    fn oldest(lru: &Lru, volatile: bool) -> Vec<u64> {
        lru.oldest(volatile, usize::MAX, |_| true)
            .iter()
            .map(|oid| oid.0)
            .collect()
    }

    #[test]
    fn all_goes_least_recently_used_first() {
        let mut lru = lru(0..4);
        lru.touch(ObjectId(0));
        lru.remove(ObjectId(2));
        lru.remove(ObjectId(2)); // already gone: nothing happens
        lru.touch(ObjectId(2));
        assert_eq!(oldest(&lru, false), [1, 3, 0]);
        // `take` refuses 1, and `n` stops the walk.
        assert_eq!(lru.oldest(false, 1, |oid| oid.0 != 1), [ObjectId(3)]);
    }

    #[test]
    fn volatile_goes_through_keys_with_a_ttl_in_the_same_order() {
        let mut lru = lru(0..4);
        lru.set_ttl(ObjectId(1), true);
        lru.set_ttl(ObjectId(3), true);
        lru.set_ttl(ObjectId(9), true); // not listed: ignored
        lru.touch(ObjectId(1));
        assert_eq!(oldest(&lru, true), [3, 1]);
        // A TTL change is a use of the key, so it also moves in the full order.
        assert_eq!(oldest(&lru, false), [0, 2, 3, 1]);
        lru.set_ttl(ObjectId(3), false);
        assert_eq!(oldest(&lru, true), [1]);
        lru.remove(ObjectId(1));
        assert!(oldest(&lru, true).is_empty());
    }

    #[test]
    fn take_removes_the_oldest_accepted_until_need_or_max() {
        let mut lru = Lru::default();
        for oid in 0..6 {
            lru.all.insert(ObjectId(oid), 10);
        }
        lru.set_ttl(ObjectId(4), true);
        // 0 is refused (in use); 1 and 2 cover 15 bytes.
        assert_eq!(
            lru.take(false, 15, usize::MAX, |oid| oid.0 != 0),
            [(ObjectId(1), 10), (ObjectId(2), 10)]
        );
        assert_eq!(oldest(&lru, false), [0, 3, 5, 4], "taken ones are gone");
        // `max` stops it before `need` is covered.
        assert_eq!(lru.take(false, 100, 1, |_| true), [(ObjectId(0), 10)]);
        // Volatile takes only keys with a TTL, and a taken one leaves both orders.
        assert_eq!(
            lru.take(true, 100, usize::MAX, |_| true),
            [(ObjectId(4), 10)]
        );
        assert_eq!(oldest(&lru, false), [3, 5]);
        assert!(
            lru.take(false, 0, usize::MAX, |_| true).is_empty(),
            "nothing needed"
        );
    }
}

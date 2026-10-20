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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};

use hashlink::LinkedHashSet;
use valkey_module::{Context, NotifyEvent, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};

/// Exact least-recently-used order: the least recently used oid is first in each set.
/// `ttl` holds the oids of `all` whose key has a TTL.
#[derive(Default)]
struct Lru {
    all: LinkedHashSet<ObjectId>,
    ttl: LinkedHashSet<ObjectId>,
}

impl Lru {
    fn touch(&mut self, oid: ObjectId) {
        if self.all.to_back(&oid) {
            self.ttl.to_back(&oid);
        }
    }

    /// A TTL change is a use, as in core (EXPIRE and PERSIST touch the key).
    fn set_ttl(&mut self, oid: ObjectId, has_ttl: bool) {
        if !self.all.to_back(&oid) {
            return;
        }
        if has_ttl {
            self.ttl.insert(oid);
        } else {
            self.ttl.remove(&oid);
        }
    }

    fn remove(&mut self, oid: ObjectId) {
        self.all.remove(&oid);
        self.ttl.remove(&oid);
    }

    fn oldest(
        &self,
        volatile: bool,
        n: usize,
        mut take: impl FnMut(ObjectId) -> bool,
    ) -> Vec<ObjectId> {
        let list = if volatile { &self.ttl } else { &self.all };
        list.iter()
            .copied()
            .filter(|&oid| take(oid))
            .take(n)
            .collect()
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
pub fn insert(oid: ObjectId) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => {
            lru.all.insert(oid);
        }
    }
}

/// Record a GET.
pub fn touch(oid: ObjectId) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.touch(oid),
    }
}

/// The object is evicted or gone: no eviction may pick it again.
pub fn remove(oid: ObjectId) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.remove(oid),
    }
}

/// Up to `n` eviction candidates `take` accepts, in eviction order, among the objects
/// the policy allows: under `volatile-*`, those whose key has a TTL. The candidates lock
/// is held while `take` runs, so it must not touch the candidates.
pub fn pick(n: usize, take: impl FnMut(ObjectId) -> bool) -> Vec<ObjectId> {
    match &*candidates() {
        EvictionCandidates::Lru(lru) => lru.oldest(VOLATILE.load(Ordering::Relaxed), n, take),
    }
}

/// Keyspace events that can change a key's TTL without changing its oid. An overwrite
/// (BLOB.TCP_SET / BLOB.RDMA_SET) needs none: it mints a new oid, and Valkey drops the key's TTL.
/// RENAME and MOVE need none either: the object and its TTL move together. `expire` covers
/// EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT.
pub fn on_keyspace_event(ctx: &Context, _: NotifyEvent, event: &str, key: &[u8]) {
    if !matches!(event, "expire" | "persist" | "copy_to") {
        return;
    }
    let key = ctx.create_string(key);
    let oid = match ctx.open_key(&key).get_value::<LoValue>(&LO_TYPE) {
        Ok(Some(lo)) => lo.object_id,
        _ => return,
    };
    let has_ttl = matches!(ctx.call("PTTL", &[&key]), Ok(ValkeyValue::Integer(ms)) if ms >= 0);
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.set_ttl(oid, has_ttl),
    }
}

// ─── maxmemory-policy ────────────────────────────────────────────────────────

/// `volatile-*`: only keys with a TTL may be evicted.
static VOLATILE: AtomicBool = AtomicBool::new(false);
/// `noeviction`: nothing may be evicted.
static NOEVICTION: AtomicBool = AtomicBool::new(false);

pub fn noeviction() -> bool {
    NOEVICTION.load(Ordering::Relaxed)
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
    VOLATILE.store(volatile, Ordering::Relaxed);
    NOEVICTION.store(name == "noeviction", Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lru(oids: std::ops::Range<u64>) -> Lru {
        let mut lru = Lru::default();
        for oid in oids {
            lru.all.insert(ObjectId(oid));
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
}

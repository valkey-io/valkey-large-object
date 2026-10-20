//! The eviction candidates: which objects exist, which of them have a TTL, and in what order
//! eviction takes them. One global set serves Dram and Tiered mode alike. In Dram mode
//! it holds the objects in the DRAMPool; in Tiered mode, the objects with an NVMe file.
//! Eviction picks its victims from it and never scans a directory or a segment.
//!
//! It is the one place that knows an eviction order, and it has one `EvictionCandidates`
//! variant per order. A new order is a new variant, and each function below gains one arm:
//! - `Lru`: every object and the objects with a TTL, each in least-recently-used order.
//!   Every `maxmemory-policy` but `*-random` uses it.
//! - `Random`: two roaring bitmaps of oids, `all` and `ttl` (a subset of `all`). The oid
//!   is a dense integer, so it is the bit position, and `select(k)` returns the k-th set
//!   bit: a uniformly random live oid. A GET records nothing.
//!
//! A change of `maxmemory-policy` to the other order drops the candidates and builds the other
//! one from the same objects (`use_random`). Which objects exist and which have a TTL are
//! kept; the order is not, so a new LRU order starts from creation order.
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
use rand::RngExt;
use roaring::RoaringTreemap;
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

/// The bitmaps behind `EvictionCandidates::Random`.
#[derive(Default)]
struct Bitmaps {
    all: RoaringTreemap,
    /// The objects in `all` whose key has a TTL.
    ttl: RoaringTreemap,
}

enum EvictionCandidates {
    Lru(Lru),
    Random(Bitmaps),
}

static CANDIDATES: LazyLock<Mutex<EvictionCandidates>> =
    LazyLock::new(|| Mutex::new(EvictionCandidates::Lru(Lru::default())));

/// Whether the candidates order by use, so a GET must tell it. Read without the lock, so a
/// `*-random` GET takes none.
static ORDERS_BY_USE: AtomicBool = AtomicBool::new(true);

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
        EvictionCandidates::Random(maps) => {
            maps.all.insert(oid.0);
        }
    }
}

/// Record a GET.
pub fn touch(oid: ObjectId) {
    if !ORDERS_BY_USE.load(Ordering::Relaxed) {
        return;
    }
    if let EvictionCandidates::Lru(lru) = &mut *candidates() {
        lru.touch(oid);
    }
}

/// The object is evicted or gone: no eviction may pick it again.
pub fn remove(oid: ObjectId) {
    match &mut *candidates() {
        EvictionCandidates::Lru(lru) => lru.remove(oid),
        EvictionCandidates::Random(maps) => {
            maps.all.remove(oid.0);
            maps.ttl.remove(oid.0);
        }
    }
}

/// Up to `n` eviction candidates `take` accepts, in eviction order, among the objects
/// the policy allows: under `volatile-*`, those whose key has a TTL. The candidates lock
/// is held while `take` runs, so it must not touch the candidates.
pub fn pick(n: usize, take: impl FnMut(ObjectId) -> bool) -> Vec<ObjectId> {
    match &*candidates() {
        EvictionCandidates::Lru(lru) => lru.oldest(VOLATILE.load(Ordering::Relaxed), n, take),
        EvictionCandidates::Random(maps) => maps.random(VOLATILE.load(Ordering::Relaxed), n, take),
    }
}

/// Make the candidates random if `random`, else LRU. A no-op when it already is. Called with
/// every change of `maxmemory-policy`.
fn use_random(random: bool) {
    let mut candidates = candidates();
    let current = std::mem::replace(&mut *candidates, EvictionCandidates::Lru(Lru::default()));
    *candidates = current.into_order(random);
    ORDERS_BY_USE.store(!random, Ordering::Relaxed);
}

impl EvictionCandidates {
    /// The same objects and TTLs in the random order if `random`, else in the LRU order.
    fn into_order(self, random: bool) -> Self {
        match (self, random) {
            (Self::Lru(lru), true) => Self::Random(Bitmaps {
                all: lru.all.iter().map(|oid| oid.0).collect(),
                ttl: lru.ttl.iter().map(|oid| oid.0).collect(),
            }),
            // Oids grow with creation, so ascending is oldest first.
            (Self::Random(maps), false) => {
                let mut lru = Lru::default();
                for oid in maps.all.iter() {
                    lru.all.insert(ObjectId(oid));
                    if maps.ttl.contains(oid) {
                        lru.ttl.insert(ObjectId(oid));
                    }
                }
                Self::Lru(lru)
            }
            (current, _) => current,
        }
    }
}

impl Bitmaps {
    /// Up to `n` distinct oids `take` accepts, uniformly at random, from `ttl` if
    /// `volatile`, else from `all`. A refused oid is skipped, and the search gives up
    /// after a bounded number of draws.
    fn random(
        &self,
        volatile: bool,
        n: usize,
        mut take: impl FnMut(ObjectId) -> bool,
    ) -> Vec<ObjectId> {
        let bitmap = if volatile { &self.ttl } else { &self.all };
        let draws = n.saturating_mul(4).max(16);
        let mut rng = rand::rng();
        let mut taken: Vec<u64> = Vec::new();
        if bitmap.len() <= draws as u64 {
            // Few enough to see every one.
            taken.extend(bitmap.iter().filter(|&oid| take(ObjectId(oid))));
            for i in (1..taken.len()).rev() {
                taken.swap(i, rng.random_range(0..=i));
            }
            taken.truncate(n);
        } else {
            for _ in 0..draws {
                if taken.len() == n {
                    break;
                }
                let Some(oid) = bitmap.select(rng.random_range(0..bitmap.len())) else {
                    continue;
                };
                if !taken.contains(&oid) && take(ObjectId(oid)) {
                    taken.push(oid);
                }
            }
        }
        taken.into_iter().map(ObjectId).collect()
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
        EvictionCandidates::Random(maps) if maps.all.contains(oid.0) => {
            if has_ttl {
                maps.ttl.insert(oid.0);
            } else {
                maps.ttl.remove(oid.0);
            }
        }
        EvictionCandidates::Random(_) => {}
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
    if name.ends_with("-lfu") || name == "volatile-ttl" {
        valkey_module::logging::log_warning(format!(
            "largeobj: maxmemory-policy {name} evicts in {}-lru order: module eviction has \
             no LFU or TTL-ordered eviction",
            if volatile { "volatile" } else { "allkeys" }
        ));
    }
    // The candidates first, so an eviction that sees the new policy finds its order.
    use_random(name.ends_with("-random"));
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

    fn bitmaps(oids: &[(u64, bool)]) -> Bitmaps {
        let mut maps = Bitmaps::default();
        for &(oid, ttl) in oids {
            maps.all.insert(oid);
            if ttl {
                maps.ttl.insert(oid);
            }
        }
        maps
    }

    fn random(
        maps: &Bitmaps,
        volatile: bool,
        n: usize,
        take: impl FnMut(ObjectId) -> bool,
    ) -> Vec<u64> {
        let mut picked: Vec<u64> = maps
            .random(volatile, n, take)
            .iter()
            .map(|oid| oid.0)
            .collect();
        picked.sort_unstable();
        picked
    }

    #[test]
    fn random_picks_distinct_allowed_oids() {
        // Many oids: drawn at random. Few: all seen.
        for count in [200, 6] {
            let maps = bitmaps(
                &(1..=count)
                    .map(|oid| (oid, oid % 2 == 0))
                    .collect::<Vec<_>>(),
            );
            for volatile in [false, true] {
                let picked = random(&maps, volatile, 2, |oid| oid.0 != 1);
                assert_eq!(picked.len(), 2, "{count} oids, volatile {volatile}");
                assert!(
                    picked.windows(2).all(|pair| pair[0] != pair[1]),
                    "{picked:?}"
                );
                assert!(
                    picked
                        .iter()
                        .all(|oid| *oid != 1 && (!volatile || oid % 2 == 0)),
                    "{picked:?}"
                );
            }
        }
        assert_eq!(
            random(
                &bitmaps(&[(1, true), (2, false), (3, true)]),
                true,
                usize::MAX,
                |_| true
            ),
            [1, 3]
        );
    }

    #[test]
    fn switching_the_order_keeps_the_objects_and_their_ttls() {
        let lru = Lru {
            all: [5, 2, 9, 7].map(ObjectId).into_iter().collect(),
            ttl: [2, 9].map(ObjectId).into_iter().collect(),
        };
        let EvictionCandidates::Random(maps) = EvictionCandidates::Lru(lru).into_order(true) else {
            panic!("not random");
        };
        assert_eq!(random(&maps, false, usize::MAX, |_| true), [2, 5, 7, 9]);
        assert_eq!(random(&maps, true, usize::MAX, |_| true), [2, 9]);

        // Back to LRU, it starts from creation order. The same order again changes nothing.
        let candidates = EvictionCandidates::Random(maps).into_order(false);
        let EvictionCandidates::Lru(lru) = candidates.into_order(false) else {
            panic!("not LRU");
        };
        assert_eq!(oldest(&lru, false), [2, 5, 7, 9]);
        assert_eq!(oldest(&lru, true), [2, 9]);
    }
}

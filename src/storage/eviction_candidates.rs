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

use valkey_module::{Context, NotifyEvent};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};

/// A least-recently-used order: a list of oids, least recently used first, linked through a
/// map from each oid to its neighbours and a value. The map is a griddle map, which grows
/// incrementally: each insert moves a few entries into the larger table, so no operation
/// rehashes every entry. A map that rehashed all at once held the candidates lock for
/// seconds at 100M objects, stalling every GET and write.
struct Order<V> {
    nodes: griddle::HashMap<ObjectId, Node<V>>,
    head: u64,
    tail: u64,
}

/// No neighbour. Oids come from a counter starting at 1, which never reaches it.
const NIL: u64 = u64::MAX;

/// An oid's neighbours, as raw oids or `NIL`, and its value.
struct Node<V> {
    prev: u64,
    next: u64,
    value: V,
}

impl<V> Default for Order<V> {
    fn default() -> Self {
        Self {
            nodes: griddle::HashMap::new(),
            head: NIL,
            tail: NIL,
        }
    }
}

impl<V> Order<V> {
    /// Add `oid` at the back with `value`. An oid already listed moves to the back and takes
    /// `value`.
    fn insert(&mut self, oid: ObjectId, value: V) {
        if self.move_to_back(&oid) {
            *self.get_mut(&oid).expect("just moved") = value;
            return;
        }
        let prev = std::mem::replace(&mut self.tail, oid.0);
        self.nodes.insert(
            oid,
            Node {
                prev,
                next: NIL,
                value,
            },
        );
        self.set_next(prev, oid.0);
    }

    /// Move `oid` to the back. False if it is not listed.
    fn move_to_back(&mut self, oid: &ObjectId) -> bool {
        let tail = self.tail;
        let Some(node) = self.nodes.get_mut(oid) else {
            return false;
        };
        if oid.0 != tail {
            let (prev, next) = (node.prev, node.next);
            node.prev = tail;
            node.next = NIL;
            self.set_next(prev, next);
            // Not the tail, so it has a next.
            self.link(next).prev = prev;
            self.link(tail).next = oid.0;
            self.tail = oid.0;
        }
        true
    }

    fn remove(&mut self, oid: &ObjectId) -> Option<V> {
        let node = self.nodes.remove(oid)?;
        self.set_next(node.prev, node.next);
        if node.next == NIL {
            self.tail = node.prev;
        } else {
            self.link(node.next).prev = node.prev;
        }
        Some(node.value)
    }

    fn get_mut(&mut self, oid: &ObjectId) -> Option<&mut V> {
        self.nodes.get_mut(oid).map(|node| &mut node.value)
    }

    fn contains(&self, oid: &ObjectId) -> bool {
        self.nodes.contains_key(oid)
    }

    /// The oids from the least recently used.
    fn iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        let first = (self.head != NIL).then_some(ObjectId(self.head));
        std::iter::successors(first, |oid| {
            let next = self.nodes[oid].next;
            (next != NIL).then_some(ObjectId(next))
        })
    }

    fn link(&mut self, oid: u64) -> &mut Node<V> {
        self.nodes
            .get_mut(&ObjectId(oid))
            .expect("eviction candidate is listed")
    }

    /// Point `prev`'s next (or the head, if `prev` is `NIL`) at `next`.
    fn set_next(&mut self, prev: u64, next: u64) {
        if prev == NIL {
            self.head = next;
        } else {
            self.link(prev).next = next;
        }
    }
}

/// Exact least-recently-used order: the least recently used oid is first in each.
/// `all` maps every oid to its `disk_bytes` (`insert`); `ttl` holds the oids of `all`
/// whose key has a TTL.
#[derive(Default)]
struct Lru {
    all: Order<u64>,
    ttl: Order<()>,
}

impl Lru {
    fn touch(&mut self, oid: ObjectId) {
        if self.all.move_to_back(&oid) {
            self.ttl.move_to_back(&oid);
        }
    }

    /// A TTL change is a use, as in core (EXPIRE and PERSIST touch the key).
    fn set_ttl(&mut self, oid: ObjectId, has_ttl: bool) {
        if !self.all.move_to_back(&oid) {
            // Evicted: the key lingers until the reclaim cron deletes it.
            return;
        }
        if has_ttl {
            self.ttl.insert(oid, ());
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
        let mut first = |oids: &mut dyn Iterator<Item = ObjectId>| -> Vec<ObjectId> {
            oids.filter(|&oid| take(oid)).take(n).collect()
        };
        if volatile {
            first(&mut self.ttl.iter())
        } else {
            first(&mut self.all.iter())
        }
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
    fn order_matches_a_list_model_through_incremental_resizes() {
        let mut order = Order::default();
        let mut model: Vec<u64> = Vec::new();
        let mut next = 0;
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        // Thousands of entries cross several resizes, each moving entries over many inserts.
        for step in 0..20_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let pick = (!model.is_empty()).then(|| model[(rng >> 8) as usize % model.len()]);
            match (rng % 5, pick) {
                (0, Some(oid)) => {
                    assert_eq!(order.remove(&ObjectId(oid)), Some(oid));
                    model.retain(|&o| o != oid);
                }
                (1, Some(oid)) => {
                    assert!(order.move_to_back(&ObjectId(oid)));
                    model.retain(|&o| o != oid);
                    model.push(oid);
                }
                (2, Some(oid)) => {
                    // Re-insert: moves to the back and takes the new value.
                    order.insert(ObjectId(oid), oid + 1);
                    assert_eq!(order.get_mut(&ObjectId(oid)), Some(&mut (oid + 1)));
                    *order.get_mut(&ObjectId(oid)).unwrap() = oid;
                    model.retain(|&o| o != oid);
                    model.push(oid);
                }
                _ => {
                    order.insert(ObjectId(next), next);
                    model.push(next);
                    next += 1;
                }
            }
            if step % 1_000 == 0 {
                assert!(order.iter().map(|oid| oid.0).eq(model.iter().copied()));
            }
        }
        assert!(order.iter().map(|oid| oid.0).eq(model.iter().copied()));
        assert!(!order.move_to_back(&ObjectId(next)));
        assert_eq!(order.remove(&ObjectId(next)), None);
    }
}

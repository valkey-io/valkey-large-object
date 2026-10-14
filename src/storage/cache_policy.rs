//! Cache policy primitives shared by DRAMPool and FdPool (see
//! `docs/CACHE_POLICY_DESIGN.md`): `AccessStats` (Valkey-style LFU score),
//! `OIDIndexedMap` (an `IndexMap` with sampled reclaim), and the Tiered-only
//! `AdmissionFilter`, `CacheStats` and `TieredCache`. Only `AdmissionFilter`
//! locks; the pools call the rest under their own locks.

use std::collections::{HashMap, VecDeque};

use indexmap::{IndexMap, IndexSet};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use linkme::distributed_slice;
use rand::RngExt;

use crate::data_type::ObjectId;

// ─── Clock ───────────────────────────────────────────────────────────────────

static START: OnceLock<Instant> = OnceLock::new();

/// Minutes since first use, wrapping at 16 bits, like Valkey's LFU clock. Read
/// it once per operation so the whole operation sees one time.
pub fn now_minutes() -> u16 {
    let start = START.get_or_init(Instant::now);
    (start.elapsed().as_secs() / 60) as u16
}

// ─── AccessStats ─────────────────────────────────────────────────────────────

/// Initial counter for a fresh entry. A new entry ranks below anything that has
/// been hit a few times but above anything that has fully decayed.
pub const LFU_INIT_VAL: u8 = 5;

/// Counter growth factor (see `touch`). Valkey's default `lfu-log-factor`.
pub const LFU_LOG_FACTOR: u64 = 10;

/// Admission filter capacity in ObjectIds (about 24 bytes each).
pub const ADMISSION_FILTER_CAPACITY: usize = 65_536;

/// Most cached objects one promotion may reclaim from its target segment.
/// Bounds the reclaim loop under the objects write lock.
pub const RECLAIM_MAX_VICTIMS: usize = 16;

/// Most segments one reclaim may try, the least-loaded ones.
pub const RECLAIM_MAX_SEGMENTS: usize = 3;

const COUNTER_MASK: u32 = 0xFF;
const MINUTES_SHIFT: u32 = 8;
const MINUTES_MASK: u32 = 0xFFFF;

/// Packed LFU state: bits 0..8 counter, bits 8..24 last-decay minute.
#[derive(Debug)]
pub struct AccessStats(AtomicU32);

impl AccessStats {
    pub fn new(now_min: u16) -> Self {
        Self(AtomicU32::new(Self::pack(LFU_INIT_VAL, now_min)))
    }

    /// Overwrite both fields. Lets tests place a counter without touching.
    #[cfg(test)]
    pub fn set(&self, counter: u8, now_min: u16) {
        self.0
            .store(Self::pack(counter, now_min), Ordering::Relaxed);
    }

    /// Counter after applying decay for the minutes elapsed since the last
    /// decay. This is what reclaim ranks by. `decay_time == 0` disables decay.
    pub fn decayed_counter(&self, now_min: u16, decay_time: u64) -> u8 {
        let raw = self.0.load(Ordering::Relaxed);
        Self::decay(
            (raw & COUNTER_MASK) as u8,
            ((raw >> MINUTES_SHIFT) & MINUTES_MASK) as u16,
            now_min,
            decay_time,
        )
    }

    /// Record a hit: decay, then increment with probability
    /// `1 / ((counter - LFU_INIT_VAL) * LFU_LOG_FACTOR + 1)`, then stamp `now_min`.
    ///
    /// A plain store, not a CAS. Two racing touches may lose one increment,
    /// which is acceptable for an approximate counter and matches Valkey.
    pub fn touch(&self, now_min: u16, decay_time: u64) {
        let counter = self.decayed_counter(now_min, decay_time);
        let r: f64 = rand::rng().random_range(0.0..1.0);
        let counter = Self::log_incr(counter, r);
        self.0
            .store(Self::pack(counter, now_min), Ordering::Relaxed);
    }

    fn pack(counter: u8, now_min: u16) -> u32 {
        ((now_min as u32) << MINUTES_SHIFT) | counter as u32
    }

    /// One point off per `decay_time` minutes since `last_min`, on a 16-bit
    /// wrapping clock. `decay_time == 0` disables decay.
    fn decay(counter: u8, last_min: u16, now_min: u16, decay_time: u64) -> u8 {
        if decay_time == 0 {
            return counter;
        }
        let periods = now_min.wrapping_sub(last_min) as u64 / decay_time;
        counter.saturating_sub(periods.min(u8::MAX as u64) as u8)
    }

    /// Logarithmic increment. `r` is a uniform draw in `[0, 1)`. At `LFU_INIT_VAL`
    /// the probability is 1.
    fn log_incr(counter: u8, r: f64) -> u8 {
        if counter == u8::MAX {
            return counter;
        }
        let base = counter.saturating_sub(LFU_INIT_VAL) as f64;
        let p = 1.0 / (base * LFU_LOG_FACTOR as f64 + 1.0);
        if r < p {
            counter + 1
        } else {
            counter
        }
    }
}

// ─── AdmissionFilter ─────────────────────────────────────────────────────────

/// Second-touch admission for the DRAM cache: miss counts for recently missed
/// objects, in a FIFO of at most `capacity` entries. ObjectIds are never
/// reused, so an entry for a deleted object just ages out.
#[derive(Debug)]
pub struct AdmissionFilter {
    misses: Mutex<MissLog>,
    capacity: usize,
    /// Tiered GET misses served transiently because the object had not yet
    /// missed `promote-min-hits` times.
    pub rejects: AtomicU64,
}

#[derive(Debug, Default)]
struct MissLog {
    counts: HashMap<ObjectId, u8>,
    order: VecDeque<ObjectId>,
}

impl Default for AdmissionFilter {
    fn default() -> Self {
        Self::new(ADMISSION_FILTER_CAPACITY)
    }
}

impl AdmissionFilter {
    /// Nothing is preallocated, so a pool that never misses pays nothing.
    pub fn new(capacity: usize) -> Self {
        Self {
            misses: Mutex::new(MissLog::default()),
            capacity,
            rejects: AtomicU64::new(0),
        }
    }

    /// True once the object has missed `promote-min-hits` times. Oversize
    /// objects are refused without being tracked. Entries stay after promotion
    /// and age out FIFO (design doc §4.3).
    pub fn admit(&self, oid: ObjectId, obj_len: u64) -> bool {
        if obj_len > crate::max_promote_size() {
            return false;
        }
        let min_hits = crate::promote_min_hits();
        if min_hits <= 1 {
            return true;
        }
        if self.track(oid) >= min_hits {
            true
        } else {
            self.rejects.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Count a miss for `oid` and return its total misses, saturating at 255.
    /// When full, the oldest entry is dropped first, so `counts` and `order`
    /// always hold the same OIDs.
    fn track(&self, oid: ObjectId) -> u8 {
        let mut log = self
            .misses
            .lock()
            .expect("AdmissionFilter lock unavailable");
        if let Some(n) = log.counts.get_mut(&oid) {
            *n = n.saturating_add(1);
            return *n;
        }
        if log.counts.len() >= self.capacity {
            if let Some(old) = log.order.pop_front() {
                log.counts.remove(&old);
            }
        }
        log.counts.insert(oid, 1);
        log.order.push_back(oid);
        1
    }
}

// ─── TieredCache ─────────────────────────────────────────────────────────────

/// DRAM cache effectiveness counters. Tiered mode only.
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Tiered GETs served from a Ready cached object.
    pub hits: AtomicU64,
    /// Tiered GETs that found no Ready cached object (absent or still Filling).
    pub misses: AtomicU64,
    /// Successful `try_promote_object` calls.
    pub promotions: AtomicU64,
    /// Cached copies demoted (dropped from DRAM, NVMe copy kept) by
    /// `make_room_for` to free space, or by a shrink.
    pub demotions: AtomicU64,
}

impl CacheStats {
    /// Record a GET served from the cache: touch the object's LFU score and
    /// count the hit. Atomic only; no lock needed.
    pub fn record_hit(&self, stats: &AccessStats) {
        stats.touch(now_minutes(), crate::blob_decay_time());
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a GET that could not be served from the cache.
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}

/// State that exists only when DRAM caches NVMe (Tiered mode). DRAMPool holds
/// it as `Option`, `None` in Dram mode.
#[derive(Debug, Default)]
pub struct TieredCache {
    pub admission: AdmissionFilter,
    pub stats: CacheStats,
}

// ─── MaxmemoryPolicy ─────────────────────────────────────────────────────────

/// The server's `maxmemory-policy`, as module eviction uses it. The module API has no
/// getter for it (`GetContextFlags` says only whether some evicting policy is set), so it
/// is read once at load and again whenever a `CONFIG SET` changes it, and cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxmemoryPolicy {
    /// `volatile-*`: only keys with a TTL may be evicted.
    pub volatile: bool,
    /// `noeviction`: nothing may be evicted.
    pub noeviction: bool,
    /// `*-lru`: evict in exact least-recently-used order (`LruList`).
    pub lru: bool,
}

/// The cached policy, as `MaxmemoryPolicy::bits`: one value, so a reader never sees half
/// of a change.
static POLICY: AtomicU8 = AtomicU8::new(0);
const VOLATILE: u8 = 1;
const NOEVICTION: u8 = 2;
const LRU: u8 = 4;

impl MaxmemoryPolicy {
    fn parse(name: &str) -> Self {
        Self {
            volatile: name.starts_with("volatile-"),
            noeviction: name == "noeviction",
            lru: name.ends_with("-lru"),
        }
    }

    fn bits(self) -> u8 {
        (if self.volatile { VOLATILE } else { 0 })
            | (if self.noeviction { NOEVICTION } else { 0 })
            | (if self.lru { LRU } else { 0 })
    }

    /// The policy now in force: one atomic load.
    pub fn current() -> Self {
        let bits = POLICY.load(Ordering::Relaxed);
        Self {
            volatile: bits & VOLATILE != 0,
            noeviction: bits & NOEVICTION != 0,
            lru: bits & LRU != 0,
        }
    }

    /// Read the policy from the server and cache it. Call at load, and on a change of
    /// `maxmemory-policy` (`on_config_change`).
    pub fn refresh(ctx: &valkey_module::Context) {
        let info = ctx.server_info("memory");
        let name = info
            .field("maxmemory_policy")
            .map(|v| v.to_string())
            .unwrap_or_default();
        POLICY.store(Self::parse(&name).bits(), Ordering::Relaxed);
    }
}

/// `CONFIG SET` fired this after applying the change, so `refresh` reads the new value.
#[distributed_slice(valkey_module::server_events::CONFIG_CHANGED_SERVER_EVENTS_LIST)]
fn on_config_change(ctx: &valkey_module::Context, changed: &[&str]) {
    if changed.contains(&"maxmemory-policy") {
        MaxmemoryPolicy::refresh(ctx);
    }
}

// ─── LruList ─────────────────────────────────────────────────────────────────

/// End of a list.
const NIL: u32 = u32::MAX;
/// `prev` of a node that is not in that list.
const OFF: u32 = u32::MAX - 1;
/// The oid of a free slot. Real oids start at 1.
const FREE: ObjectId = ObjectId(0);

#[derive(Debug, Clone, Copy)]
struct Links {
    prev: u32,
    next: u32,
}

const UNLINKED: Links = Links {
    prev: OFF,
    next: NIL,
};

/// `oldest` and `newest` of one list.
#[derive(Debug, Clone, Copy)]
struct Ends {
    oldest: u32,
    newest: u32,
}

/// Index into a node's `links` and the list's `ends`.
#[derive(Debug, Clone, Copy)]
enum List {
    All = 0,
    Ttl = 1,
}

#[derive(Debug)]
struct Node {
    oid: ObjectId,
    links: [Links; 2],
}

/// What `LruList::oldest` does with a node it walks past.
pub enum Pick {
    /// A candidate.
    Take,
    /// Not now (in use); it keeps its place.
    Pass,
    /// No longer evictable (already evicted). Unlinked, so no later walk meets it; its
    /// owner still `remove`s it.
    Unlink,
}

/// Exact least-recently-used order of objects, for `*-lru` eviction. Two doubly linked
/// lists over one slab: every object, and the objects whose key has a TTL. A node's slot
/// is its handle, kept by its owner, so a touch or remove needs no lookup. Each call also
/// names the oid, and is ignored when the slot no longer holds it (freed, maybe reused).
///
/// A TTL change is a use, as in core (EXPIRE and PERSIST touch the key), so every move
/// into the TTL list also moves the node to the newest end of the full list: the TTL list
/// stays the full list's order restricted to keys with a TTL.
///
/// 24 bytes a node; freed slots are reused, and the slab never shrinks. Not locked: its
/// owner calls it under its own lock.
#[derive(Debug)]
pub struct LruList {
    nodes: Vec<Node>,
    /// Free slots, chained through `links[All].next`.
    free: u32,
    ends: [Ends; 2],
}

impl Default for LruList {
    fn default() -> Self {
        let empty = Ends {
            oldest: NIL,
            newest: NIL,
        };
        Self {
            nodes: Vec::new(),
            free: NIL,
            ends: [empty; 2],
        }
    }
}

impl LruList {
    /// Add `oid` as the most recently used object. Returns its slot.
    pub fn insert(&mut self, oid: ObjectId) -> u32 {
        let node = Node {
            oid,
            links: [UNLINKED; 2],
        };
        let slot = if self.free == NIL {
            self.nodes.push(node);
            u32::try_from(self.nodes.len() - 1).expect("LruList slots fit in u32")
        } else {
            let slot = self.free;
            self.free = self.links(List::All, slot).next;
            self.nodes[slot as usize] = node;
            slot
        };
        self.push_newest(List::All, slot);
        slot
    }

    /// Record a use: `oid` becomes the most recently used object.
    pub fn touch(&mut self, slot: u32, oid: ObjectId) {
        if !self.listed(slot, oid) {
            return;
        }
        for list in [List::All, List::Ttl] {
            if self.linked(list, slot) {
                self.unlink(list, slot);
                self.push_newest(list, slot);
            }
        }
    }

    /// Record whether `oid`'s key has a TTL. Also a use.
    pub fn set_ttl(&mut self, slot: u32, oid: ObjectId, has_ttl: bool) {
        if !self.listed(slot, oid) {
            return;
        }
        self.touch(slot, oid);
        match (has_ttl, self.linked(List::Ttl, slot)) {
            (true, false) => self.push_newest(List::Ttl, slot),
            (false, true) => self.unlink(List::Ttl, slot),
            _ => {}
        }
    }

    /// The object is gone: drop its node and free its slot.
    pub fn remove(&mut self, slot: u32, oid: ObjectId) {
        if !self.holds(slot, oid) {
            return;
        }
        self.unlink_both(slot);
        let free = self.free;
        let node = &mut self.nodes[slot as usize];
        node.oid = FREE;
        node.links[List::All as usize].next = free;
        self.free = slot;
    }

    /// Up to `n` `(slot, oid)` candidates, least recently used first, from the TTL list if
    /// `volatile`, else from every object. `pick` decides each node walked past.
    pub fn oldest(
        &mut self,
        volatile: bool,
        n: usize,
        mut pick: impl FnMut(ObjectId) -> Pick,
    ) -> Vec<(u32, ObjectId)> {
        let list = if volatile { List::Ttl } else { List::All };
        let mut taken = Vec::new();
        let mut slot = self.ends[list as usize].oldest;
        while slot != NIL && taken.len() < n {
            let oid = self.nodes[slot as usize].oid;
            let next = self.links(list, slot).next;
            match pick(oid) {
                Pick::Take => taken.push((slot, oid)),
                Pick::Pass => {}
                Pick::Unlink => self.unlink_both(slot),
            }
            slot = next;
        }
        taken
    }

    fn holds(&self, slot: u32, oid: ObjectId) -> bool {
        oid != FREE && self.nodes.get(slot as usize).is_some_and(|n| n.oid == oid)
    }

    /// `slot` holds `oid` and is still in the full list (not evicted).
    fn listed(&self, slot: u32, oid: ObjectId) -> bool {
        self.holds(slot, oid) && self.linked(List::All, slot)
    }

    fn linked(&self, list: List, slot: u32) -> bool {
        self.links(list, slot).prev != OFF
    }

    fn links(&self, list: List, slot: u32) -> Links {
        self.nodes[slot as usize].links[list as usize]
    }

    fn links_mut(&mut self, list: List, slot: u32) -> &mut Links {
        &mut self.nodes[slot as usize].links[list as usize]
    }

    fn push_newest(&mut self, list: List, slot: u32) {
        let newest = self.ends[list as usize].newest;
        *self.links_mut(list, slot) = Links {
            prev: newest,
            next: NIL,
        };
        if newest == NIL {
            self.ends[list as usize].oldest = slot;
        } else {
            self.links_mut(list, newest).next = slot;
        }
        self.ends[list as usize].newest = slot;
    }

    fn unlink(&mut self, list: List, slot: u32) {
        let Links { prev, next } = self.links(list, slot);
        if prev == NIL {
            self.ends[list as usize].oldest = next;
        } else {
            self.links_mut(list, prev).next = next;
        }
        if next == NIL {
            self.ends[list as usize].newest = prev;
        } else {
            self.links_mut(list, next).prev = prev;
        }
        *self.links_mut(list, slot) = UNLINKED;
    }

    fn unlink_both(&mut self, slot: u32) {
        for list in [List::All, List::Ttl] {
            if self.linked(list, slot) {
                self.unlink(list, slot);
            }
        }
    }
}

// ─── Sampled victim selection ────────────────────────────────────────────────

/// A dense collection that `sample_victim` can read by index.
pub(super) trait Sampled {
    type Item;
    fn len(&self) -> usize;
    fn at(&self, index: usize) -> Option<&Self::Item>;
}

impl<V> Sampled for OIDIndexedMap<V> {
    type Item = V;
    fn len(&self) -> usize {
        IndexMap::len(self)
    }
    fn at(&self, index: usize) -> Option<&V> {
        self.get_index(index).map(|(_, v)| v)
    }
}

impl Sampled for OIDIndexedSet {
    type Item = ObjectId;
    fn len(&self) -> usize {
        IndexSet::len(self)
    }
    fn at(&self, index: usize) -> Option<&ObjectId> {
        self.get_index(index)
    }
}

/// Index of the lowest-scoring unpinned entry among up to `samples` entries
/// of `items`; `probe` returns `None` for a pinned entry. Collections with
/// `len <= samples` are scanned fully (exact); larger ones get `samples`
/// random draws, as in Valkey.
pub(super) fn sample_victim<C, F>(items: &C, samples: usize, mut probe: F) -> Option<usize>
where
    C: Sampled,
    F: FnMut(&C::Item) -> Option<u8>,
{
    let len = items.len();
    let mut best: Option<(usize, u8)> = None;
    let mut consider = |index: usize| {
        if let Some(score) = items.at(index).and_then(&mut probe) {
            if best.is_none_or(|(_, s)| score < s) {
                best = Some((index, score));
            }
        }
    };
    if len <= samples {
        (0..len).for_each(&mut consider);
    } else {
        let mut rng = rand::rng();
        (0..samples).for_each(|_| consider(rng.random_range(0..len)));
    }
    best.map(|(index, _)| index)
}

// ─── OIDIndexedMap ─────────────────────────────────────────────────────────────

/// A map from `ObjectId` that can also be sampled by index. `IndexMap` keeps
/// entries in a dense `Vec`, so a sample is a direct index with no hashing,
/// and `swap_remove` is O(1).
pub type OIDIndexedMap<V> = IndexMap<ObjectId, V>;

/// The set form of `OIDIndexedMap`: same dense storage and O(1) `swap_remove`.
pub type OIDIndexedSet = IndexSet<ObjectId>;

/// Remove and return the lowest-scoring entry among up to `samples` entries
/// (see `sample_victim`). `score` returns `None` for a pinned entry.
pub fn reclaim_one<V, F>(
    map: &mut OIDIndexedMap<V>,
    samples: usize,
    score: F,
) -> Option<(ObjectId, V)>
where
    F: FnMut(&V) -> Option<u8>,
{
    let victim = sample_victim(map, samples, score)?;
    map.swap_remove_index(victim)
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── AccessStats ───

    /// Raw counter: decay disabled, so the minute argument is irrelevant.
    fn raw(s: &AccessStats) -> u8 {
        s.decayed_counter(0, 0)
    }

    #[test]
    fn decay_subtracts_one_per_period() {
        let s = AccessStats::new(0);
        s.set(LFU_INIT_VAL + 20, 10);
        // 7 minutes later at decay_time 1: minus 7.
        assert_eq!(s.decayed_counter(17, 1), LFU_INIT_VAL + 13);
        // decay_time 5: 7/5 = 1 period.
        assert_eq!(s.decayed_counter(17, 5), LFU_INIT_VAL + 19);
        // decay_time 0 turns decay off.
        assert_eq!(s.decayed_counter(5000, 0), LFU_INIT_VAL + 20);
        // Saturates at zero; elapsed beyond 255 periods must not wrap the u8.
        assert_eq!(s.decayed_counter(u16::MAX, 1), 0);
        // Reads never write back.
        assert_eq!(raw(&s), LFU_INIT_VAL + 20);
        // 16-bit clock wrap: 65534 -> 1 is 3 minutes.
        s.set(LFU_INIT_VAL + 10, u16::MAX - 1);
        assert_eq!(s.decayed_counter(1, 1), LFU_INIT_VAL + 7);
    }

    #[test]
    fn touch_applies_decay_then_restamps() {
        // Four points above init at minute 0. Four minutes later the decay
        // brings it back to INIT_VAL, where the increment is certain, so the
        // result is INIT_VAL + 1. Incrementing before decaying would give
        // INIT_VAL instead.
        let s = AccessStats::new(0);
        s.set(LFU_INIT_VAL + 4, 0);
        s.touch(4, 1);
        assert_eq!(raw(&s), LFU_INIT_VAL + 1);
        // Restamped to minute 4: one minute later exactly one point is gone.
        // A stale stamp of 0 would read five minutes and give INIT_VAL - 4.
        assert_eq!(s.decayed_counter(5, 1), LFU_INIT_VAL);
    }

    // ─── AdmissionFilter ───

    #[test]
    fn filter_counts_misses_and_drops_oldest() {
        let f = AdmissionFilter::new(3);
        let len = |f: &AdmissionFilter| {
            let log = f.misses.lock().unwrap();
            (log.counts.len(), log.order.len())
        };
        assert_eq!(f.track(ObjectId(1)), 1);
        assert_eq!(f.track(ObjectId(1)), 2);
        for i in 2..=4 {
            f.track(ObjectId(i));
        }
        // Full at 3: OID 1 was the oldest and is gone; OID 4 is still counted.
        assert_eq!(len(&f), (3, 3));
        assert_eq!(f.track(ObjectId(4)), 2);
        assert_eq!(f.track(ObjectId(1)), 1);
        assert_eq!(len(&f), (3, 3));
    }

    // ─── MaxmemoryPolicy ───

    #[test]
    fn parse_policies() {
        let p = |volatile, noeviction, lru| MaxmemoryPolicy {
            volatile,
            noeviction,
            lru,
        };
        assert_eq!(MaxmemoryPolicy::parse("volatile-lru"), p(true, false, true));
        assert_eq!(
            MaxmemoryPolicy::parse("volatile-lfu"),
            p(true, false, false)
        );
        assert_eq!(
            MaxmemoryPolicy::parse("volatile-random"),
            p(true, false, false)
        );
        assert_eq!(
            MaxmemoryPolicy::parse("volatile-ttl"),
            p(true, false, false)
        );
        assert_eq!(MaxmemoryPolicy::parse("allkeys-lru"), p(false, false, true));
        assert_eq!(
            MaxmemoryPolicy::parse("allkeys-lfu"),
            p(false, false, false)
        );
        assert_eq!(
            MaxmemoryPolicy::parse("allkeys-random"),
            p(false, false, false)
        );
        assert_eq!(MaxmemoryPolicy::parse("noeviction"), p(false, true, false));
    }

    // ─── LruList ───

    /// Every candidate, oldest first: of the TTL list if `volatile`, else of all.
    fn order(lru: &mut LruList, volatile: bool) -> Vec<u64> {
        lru.oldest(volatile, usize::MAX, |_| Pick::Take)
            .into_iter()
            .map(|(_, oid)| oid.0)
            .collect()
    }

    #[test]
    fn lru_orders_by_last_use() {
        let mut lru = LruList::default();
        let slots: Vec<u32> = (1..=4).map(|oid| lru.insert(ObjectId(oid))).collect();
        assert_eq!(order(&mut lru, false), vec![1, 2, 3, 4]);
        lru.touch(slots[0], ObjectId(1));
        lru.touch(slots[2], ObjectId(3));
        assert_eq!(order(&mut lru, false), vec![2, 4, 1, 3]);
        // The oldest and the newest end both unlink cleanly.
        lru.remove(slots[1], ObjectId(2));
        lru.remove(slots[2], ObjectId(3));
        assert_eq!(order(&mut lru, false), vec![4, 1]);
    }

    #[test]
    fn lru_ttl_list_follows_the_full_order() {
        let mut lru = LruList::default();
        let slots: Vec<u32> = (1..=4).map(|oid| lru.insert(ObjectId(oid))).collect();
        // A TTL change is a use: 4 and then 2 move to the newest end.
        lru.set_ttl(slots[3], ObjectId(4), true);
        lru.set_ttl(slots[1], ObjectId(2), true);
        assert_eq!(order(&mut lru, false), vec![1, 3, 4, 2]);
        assert_eq!(order(&mut lru, true), vec![4, 2]);
        lru.touch(slots[3], ObjectId(4));
        assert_eq!(order(&mut lru, true), vec![2, 4]);
        // PERSIST: out of the TTL list, still in the full one.
        lru.set_ttl(slots[1], ObjectId(2), false);
        assert_eq!(order(&mut lru, true), vec![4]);
        assert_eq!(order(&mut lru, false), vec![1, 3, 4, 2]);
        lru.remove(slots[3], ObjectId(4));
        assert!(order(&mut lru, true).is_empty());
    }

    #[test]
    fn lru_ignores_a_stale_slot() {
        let mut lru = LruList::default();
        let old = lru.insert(ObjectId(1));
        lru.insert(ObjectId(2));
        lru.remove(old, ObjectId(1));
        // The freed slot is reused by oid 3; calls naming oid 1 must leave it alone.
        let reused = lru.insert(ObjectId(3));
        assert_eq!(reused, old);
        lru.touch(old, ObjectId(1));
        lru.set_ttl(old, ObjectId(1), true);
        lru.remove(old, ObjectId(1));
        assert_eq!(order(&mut lru, false), vec![2, 3]);
        assert!(order(&mut lru, true).is_empty());
    }

    #[test]
    fn lru_walk_passes_or_unlinks() {
        let mut lru = LruList::default();
        let slots: Vec<u32> = (1..=4).map(|oid| lru.insert(ObjectId(oid))).collect();
        let picked = lru.oldest(false, 2, |oid| match oid.0 {
            1 => Pick::Pass,
            2 => Pick::Unlink,
            _ => Pick::Take,
        });
        assert_eq!(
            picked,
            vec![(slots[2], ObjectId(3)), (slots[3], ObjectId(4))]
        );
        // 1 kept its place; 2 left the walk, and a touch does not bring it back.
        lru.touch(slots[1], ObjectId(2));
        assert_eq!(order(&mut lru, false), vec![1, 3, 4]);
        // Its owner still frees the slot.
        lru.remove(slots[1], ObjectId(2));
        assert_eq!(lru.insert(ObjectId(5)), slots[1]);
    }

    // ─── sample_victim ───

    /// A map of `len` entries whose value is its own index.
    fn indexed(len: usize) -> OIDIndexedMap<usize> {
        (0..len).map(|i| (ObjectId(i as u64), i)).collect()
    }

    #[test]
    fn sample_victim_scans_small_maps_and_skips_pinned() {
        // len <= samples: every index is probed exactly once, so the minimum
        // reclaimable score is found exactly. Index 1 (score 3) is pinned.
        let scores = [50u8, 3, 20, 7];
        let mut probed = [0u32; 4];
        let v = sample_victim(&indexed(4), 4, |&i| {
            probed[i] += 1;
            (i != 1).then_some(scores[i])
        });
        assert_eq!(v, Some(3));
        assert_eq!(probed, [1; 4]);
        // Empty map or everything pinned: no victim.
        assert_eq!(sample_victim(&indexed(0), 5, |_| Some(0)), None);
        assert_eq!(sample_victim(&indexed(5), 0, |_| Some(0)), None);
        assert_eq!(sample_victim(&indexed(4), 16, |_| None), None);
        assert_eq!(sample_victim(&indexed(100), 5, |_| None), None);
    }

    #[test]
    fn sample_victim_samples_when_large() {
        // len > samples: indices are drawn at random, so the assertions hold for
        // any draw. At most `samples` distinct indices are probed (fewer than
        // len), and the victim is the lowest score among exactly those.
        let mut probed = std::collections::BTreeSet::new();
        let v = sample_victim(&indexed(100), 64, |&i| {
            probed.insert(i);
            Some(i as u8)
        })
        .expect("some entry is unpinned");
        assert!(!probed.is_empty() && probed.len() <= 64, "{probed:?}");
        assert_eq!(Some(&v), probed.iter().next());
    }

    // ─── reclaim_one ───

    #[test]
    fn reclaim_one_removes_lowest_unpinned() {
        let mut m: OIDIndexedMap<u8> = OIDIndexedMap::new();
        for (i, score) in [50u8, 3, 20, 7].into_iter().enumerate() {
            m.insert(ObjectId(i as u64), score);
        }
        // Score 3 is pinned, so 7 is the victim.
        let v = reclaim_one(&mut m, 16, |&s| (s != 3).then_some(s));
        assert_eq!(v, Some((ObjectId(3), 7)));
        assert_eq!(m.len(), 3);
        assert!(!m.contains_key(&ObjectId(3)));
        // Everything pinned: nothing removed.
        assert_eq!(reclaim_one(&mut m, 16, |_| None), None);
        assert_eq!(m.len(), 3);
    }
}

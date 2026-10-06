//! Cache policy primitives shared by DRAMPool and FdPool (see
//! `docs/CACHE_POLICY_DESIGN.md`): `AccessStats` (Valkey-style LFU score),
//! `OIDIndexedMap` (an `IndexMap` with sampled reclaim), and the Tiered-only
//! `AdmissionFilter`, `CacheStats` and `TieredCache`. Only `AdmissionFilter`
//! locks; the pools call the rest under their own locks.

use std::collections::{HashMap, VecDeque};

use indexmap::{IndexMap, IndexSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

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
}

impl CacheStats {
    /// Record a GET served from the cache: touch the object's LFU score and
    /// count the hit. Atomic only; no lock needed.
    pub fn record_hit(&self, stats: &AccessStats) {
        stats.touch(now_minutes(), crate::tiered_decay_time());
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

// ─── Sampled victim selection ────────────────────────────────────────────────

/// Lowest-scoring unpinned slot among up to `samples` slots in `0..len`;
/// `probe` returns `None` for a pinned slot. Maps with `len <= samples` are
/// scanned fully (exact); larger ones get `samples` random draws, as in Valkey.
pub(super) fn sample_victim<F>(len: usize, samples: usize, mut probe: F) -> Option<usize>
where
    F: FnMut(usize) -> Option<u8>,
{
    let mut best: Option<(usize, u8)> = None;
    let mut consider = |slot: usize| {
        if let Some(score) = probe(slot) {
            if best.is_none_or(|(_, s)| score < s) {
                best = Some((slot, score));
            }
        }
    };
    if len <= samples {
        (0..len).for_each(&mut consider);
    } else {
        let mut rng = rand::rng();
        (0..samples).for_each(|_| consider(rng.random_range(0..len)));
    }
    best.map(|(slot, _)| slot)
}

// ─── OIDIndexedMap ─────────────────────────────────────────────────────────────

/// A map from `ObjectId` that can also be sampled by slot. `IndexMap` keeps
/// entries in a dense `Vec`, so a sample is a direct index with no hashing,
/// and `swap_remove` is O(1).
pub type OIDIndexedMap<V> = IndexMap<ObjectId, V>;

/// The set form of `OIDIndexedMap`: same dense storage and O(1) `swap_remove`.
pub type OIDIndexedSet = IndexSet<ObjectId>;

/// Remove and return the lowest-scoring entry among up to `samples` slots
/// (see `sample_victim`). `score` returns `None` for a pinned entry.
pub fn reclaim_one<V, F>(
    map: &mut OIDIndexedMap<V>,
    samples: usize,
    mut score: F,
) -> Option<(ObjectId, V)>
where
    F: FnMut(&V) -> Option<u8>,
{
    let slot = sample_victim(map.len(), samples, |s| score(&map[s]))?;
    map.swap_remove_index(slot)
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

    // ─── sample_victim ───

    #[test]
    fn sample_victim_scans_small_maps_and_skips_pinned() {
        // len <= samples: every slot is probed exactly once, so the minimum
        // reclaimable score is found exactly. Slot 1 (score 3) is pinned.
        let scores = [50u8, 3, 20, 7];
        let mut probed = [0u32; 4];
        let v = sample_victim(4, 4, |i| {
            probed[i] += 1;
            (i != 1).then_some(scores[i])
        });
        assert_eq!(v, Some(3));
        assert_eq!(probed, [1; 4]);
        // Empty map or everything pinned: no victim.
        assert_eq!(sample_victim(0, 5, |_| Some(0)), None);
        assert_eq!(sample_victim(5, 0, |_| Some(0)), None);
        assert_eq!(sample_victim(4, 16, |_| None), None);
        assert_eq!(sample_victim(100, 5, |_| None), None);
    }

    #[test]
    fn sample_victim_samples_when_large() {
        // len > samples: slots are drawn at random, so the assertions hold for
        // any draw. At most `samples` distinct slots are probed (fewer than
        // len), and the victim is the lowest score among exactly those.
        let mut probed = std::collections::BTreeSet::new();
        let v = sample_victim(100, 64, |i| {
            probed.insert(i);
            Some(i as u8)
        })
        .expect("some slot is unpinned");
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

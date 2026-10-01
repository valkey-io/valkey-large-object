//! Cache policy primitives shared by DRAMPool and FdPool (see
//! `docs/CACHE_POLICY_DESIGN.md`): `AccessStats` (Valkey-style LFU score),
//! `GhostTable` (second-touch admission) and `IndexedMap` (an `IndexMap` with
//! sampled demotion). None of them lock; the pools call them under their own locks.

use std::collections::{HashMap, VecDeque};

use indexmap::IndexMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
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

/// Ghost table capacity in ObjectIds (about 24 bytes each).
pub const GHOST_CAPACITY: usize = 65_536;

/// Most cached objects one promotion may demote to make room. Bounds the inline
/// demotion loop on the main thread.
pub const DEMOTE_MAX_VICTIMS: usize = 16;

const COUNTER_MASK: u32 = 0xFF;
const MINUTES_SHIFT: u32 = 8;
const MINUTES_MASK: u32 = 0xFFFF;

/// Packed LFU state: bits 0..8 counter, bits 8..24 last-decay minute.
#[derive(Debug)]
pub struct AccessStats(AtomicU32);

impl AccessStats {
    pub fn new(now_min: u16) -> Self {
        Self(AtomicU32::new(pack(LFU_INIT_VAL, now_min)))
    }

    /// Overwrite both fields. Lets tests place a counter without touching.
    #[cfg(test)]
    pub fn set(&self, counter: u8, now_min: u16) {
        self.0.store(pack(counter, now_min), Ordering::Relaxed);
    }

    /// Counter after applying decay for the minutes elapsed since the last
    /// decay. This is what demotion ranks by. `decay_time == 0` disables decay.
    pub fn decayed_counter(&self, now_min: u16, decay_time: u64) -> u8 {
        let raw = self.0.load(Ordering::Relaxed);
        decay(
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
        let counter = log_incr(counter, r);
        self.0.store(pack(counter, now_min), Ordering::Relaxed);
    }
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

// ─── GhostTable ──────────────────────────────────────────────────────────────

/// Admission filter: miss counts for recently missed objects, in a FIFO of at
/// most `capacity` entries. ObjectIds are never reused, so an entry for a
/// deleted object just ages out. Not thread safe; the pool locks it.
#[derive(Debug)]
pub struct GhostTable {
    hits: HashMap<ObjectId, u8>,
    order: VecDeque<ObjectId>,
    capacity: usize,
}

impl GhostTable {
    /// Nothing is preallocated, so a pool that never misses pays nothing.
    pub fn new(capacity: usize) -> Self {
        Self {
            hits: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    /// Count a miss for `oid` and return its total misses, saturating at 255.
    pub fn record_miss(&mut self, oid: ObjectId) -> u8 {
        if let Some(n) = self.hits.get_mut(&oid) {
            *n = n.saturating_add(1);
            return *n;
        }
        self.make_room();
        self.hits.insert(oid, 1);
        self.order.push_back(oid);
        1
    }

    /// Drop the oldest entry if the table is full. Entries are never removed
    /// out of order, so `hits` and `order` always hold the same OIDs.
    fn make_room(&mut self) {
        if self.hits.len() >= self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.hits.remove(&old);
            }
        }
    }
}

// ─── Sampled victim selection ────────────────────────────────────────────────

/// Lowest-scoring unpinned slot among up to `samples` slots in `0..len`;
/// `probe` returns `None` for a pinned slot. Maps with `len <= samples` are
/// scanned fully (exact); larger ones get `samples` random draws, as in Valkey.
fn sample_victim<F>(len: usize, samples: usize, mut probe: F) -> Option<usize>
where
    F: FnMut(usize) -> Option<u8>,
{
    if len == 0 || samples == 0 {
        return None;
    }
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

// ─── IndexedMap ──────────────────────────────────────────────────────────────

/// A map from `ObjectId` that can also be sampled by slot. `IndexMap` keeps
/// entries in a dense `Vec`, so a sample is a direct index with no hashing,
/// and `swap_remove` is O(1).
pub type IndexedMap<V> = IndexMap<ObjectId, V>;

/// Remove and return the lowest-scoring entry among up to `samples` slots
/// (see `sample_victim`). `score` returns `None` for a pinned entry.
pub fn demote_one<V, F>(
    map: &mut IndexedMap<V>,
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

    // ─── GhostTable ───

    #[test]
    fn ghost_counts_misses_and_drops_oldest() {
        let mut g = GhostTable::new(3);
        assert_eq!(g.record_miss(ObjectId(1)), 1);
        assert_eq!(g.record_miss(ObjectId(1)), 2);
        for i in 2..=4 {
            g.record_miss(ObjectId(i));
        }
        // Full at 3: OID 1 was the oldest and is gone; OID 4 is still counted.
        assert_eq!((g.hits.len(), g.order.len()), (3, 3));
        assert_eq!(g.record_miss(ObjectId(4)), 2);
        assert_eq!(g.record_miss(ObjectId(1)), 1);
        assert_eq!((g.hits.len(), g.order.len()), (3, 3));
    }

    // ─── sample_victim ───

    #[test]
    fn sample_victim_scans_small_maps_and_skips_pinned() {
        // len <= samples: every slot is probed exactly once, so the minimum
        // demotable score is found exactly. Slot 1 (score 3) is pinned.
        let scores = [50u8, 3, 20, 7];
        let mut probed = [0u32; 4];
        let v = sample_victim(4, 4, |i| {
            probed[i] += 1;
            (i != 1).then_some(scores[i])
        });
        assert_eq!(v, Some(3));
        assert_eq!(probed, [1; 4]);
        // Empty map, no draws, or everything pinned: no victim.
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

    // ─── demote_one ───

    #[test]
    fn demote_one_removes_lowest_unpinned() {
        let mut m: IndexedMap<u8> = IndexedMap::new();
        for (i, score) in [50u8, 3, 20, 7].into_iter().enumerate() {
            m.insert(ObjectId(i as u64), score);
        }
        // Score 3 is pinned, so 7 is the victim.
        let v = demote_one(&mut m, 16, |&s| (s != 3).then_some(s));
        assert_eq!(v, Some((ObjectId(3), 7)));
        assert_eq!(m.len(), 3);
        assert!(!m.contains_key(&ObjectId(3)));
        // Everything pinned: nothing removed.
        assert_eq!(demote_one(&mut m, 16, |_| None), None);
        assert_eq!(m.len(), 3);
    }
}

//! Live object index (Tiered): which oids have a file, which of those have a TTL, and
//! their LRU order. Eviction picks from it instead of reading `disk-dir`. Also home of the
//! keyspace-event handler that records TTL changes for both modes (Dram keeps them on
//! `ObjectContext` and in the DRAMPool's own LRU list).
//!
//! The oid is a dense integer, so it is the bit position in both bitmaps.
//! `select(k)` on a bitmap returns its k-th set bit, a uniformly random live oid.

use std::sync::{LazyLock, Mutex};

use rand::RngExt;
use roaring::RoaringTreemap;
use valkey_module::{Context, NotifyEvent, ValkeyValue};

use super::cache_policy::{LruList, MaxmemoryPolicy, Pick};
use crate::data_type::{LoValue, ObjectId, LO_TYPE};

#[derive(Default)]
struct Live {
    /// Every oid with a live file.
    all: RoaringTreemap,
    /// The oids in `all` whose key has a TTL. Always a subset of `all`.
    ttl: RoaringTreemap,
    /// Every file, least recently used first. An evicted oid leaves the bitmaps at once
    /// and this list when an LRU walk meets it; its slot is freed when the file drops.
    lru: LruList,
}

static LIVE: LazyLock<Mutex<Live>> = LazyLock::new(Mutex::default);

fn live() -> std::sync::MutexGuard<'static, Live> {
    LIVE.lock().expect("LIVE lock unavailable")
}

/// A new object file exists. Returns its LRU slot.
pub fn insert(oid: ObjectId) -> u32 {
    let mut live = live();
    live.all.insert(oid.0);
    live.lru.insert(oid)
}

/// Record a GET of the file.
pub fn touch(slot: u32, oid: ObjectId) {
    live().lru.touch(slot, oid);
}

/// The object is evicted: no eviction may pick it again. Clears both bitmaps.
pub fn evicted(oid: ObjectId) {
    let mut live = live();
    live.all.remove(oid.0);
    live.ttl.remove(oid.0);
}

/// The file is gone. Clears both bitmaps and frees its LRU slot.
pub fn remove(oid: ObjectId, slot: u32) {
    let mut live = live();
    live.all.remove(oid.0);
    live.ttl.remove(oid.0);
    live.lru.remove(slot, oid);
}

/// Record whether `oid`'s key has a TTL. Ignored for an oid with no live file.
fn set_ttl(oid: ObjectId, slot: u32, has_ttl: bool) {
    let mut live = live();
    if !live.all.contains(oid.0) {
        return;
    }
    if has_ttl {
        live.ttl.insert(oid.0);
    } else {
        live.ttl.remove(oid.0);
    }
    live.lru.set_ttl(slot, oid, has_ttl);
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
    let (oid, file_slot) = match ctx.open_key(&key).get_value::<LoValue>(&LO_TYPE) {
        Ok(Some(lo)) => (lo.object_id, lo.file.as_ref().map(|file| file.lru_slot())),
        _ => return,
    };
    let has_ttl = matches!(ctx.call("PTTL", &[&key]), Ok(ValkeyValue::Integer(ms)) if ms >= 0);
    match (crate::operating_mode(), file_slot) {
        (crate::OperatingMode::Tiered, Some(slot)) => set_ttl(oid, slot, has_ttl),
        (crate::OperatingMode::Dram, _) => super::get_dram_pool().set_ttl(&oid, has_ttl),
        (crate::OperatingMode::Tiered, None) => {}
    }
}

// ─── Candidates ──────────────────────────────────────────────────────────────

/// Up to `n` distinct eviction candidates among the objects the policy allows: the
/// least recently used under `*-lru`, else random.
pub fn candidates(policy: MaxmemoryPolicy, n: usize) -> Vec<ObjectId> {
    let mut live = live();
    if policy.lru {
        let Live { all, lru, .. } = &mut *live;
        return lru
            .oldest(policy.volatile, n, |oid| {
                if all.contains(oid.0) {
                    Pick::Take
                } else {
                    Pick::Unlink
                }
            })
            .into_iter()
            .map(|(_, oid)| oid)
            .collect();
    }
    let bitmap = if policy.volatile {
        &live.ttl
    } else {
        &live.all
    };
    let len = bitmap.len();
    let mut oids: Vec<u64> = if len <= n as u64 {
        bitmap.iter().collect()
    } else {
        let mut rng = rand::rng();
        (0..n)
            .filter_map(|_| bitmap.select(rng.random_range(0..len)))
            .collect()
    };
    oids.sort_unstable();
    oids.dedup();
    shuffle(&mut oids);
    oids.into_iter().map(ObjectId).collect()
}

fn shuffle<T>(items: &mut [T]) {
    let mut rng = rand::rng();
    for i in (1..items.len()).rev() {
        items.swap(i, rng.random_range(0..=i));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // LIVE is global: each test uses its own oid range and only looks at it.
    fn add(oid: u64, ttl: bool) -> u32 {
        let slot = insert(ObjectId(oid));
        set_ttl(ObjectId(oid), slot, ttl);
        slot
    }

    fn pick(volatile: bool, lru: bool, range: std::ops::Range<u64>) -> Vec<u64> {
        candidates(
            MaxmemoryPolicy {
                volatile,
                noeviction: false,
                lru,
            },
            usize::MAX,
        )
        .into_iter()
        .map(|oid| oid.0)
        .filter(|oid| range.contains(oid))
        .collect()
    }

    #[test]
    fn volatile_picks_only_ttl_keys() {
        for oid in 1_000..1_010 {
            add(oid, oid % 2 == 0);
        }
        for lru in [false, true] {
            let picked = pick(true, lru, 1_000..1_010);
            assert_eq!(picked.len(), 5);
            assert!(picked.iter().all(|oid| oid % 2 == 0), "{picked:?}");
            assert_eq!(pick(false, lru, 1_000..1_010).len(), 10);
        }
    }

    #[test]
    fn lru_picks_the_least_recently_used_first() {
        let slots: Vec<u32> = (2_000..2_003).map(|oid| add(oid, false)).collect();
        touch(slots[0], ObjectId(2_000));
        assert_eq!(pick(false, true, 2_000..2_003), vec![2_001, 2_002, 2_000]);
        // An evicted oid is never picked again, even before its file drops.
        evicted(ObjectId(2_001));
        assert_eq!(pick(false, true, 2_000..2_003), vec![2_002, 2_000]);
        remove(ObjectId(2_001), slots[1]);
        assert_eq!(pick(false, true, 2_000..2_003), vec![2_002, 2_000]);
    }

    #[test]
    fn remove_clears_both_bitmaps_and_ttl_needs_a_live_file() {
        let slot = add(4_000, true);
        remove(ObjectId(4_000), slot);
        for lru in [false, true] {
            assert!(pick(false, lru, 4_000..4_001).is_empty());
            assert!(pick(true, lru, 4_000..4_001).is_empty());
        }
        set_ttl(ObjectId(4_000), slot, true);
        assert!(pick(true, false, 4_000..4_001).is_empty());
    }
}

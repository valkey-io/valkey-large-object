//! Live object index (Tiered): which oids have a file, and which of those have a TTL.
//! Eviction samples it instead of reading `disk-dir`.
//!
//! The oid is a dense integer, so it is the bit position in both bitmaps.
//! `select(k)` on a bitmap returns its k-th set bit, a uniformly random live oid.

use std::sync::{LazyLock, Mutex};

use rand::RngExt;
use roaring::RoaringTreemap;
use valkey_module::{Context, NotifyEvent, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};

#[derive(Default)]
struct Live {
    /// Every oid with a live file.
    all: RoaringTreemap,
    /// The oids in `all` whose key has a TTL. Always a subset of `all`.
    ttl: RoaringTreemap,
}

static LIVE: LazyLock<Mutex<Live>> = LazyLock::new(Mutex::default);

fn live() -> std::sync::MutexGuard<'static, Live> {
    LIVE.lock().expect("LIVE lock unavailable")
}

/// A new object file exists.
pub fn insert(oid: ObjectId) {
    live().all.insert(oid.0);
}

/// The object is gone or evicted. Clears both bitmaps.
pub fn remove(oid: ObjectId) {
    let mut live = live();
    live.all.remove(oid.0);
    live.ttl.remove(oid.0);
}

/// Record whether `oid`'s key has a TTL. Ignored for an oid with no live file.
fn set_ttl(oid: ObjectId, has_ttl: bool) {
    let mut live = live();
    if !live.all.contains(oid.0) {
        return;
    }
    if has_ttl {
        live.ttl.insert(oid.0);
    } else {
        live.ttl.remove(oid.0);
    }
}

/// Keyspace events that can change a key's TTL without changing its oid. An overwrite
/// (BLOB.TCP_SET / BLOB.RDMA_SET) needs none: it mints a new oid, and Valkey drops the key's TTL.
pub fn on_keyspace_event(ctx: &Context, _: NotifyEvent, event: &str, key: &[u8]) {
    if !matches!(event, "expire" | "persist" | "copy_to")
        || crate::operating_mode() != crate::OperatingMode::Tiered
    {
        return;
    }
    let key = ctx.create_string(key);
    let oid = match ctx.open_key(&key).get_value::<LoValue>(&LO_TYPE) {
        Ok(Some(LoValue {
            file: Some(file), ..
        })) => file.object_id(),
        _ => return,
    };
    let has_ttl = matches!(ctx.call("PTTL", &[&key]), Ok(ValkeyValue::Integer(ms)) if ms >= 0);
    set_ttl(oid, has_ttl);
}

// ─── Policy ──────────────────────────────────────────────────────────────────

/// `maxmemory-policy`, as eviction uses it: which bitmap to sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    volatile: bool,
}

impl Policy {
    fn parse(name: &str) -> Self {
        Self {
            volatile: name.starts_with("volatile-"),
        }
    }

    /// The server's `maxmemory-policy`. Call under the server lock.
    pub fn read(ctx: &Context) -> Self {
        let info = ctx.server_info("memory");
        Self::parse(
            &info
                .field("maxmemory_policy")
                .map(|v| v.to_string())
                .unwrap_or_default(),
        )
    }
}

// ─── Sampling ────────────────────────────────────────────────────────────────

/// Up to `n` distinct random eviction candidates from the policy's bitmap.
pub fn sample(policy: Policy, n: usize) -> Vec<ObjectId> {
    let live = live();
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
    fn add(oid: u64, ttl: bool) {
        insert(ObjectId(oid));
        set_ttl(ObjectId(oid), ttl);
    }

    fn pick(policy: &str, range: std::ops::Range<u64>) -> Vec<u64> {
        sample(Policy::parse(policy), 1000)
            .into_iter()
            .map(|oid| oid.0)
            .filter(|oid| range.contains(oid))
            .collect()
    }

    #[test]
    fn parse_policies() {
        for name in [
            "volatile-lru",
            "volatile-lfu",
            "volatile-random",
            "volatile-ttl",
        ] {
            assert!(Policy::parse(name).volatile, "{name}");
        }
        for name in ["allkeys-lru", "allkeys-lfu", "allkeys-random", ""] {
            assert!(!Policy::parse(name).volatile, "{name}");
        }
    }

    #[test]
    fn volatile_samples_only_ttl_keys() {
        for oid in 1_000..1_010 {
            add(oid, oid % 2 == 0);
        }
        let picked = pick("volatile-random", 1_000..1_010);
        assert_eq!(picked.len(), 5);
        assert!(picked.iter().all(|oid| oid % 2 == 0), "{picked:?}");
        assert_eq!(pick("allkeys-random", 1_000..1_010).len(), 10);
    }

    #[test]
    fn remove_clears_both_bitmaps_and_ttl_needs_a_live_file() {
        add(4_000, true);
        remove(ObjectId(4_000));
        assert!(pick("allkeys-random", 4_000..4_001).is_empty());
        assert!(pick("volatile-random", 4_000..4_001).is_empty());
        set_ttl(ObjectId(4_000), true);
        assert!(pick("volatile-random", 4_000..4_001).is_empty());
    }
}

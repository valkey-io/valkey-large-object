//! A hash map split into `SHARDS` maps, for maps that hold an entry per object.
//!
//! A hash map grows by rehashing all its entries at once. With one entry per object, at
//! 100M objects that held a lock for seconds and stalled the server. A shard holds
//! 1/`SHARDS` of the entries, so one rehash is that much shorter.
//!
//! The shard comes from a quality hash of the key. Oids are a counter: `oid % SHARDS`,
//! or a fast hash, spreads consecutive oids so evenly that the shards fill in lockstep and
//! all rehash within the same few thousand inserts. A quality hash makes shard sizes vary,
//! so their rehashes spread over millions of inserts.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};

const SHARDS: usize = 1024;

/// foldhash: the hasher hashbrown and hashlink default to. Keys are never chosen by a
/// client, so the maps need no protection against crafted collisions.
type Shard<K, V> = HashMap<K, V, foldhash::fast::RandomState>;

pub(crate) struct ShardedMap<K, V> {
    shards: Box<[Shard<K, V>]>,
    pick: foldhash::quality::RandomState,
}

impl<K, V> Default for ShardedMap<K, V> {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Shard::default()).collect(),
            pick: Default::default(),
        }
    }
}

impl<K: Hash + Eq, V> ShardedMap<K, V> {
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.shard_mut(&key).insert(key, value)
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        self.shard_mut(key).remove(key)
    }

    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        self.shard(key).get(key)
    }

    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.shard_mut(key).get_mut(key)
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.shard(key).contains_key(key)
    }

    /// Sums the shards: for INFO and tests, not a hot path.
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(HashMap::len).sum()
    }

    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.shards.iter().flatten()
    }

    fn shard(&self, key: &K) -> &Shard<K, V> {
        &self.shards[self.index(key)]
    }

    fn shard_mut(&mut self, key: &K) -> &mut Shard<K, V> {
        let i = self.index(key);
        &mut self.shards[i]
    }

    fn index(&self, key: &K) -> usize {
        self.pick.hash_one(key) as usize % SHARDS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn behaves_as_one_map_across_shards() {
        let mut map = ShardedMap::default();
        for k in 0..10_000u64 {
            assert_eq!(map.insert(k, k * 2), None);
        }
        assert_eq!(
            map.insert(7, 0),
            Some(14),
            "an existing key takes the new value"
        );
        assert_eq!(map.len(), 10_000);
        assert_eq!(map.get(&7), Some(&0));
        *map.get_mut(&8).unwrap() += 1;
        assert_eq!(map.get(&8), Some(&17));
        for k in (0..10_000u64).step_by(2) {
            assert!(map.remove(&k).is_some());
        }
        assert_eq!(map.remove(&0), None);
        assert!(!map.contains_key(&2) && map.contains_key(&3));
        assert_eq!(map.len(), 5_000);
        let mut keys: Vec<u64> = map.iter().map(|(&k, _)| k).collect();
        keys.sort_unstable();
        assert!(keys.iter().copied().eq((1..10_000).step_by(2)));
    }

    #[test]
    fn spreads_counter_keys_over_every_shard_unevenly() {
        let mut map = ShardedMap::default();
        for k in 0..(SHARDS as u64 * 100) {
            map.insert(k, ());
        }
        let sizes: Vec<usize> = map.shards.iter().map(HashMap::len).collect();
        assert!(sizes.iter().all(|&n| n > 0), "every shard is used");
        // Not lockstep: `oid % SHARDS` would give every shard exactly 100.
        assert!(sizes.iter().any(|&n| n != 100));
    }
}

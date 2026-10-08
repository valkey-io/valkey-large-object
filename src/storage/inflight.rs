//! Objects with a request in flight, which eviction must not take.
//!
//! Eviction picks victims from the NVMe directory, so it sees files, not keys. A file with no live
//! key behind it, or one a request is still using, is not its to take. Every command pins the
//! object ids it touches as soon as it knows them and holds the pin until it is done:
//! - SET and COPY: the new id from the moment it is minted until the key commits, and an
//!   overwritten or copied-from id until the command ends;
//! - GET: the id until the reply or transfer completes;
//! - a freed key (`lo_free`): its file, until it is unlinked;
//! - an eviction victim: its file, from the claim until it is unlinked.
//!
//! Lock order: `RECLAIM_LIST`, then this map.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard};

use crate::data_type::ObjectId;

pub static INFLIGHT: LazyLock<Inflight> = LazyLock::new(Inflight::default);

/// Object id -> number of pins on it.
#[derive(Default)]
pub struct Inflight(Mutex<HashMap<ObjectId, u32>>);

impl Inflight {
    pub fn lock(&self) -> MutexGuard<'_, HashMap<ObjectId, u32>> {
        self.0.lock().expect("INFLIGHT lock unavailable")
    }
}

/// One pin on an object id, released on drop. `std::mem::forget` it to pin the id for good.
#[derive(Debug)]
pub struct InflightGuard(ObjectId);

impl InflightGuard {
    pub fn new(oid: ObjectId) -> Self {
        *INFLIGHT.lock().entry(oid).or_default() += 1;
        Self(oid)
    }

    /// Pin `oid` unless something has it pinned already.
    pub fn new_if_unpinned(oid: ObjectId) -> Option<Self> {
        let mut pins = INFLIGHT.lock();
        if pins.contains_key(&oid) {
            return None;
        }
        pins.insert(oid, 1);
        Some(Self(oid))
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let mut pins = INFLIGHT.lock();
        if let Some(count) = pins.get_mut(&self.0) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_stays_pinned_until_its_last_guard_drops() {
        let id = ObjectId(0x91a_0001);
        assert!(!INFLIGHT.lock().contains_key(&id));
        let first = InflightGuard::new(id);
        let second = InflightGuard::new(id);
        assert!(InflightGuard::new_if_unpinned(id).is_none());
        drop(first);
        assert!(INFLIGHT.lock().contains_key(&id));
        drop(second);
        assert!(!INFLIGHT.lock().contains_key(&id));

        let claim = InflightGuard::new_if_unpinned(id).expect("unpinned");
        assert_eq!(INFLIGHT.lock().get(&id), Some(&1));
        drop(claim);
        assert!(!INFLIGHT.lock().contains_key(&id));
    }
}

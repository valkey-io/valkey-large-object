//! Data Type Layer — LoValue stored in Valkey keyspace.
//!
//! The data type layer owns:
//! - LoValue struct in keyspace (accessed via ValkeyModule_OpenKey)
//! - OID generation (monotonic counter)
//! - RDB callbacks (save/load references) (TODO)
//! - Replication (TODO)
//! - Native Valkey DEL triggers free callback → deletes NVMe file.
//! - Callbacks: MEMORY USAGE, FREE EFFORT, COPY, DEBUG DIGEST.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use valkey_module::digest::Digest;
use valkey_module::native_types::ValkeyType;
use valkey_module::raw;

use crate::storage::{Crc, ObjectFile};

// ─── ObjectId ────────────────────────────────────────────────────────────────

/// ObjectId IS the file path: deterministic mapping OID → "{nvme_dir}/{oid:016x}.dat"
/// No lookup table. Compact u64 safe for replication streams, RDB, and LoValue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

/// Monotonic OID counter.
/// TODO: Not yet unique per node. Currently a plain counter starting at 1.
/// Two nodes in a cluster will generate colliding OIDs.
/// Fix: hash VM_GetMyClusterID() to 16 bits, OR into top bits of counter.
/// Requires raw FFI call (no safe wrapper in valkey-module crate yet).
static OID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ObjectId {
    pub fn next() -> Self {
        Self(OID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Deterministic file path from OID.
    pub fn file_path(&self, nvme_dir: &str) -> String {
        format!("{}/{:016x}.dat", nvme_dir, self.0)
    }
}

// ─── Tier ────────────────────────────────────────────────────────────────────

/// Storage tier an object is currently served from. Reported by `BLOB.INFO`.
pub enum Tier {
    /// Resident in DRAMPool (Dram mode always; Tiered mode when promoted).
    Dram,
    /// On NVMe only, not cached in DRAMPool (Tiered mode).
    Nvme,
}

impl Tier {
    /// Lowercase token used in BLOB.INFO replies.
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Dram => "dram",
            Tier::Nvme => "nvme",
        }
    }
}

// ─── LoValue ─────────────────────────────────────────────────────────────────

/// LoValue — the Valkey data type value struct, stored in Valkey's keyspace.
/// Accessed via ValkeyModule_OpenKey → ModuleTypeGetValue on the main thread.
/// This is NOT in the storage layer. Command handlers read this to get file info
/// before calling storage.
#[derive(Debug, Clone)]
pub struct LoValue {
    pub object_id: ObjectId, // monotonic per-node OID (used as filename)
    pub len: u64,            // object size in bytes
    pub crc32c: Crc,         // integrity checksum (verified on replication pull)
    /// The object's `ObjectFile` handle (Tiered mode only; `None` in Dram mode).
    /// Tracks the object's on-disk existence and may hold an open read fd behind
    /// an `Arc`. Dropping the last ref closes the fd and unlinks the file.
    /// Never serialized or reconstructed on load.
    pub file: Option<Arc<ObjectFile>>,
}

// ─── LoValue Helper Methods ──────────────────────────────────────────────────

impl LoValue {
    /// Reports DRAM memory usage in bytes for `MEMORY USAGE <key>`.
    /// Always includes the LoValue struct overhead. Includes the object payload
    /// only when it is actually resident in DRAM:
    /// - DRAM-only mode: always (object lives exclusively in DRAM).
    /// - Tiered mode: only if the object has been promoted into DRAMPool.
    pub fn memory_usage(&self) -> usize {
        let base = std::mem::size_of::<LoValue>();
        let dram_usage = base + self.len as usize;
        match crate::operating_mode() {
            crate::OperatingMode::Dram => dram_usage,
            crate::OperatingMode::Tiered => {
                if crate::storage::get_dram_pool().contains_object(&self.object_id) {
                    dram_usage
                } else {
                    base
                }
            }
        }
    }

    /// Where a GET issued right now would be served from:
    /// - Dram mode: always `Dram` (objects live nowhere else).
    /// - Tiered mode: `Dram` if a Ready ObjectContext is cached in DRAMPool, else
    ///   `Nvme`.
    pub fn tier(&self) -> Tier {
        match crate::operating_mode() {
            crate::OperatingMode::Dram => Tier::Dram,
            crate::OperatingMode::Tiered => {
                match crate::storage::get_dram_pool().get_object(&self.object_id) {
                    Some(ctx) if ctx.is_ready() => Tier::Dram,
                    _ => Tier::Nvme,
                }
            }
        }
    }

    /// Returns 0 to signal Valkey to ALWAYS free asynchronously (BIO thread).
    ///
    /// Per the Module API contract: returning 0 guarantees async free.
    pub fn free_effort(&self) -> usize {
        0
    }

    /// Deep-copy for the COPY command callback.
    /// Dram mode: clone ObjectContext via try_clone. Tiered mode: copy NVMe file.
    /// Returns None on capacity exhaustion (pool full or nvme-maxmemory exceeded).
    pub fn create_copy(&self) -> Option<LoValue> {
        match crate::operating_mode() {
            crate::OperatingMode::Dram => self.create_copy_dram(),
            crate::OperatingMode::Tiered => self.create_copy_tiered(),
        }
    }

    /// Dram mode: deep-copy ObjectContext via try_clone, insert with new OID.
    fn create_copy_dram(&self) -> Option<LoValue> {
        use crate::storage::TryClone;
        let dram_pool = crate::storage::get_dram_pool();
        let src_ctx = dram_pool
            .get_object(&self.object_id)
            .expect("Dram COPY: LoValue exists but ObjectContext missing");
        // Returns None if object is Filling (incomplete) or pool is full.
        let new_ctx = src_ctx.try_clone()?;
        let new_oid = ObjectId::next();
        dram_pool.insert_object(new_oid, std::sync::Arc::new(new_ctx));
        Some(LoValue {
            object_id: new_oid,
            len: self.len,
            crc32c: self.crc32c,
            file: None,
        })
    }

    /// Tiered mode: copy NVMe file with a fresh OID.
    /// Operates at the NVMe level only — DRAMPool promotion is per-key and not carried over.
    /// Returns None if nvme-maxmemory would be exceeded or the copy fails.
    fn create_copy_tiered(&self) -> Option<LoValue> {
        let file = self.file.as_ref()?.copy(self.len, self.crc32c)?;
        Some(LoValue {
            object_id: file.object_id(),
            len: self.len,
            crc32c: self.crc32c,
            file: Some(Arc::new(file)),
        })
    }
}

// ─── Callbacks ───────────────────────────────────────────────────────────────

/// Free callback — triggered by native Valkey DEL, overwrite, expiry, eviction, flush.
///
/// Concurrency-safe via refcounted teardown: this callback drops the DRAM `ObjectContext`
/// and `LoValue` references inline. The respective `Arc<ObjectFile>` drop tears down the
/// file and fd when it is thread safe. Likewise, removing the `ObjectContext` from the
/// DRAMPool drops the map's ref, and `ObjectContext::Drop` returns its buffers to
/// the arena once the last reader drops it.
unsafe extern "C" fn lo_free(value: *mut std::ffi::c_void) {
    let lo = Box::from_raw(value as *mut LoValue);
    // Drop the DRAM cache entry.
    crate::storage::get_dram_pool().remove_object(&lo.object_id);
    // `lo` (and its Option<Arc<ObjectFile>>) drops here; teardown fires on last ref.
}

/// MEMORY USAGE callback.
/// Reports actual DRAM consumption: struct overhead + payload when resident in DRAM.
unsafe extern "C" fn lo_mem_usage(value: *const std::ffi::c_void) -> usize {
    let val = &*(value as *const LoValue);
    val.memory_usage()
}

/// FREE EFFORT callback.
/// Always returns 0 to force asynchronous free via BIO thread.
/// This keeps unlink(2) off the main event-loop thread.
/// See LoValue::free_effort() for full rationale.
unsafe extern "C" fn lo_free_effort(
    _key: *mut raw::RedisModuleString,
    value: *const std::ffi::c_void,
) -> usize {
    let val = &*(value as *const LoValue);
    val.free_effort()
}

/// COPY callback.
/// Deep-copies the object based on operating mode. Returns null on failure.
unsafe extern "C" fn lo_copy(
    _from_key: *mut raw::RedisModuleString,
    _to_key: *mut raw::RedisModuleString,
    value: *const std::ffi::c_void,
) -> *mut std::ffi::c_void {
    let src = &*(value as *const LoValue);
    match src.create_copy() {
        Some(new_val) => Box::into_raw(Box::new(new_val)) as *mut std::ffi::c_void,
        None => std::ptr::null_mut(),
    }
}

/// DEBUG DIGEST callback.
/// Feeds object_id, len, and crc32c into the digest for integrity verification.
unsafe extern "C" fn lo_digest(md: *mut raw::RedisModuleDigest, value: *mut std::ffi::c_void) {
    let mut dig = Digest::new(md);
    let val = &*(value as *const LoValue);
    dig.add_long_long(val.object_id.0 as i64);
    dig.add_long_long(val.len as i64);
    dig.add_long_long(val.crc32c as i64);
    dig.end_sequence();
}

// ─── Type Registration ───────────────────────────────────────────────────────

pub static LO_TYPE: ValkeyType = ValkeyType::new(
    "largeob-k", // 9 char type name
    0,           // encoding version
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: None,    // TODO
        rdb_save: None,    // TODO
        aof_rewrite: None, // TODO
        free: Some(lo_free),
        mem_usage: Some(lo_mem_usage),
        digest: Some(lo_digest),
        aux_load: None, // TODO
        aux_save: None, // TODO
        aux_save2: None,
        aux_save_triggers: 0,
        free_effort: Some(lo_free_effort),
        unlink: None,
        copy: Some(lo_copy),
        defrag: None, // TODO
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_monotonic() {
        let a = ObjectId::next();
        let b = ObjectId::next();
        let c = ObjectId::next();
        assert!(b.0 > a.0);
        assert!(c.0 > b.0);
    }

    #[test]
    fn test_oid_file_path_format() {
        let oid = ObjectId(0xff);
        assert_eq!(oid.file_path("/data"), "/data/00000000000000ff.dat");

        let oid2 = ObjectId(1);
        assert_eq!(
            oid2.file_path("/mnt/bigobj"),
            "/mnt/bigobj/0000000000000001.dat"
        );
    }

    // ─── free_effort tests ───────────────────────────────────────────────

    #[test]
    fn test_free_effort_always_zero() {
        // free_effort always returns 0 (async free) regardless of object size.
        let small = LoValue {
            object_id: ObjectId(1),
            len: 512,
            crc32c: 0,
            file: None,
        };
        assert_eq!(small.free_effort(), 0);

        let large = LoValue {
            object_id: ObjectId(2),
            len: 100 * 1024 * 1024,
            crc32c: 0,
            file: None,
        };
        assert_eq!(large.free_effort(), 0);

        let zero = LoValue {
            object_id: ObjectId(3),
            len: 0,
            crc32c: 0,
            file: None,
        };
        assert_eq!(zero.free_effort(), 0);
    }
}

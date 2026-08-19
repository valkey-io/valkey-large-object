//! Data Type Layer — LoValue stored in Valkey keyspace.
//!
//! The data type layer owns:
//! - LoValue struct in keyspace (accessed via ValkeyModule_OpenKey)
//! - OID generation (monotonic counter)
//! - RDB callbacks (save/load references) (TODO)
//! - TIERING.REF replication (TODO)
//! - Native Valkey DEL triggers free callback → deletes NVMe file.

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::native_types::ValkeyType;
use valkey_module::raw;

// ─── ObjectId ────────────────────────────────────────────────────────────────

/// ObjectId IS the file path: deterministic mapping OID → "{data_dir}/{oid:016x}.dat"
/// No lookup table. Compact u64 safe for replication streams, RDB, and LoValue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub fn file_path(&self, data_dir: &str) -> String {
        format!("{}/{:016x}.dat", data_dir, self.0)
    }

    /// Initialize OID counter to at least `val`.
    /// Used at startup after scanning data_dir for existing .dat files.
    /// Ensures new OIDs never collide with on-disk objects surviving a restart.
    pub fn init_counter(val: u64) {
        OID_COUNTER.fetch_max(val, Ordering::Relaxed);
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
    pub crc32c: u32,         // integrity checksum (verified on replication pull)
}

/// Free callback — triggered by native Valkey DEL.
/// Deletes the NVMe file for this object.
unsafe extern "C" fn lo_free(value: *mut std::ffi::c_void) {
    // SAFETY: value is a valid LoValue pointer that we previously returned from
    // rdb_load or set_value. We take ownership back and drop it after deleting the file.
    let lo = Box::from_raw(value as *mut LoValue);
    // Delete NVMe file via storage layer.
    crate::storage::delete(lo.object_id);
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
        mem_usage: None, // TODO
        digest: None,    // TODO
        aux_load: None,  // TODO
        aux_save: None,  // TODO
        aux_save2: None,
        aux_save_triggers: 0,
        free_effort: None, // TODO
        unlink: None,
        copy: None,   // TODO
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

    #[test]
    fn test_oid_init_counter() {
        // init_counter sets the counter to at least the given value.
        // Subsequent next() calls must return values above it.
        ObjectId::init_counter(1_000_000);
        let oid = ObjectId::next();
        assert!(
            oid.0 >= 1_000_000,
            "OID {} should be >= 1000000 after init_counter",
            oid.0
        );
    }
}

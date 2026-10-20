//! Centralized error strings for the module.
//!
//! All user-facing error messages live here via the define_errors! macro.
//! Adding a new error automatically includes it in the unit test.

macro_rules! define_errors {
    ($($name:ident => $msg:expr),+ $(,)?) => {
        $(pub const $name: &str = $msg;)+

        #[cfg(test)]
        pub(crate) const ALL_ERRORS: &[&str] = &[$($name),+];
    };
}

// ─── Error Definitions ───────────────────────────────────────────────────────
//
// Config validation errors include the "largeobj." module prefix so users know
// the full CONFIG SET name (e.g. `CONFIG SET largeobj.disk-maxmemory`).
// 'maxmemory' is a core Valkey config and has no module prefix.

define_errors! {
    // Command Errors
    ERR_RDMA_UNAVAILABLE => "ERR RDMA provider unavailable on this instance",
    ERR_INVALID_RDMA_ADDR_HEX => "ERR invalid rdma address hex",
    ERR_RDMA_ADDR_LEN => "ERR rdma address must be 32 bytes",
    ERR_RDMA_ADDR_EMPTY => "ERR rdma address must not be empty",
    ERR_MALFORMED_MEMORY_ADDR_ARGS => "ERR memory address args must be rkey, addr, len triples",
    ERR_TOO_MANY_MEMORY_ADDRESSES => "ERR too many memory addresses (max 256)",
    ERR_INVALID_RKEY => "ERR invalid rkey",
    ERR_INVALID_MEMORY_ADDR => "ERR invalid memory address",
    ERR_INVALID_MEMORY_ADDR_LEN => "ERR invalid memory address len",
    ERR_INVALID_LEN => "ERR invalid len",
    ERR_INVALID_CRC => "ERR invalid CRC value",
    ERR_MAX_OBJECT_SIZE_EXCEEDED => "ERR max object size exceeded",
    ERR_NO_RDMA_SESSION => "ERR no RDMA session (call BLOB.RDMA_HELLO first)",
    ERR_RDMA_SESSION_EXISTS => "ERR RDMA session already established",
    ERR_INVALID_INFO_FIELD => "ERR invalid information field",
    ERR_MODULE_DISABLED => "DISABLED largeobj module is disabled",

    // OOM — matches the standard Valkey OOM error prefix so clients can
    // distinguish memory exhaustion from other errors programmatically.
    // The INFO metric counters (dram_pool_exhausted, disk_staging_buffer_exhausted,
    // disk_capacity_exceeded) tell the operator which resource was exhausted.
    ERR_OOM => "OOM command not allowed when used memory > 'maxmemory'.",
    ERR_OOM_DISK => "OOM command not allowed when used disk space > 'largeobj.disk-maxmemory'.",

    // Storage/Engine Errors
    ERR_DISK_READ => "ERR disk read failed",
    ERR_DISK_WRITE => "ERR disk write failed",
    ERR_RDMA_WRITE => "ERR RDMA write failed",
    ERR_RDMA_READ => "ERR RDMA read failed",
    ERR_SESSION_CREATE => "ERR session create",
    ERR_CRC_MISMATCH => "ERR CRC mismatch after transfer",

    // Streaming Errors
    ERR_DISK_STAGING_EXHAUSTED => "ERR disk staging buffer pool exhausted",
    ERR_SET_VALUE => "ERR failed to set key",

    // Config Validation Errors
    ERR_ZERO_LENGTH_OBJECT => "ERR object length must be > 0",
    ERR_NVME_GE_MAX_OBJ => "ERR largeobj.disk-maxmemory must be >= largeobj.max-object-size in Tiered mode",
    ERR_SEGMENT_GE_MAX_OBJ => "ERR largeobj.segment-size is insufficient for largeobj.max-object-size",
    ERR_SEGMENT_GE_PROMOTE => "ERR largeobj.segment-size is insufficient for largeobj.max-promote-size",
    ERR_STAGING_GE_SEGMENT => "ERR largeobj.disk-staging-size must be >= largeobj.segment-size",
    ERR_SEGMENT_GE_CHUNK => "ERR largeobj.segment-size must be >= largeobj.chunk-size",
    ERR_MAX_BUF_GE_MIN_BUF => "ERR largeobj.max-buffers-per-op must be >= largeobj.min-buffers-per-op",
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_errors_fit_in_128_bytes() {
        for err in ALL_ERRORS {
            assert!(
                err.len() <= 128,
                "Error string exceeds 128 bytes ({} bytes): {:?}",
                err.len(),
                err
            );
        }
    }
}

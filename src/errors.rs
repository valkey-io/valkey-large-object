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

define_errors! {
    // Command Errors
    ERR_EFA_UNAVAILABLE => "ERR EFA unavailable on this instance",
    ERR_INVALID_PEER_ADDR_HEX => "ERR invalid peer address hex",
    ERR_PEER_ADDR_LEN => "ERR peer address must be 32 bytes",
    ERR_PEER_ADDR_EMPTY => "ERR peer address must not be empty",
    ERR_MALFORMED_ADDR_ARGS => "ERR address args must be rkey, addr, len triples",
    ERR_TOO_MANY_ADDRESSES => "ERR too many addresses (max 256)",
    ERR_INVALID_RKEY => "ERR invalid rkey",
    ERR_INVALID_REMOTE_ADDR => "ERR invalid remote_addr",
    ERR_INVALID_ADDR_LEN => "ERR invalid addr len",
    ERR_INSUFFICIENT_ADDR_SPACE => "ERR client address space smaller than object length",
    ERR_INVALID_LEN => "ERR invalid len",
    ERR_MAX_OBJECT_SIZE_EXCEEDED => "ERR max object size exceeded",
    ERR_NO_DMA_SESSION => "ERR no DMA session (call BLOB.HELLO first)",
    ERR_DMA_SESSION_EXISTS => "ERR DMA session already established (one BLOB.HELLO per connection)",
    ERR_DRAM_POOL_EXHAUSTED => "ERR DRAM buffer pool exhausted",
    ERR_NOT_FOUND => "ERR not found",
    ERR_INVALID_INFO_FIELD => "ERR invalid information value",

    // Storage/Engine Errors
    ERR_NVME_READ => "ERR NVMe read",
    ERR_NVME_WRITE => "ERR NVMe write",
    ERR_EFA_WRITE => "ERR EFA write",
    ERR_EFA_READ => "ERR EFA read",
    ERR_SESSION_CREATE => "ERR session create",

    // Streaming Errors
    ERR_INSUFFICIENT_NVME_BUFFERS => "ERR NVMe staging buffer pool exhausted",
    ERR_NVME_CAPACITY_EXCEEDED => "ERR NVMe disk capacity exceeded",
    ERR_SET_VALUE => "ERR failed to set key",

    // Config Validation Errors
    ERR_ZERO_LENGTH_OBJECT => "ERR object length must be > 0",
    ERR_NVME_GE_MAX_OBJ => "ERR nvme-maxmemory must be >= max-object-size in Tiered mode",
    ERR_SEGMENT_GE_MAX_OBJ => "ERR segment-size is insufficient for max-object-size",
    ERR_SEGMENT_GE_PROMOTE => "ERR segment-size is insufficient for max-promote-size",
    ERR_STAGING_GE_SEGMENT => "ERR nvme-staging-size must be >= segment-size",
    ERR_SEGMENT_GE_CHUNK => "ERR segment-size must be >= chunk-size",
    ERR_MAX_BUF_GE_MIN_BUF => "ERR max-buffers-per-op must be >= min-buffers-per-op",
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_errors_have_err_prefix() {
        for err in ALL_ERRORS {
            assert!(
                err.starts_with("ERR "),
                "Error string missing 'ERR ' prefix: {:?}",
                err
            );
        }
    }
}

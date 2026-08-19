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
    ERR_INVALID_NUM_REGIONS => "ERR invalid num_regions",
    ERR_INSUFFICIENT_REGION_ARGS => "ERR insufficient region args",
    ERR_INVALID_RKEY => "ERR invalid rkey",
    ERR_INVALID_REMOTE_ADDR => "ERR invalid remote_addr",
    ERR_INVALID_REGION_LEN => "ERR invalid region len",
    ERR_INVALID_REGION_IDX => "ERR invalid region_idx",
    ERR_INVALID_REMOTE_OFFSET => "ERR invalid remote_offset",
    ERR_INVALID_LEN => "ERR invalid len",
    ERR_OBJECT_EXCEEDS_BUF => "ERR object exceeds buffer size",
    ERR_NO_DMA_SESSION => "ERR no DMA session (call LO.HELLO first)",
    ERR_POOL_EXHAUSTED => "ERR pool exhausted",
    ERR_SESSION_GONE => "ERR session gone",

    // Storage/Engine Errors
    ERR_NVME_READ => "ERR NVMe read",
    ERR_NVME_WRITE => "ERR NVMe write",
    ERR_EFA_WRITE => "ERR EFA write",
    ERR_EFA_READ => "ERR EFA read",
    ERR_SESSION_CREATE => "ERR session create",
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

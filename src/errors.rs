//! Centralized error strings for the module.
//!
//! All user-facing error messages live here as pub const &str.
//! Ensures consistency and makes i18n/auditing trivial.

// ─── Command Errors ──────────────────────────────────────────────────────────

pub const ERR_EFA_UNAVAILABLE: &str = "ERR EFA unavailable on this instance";
pub const ERR_INVALID_PEER_ADDR_HEX: &str = "ERR invalid peer address hex";
pub const ERR_PEER_ADDR_LEN: &str = "ERR peer address must be 32 bytes";
pub const ERR_INVALID_NUM_REGIONS: &str = "ERR invalid num_regions";
pub const ERR_INSUFFICIENT_REGION_ARGS: &str = "ERR insufficient region args";
pub const ERR_INVALID_RKEY: &str = "ERR invalid rkey";
pub const ERR_INVALID_REMOTE_ADDR: &str = "ERR invalid remote_addr";
pub const ERR_INVALID_REGION_LEN: &str = "ERR invalid region len";
pub const ERR_INVALID_REGION_IDX: &str = "ERR invalid region_idx";
pub const ERR_INVALID_REMOTE_OFFSET: &str = "ERR invalid remote_offset";
pub const ERR_INVALID_LEN: &str = "ERR invalid len";
pub const ERR_OBJECT_EXCEEDS_BUF: &str = "ERR object exceeds buffer size";
pub const ERR_NO_DMA_SESSION: &str = "ERR no DMA session (call DMA.HELLO first)";
pub const ERR_POOL_EXHAUSTED: &str = "ERR pool exhausted";
pub const ERR_SESSION_GONE: &str = "ERR session gone";

// ─── Storage/Engine Errors ───────────────────────────────────────────────────

pub const ERR_NVME_READ: &str = "ERR NVMe read";
pub const ERR_NVME_WRITE: &str = "ERR NVMe write";
pub const ERR_EFA_WRITE: &str = "ERR EFA write";
pub const ERR_EFA_READ: &str = "ERR EFA read";
pub const ERR_SESSION_CREATE: &str = "ERR session create";

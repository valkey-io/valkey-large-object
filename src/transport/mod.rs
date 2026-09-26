//! The RDMA transport: libfabric services, the config, and the
//! per-client sessions from `BLOB.HELLO`. Transport never calls storage or the
//! data type. The engine hands transport buffers and awaits results.

pub mod config;
pub mod fabric;
pub mod operand;
pub mod session;

pub use fabric::{commit, fabric, shutdown, Fabric};
pub use session::Session;

// ─── EFA Types ───────────────────────────────────────────────────────────────

/// EFA endpoint address — 32 bytes, opaque to callers.
/// Contains GID (16B) + QPN (2B) + pad (2B) + QKEY (4B).
/// Obtained via fi_getname(). Exchanged during BLOB.HELLO.
#[derive(Clone)]
pub struct EfaAddress(pub [u8; 32]);

/// Client-side memory region descriptor.
/// Received during BLOB.HELLO. One per GPU memory pool (1-8 total, NOT per object).
#[derive(Debug, Clone)]
pub struct ClientRegion {
    pub rkey: u64,        // remote key (fi_write takes uint64_t key)
    pub remote_addr: u64, // base virtual address of the region on the client
    pub len: u64,         // total length of the region
}

//! Tiered mode
//!
//! Tiered storage reaches disk through io_uring, which exists only on Linux.
//! `unsupported` mirrors the `linux` signatures so the module can build
//! wherever and at least run dram mode.
//!
//! Module load refuses Tiered off of Linux (see `lib.rs`).

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{
    commit_engine, create_copy, execute_get, execute_set, free, prepare_engine, PreparedEngine,
};

#[cfg(not(target_os = "linux"))]
mod unsupported;
#[cfg(not(target_os = "linux"))]
pub use unsupported::{
    commit_engine, create_copy, execute_get, execute_set, free, prepare_engine, PreparedEngine,
};

//! Tiered mode for non-linux

use crate::data_type::{LoValue, ObjectId};
use crate::engine::{DataSource, Transport};
use crate::OperatingMode;

const REQUIRES_LINUX: &str = "Tiered mode requires Linux (io_uring)";

// ─── GET ─────────────────────────────────────────────────────────────────────

pub fn execute_get(
    _object_id: ObjectId,
    _obj_len: u64,
    _transport: Transport,
    _blocked_client: valkey_module::BlockedClient,
) {
    unreachable!("{}", REQUIRES_LINUX)
}

// ─── SET ─────────────────────────────────────────────────────────────────────

pub fn execute_set(
    _key_name: Vec<u8>,
    _obj_len: u64,
    _data_source: DataSource,
    _blocked_client: valkey_module::BlockedClient,
    _object_id: ObjectId,
) {
    unreachable!("{}", REQUIRES_LINUX)
}

// ─── Data Type Callbacks ─────────────────────────────────────────────────────

pub fn create_copy(_value: &LoValue) -> Option<LoValue> {
    unreachable!("{}", REQUIRES_LINUX)
}

pub fn free(_value: &LoValue) {
    unreachable!("{}", REQUIRES_LINUX)
}

// ─── io_uring Engine Lifecycle ───────────────────────────────────────────────

/// No engine to prepare.
pub struct PreparedEngine;

/// Always `Ok`: reaching this with `mode == Tiered` is impossible, since module load refused it.
pub fn prepare_engine(
    _mode: OperatingMode,
    _iovecs: Vec<libc::iovec>,
) -> Result<PreparedEngine, String> {
    Ok(PreparedEngine)
}

pub fn commit_engine(_engine: PreparedEngine) {}

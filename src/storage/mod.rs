//! Storage Layer — buffer pool + NVMe I/O.
//!
//! Operates on OIDs and file paths, NEVER on Valkey keys.
//! Command handler resolves key → OID via data type layer, then calls storage.

use crate::data_type::ObjectId;
pub use buffer::Buffer;
use engine::PinnedBuffer;

pub mod buffer;
pub mod engine;
pub mod fd_pool;
pub mod uring;

// ─── Callback Type Aliases ────────────────────────────────────────────────────

/// Callback for read completion: (buffer returned, bytes_read or error).
pub type ReadCallback = Box<dyn FnOnce(Buffer, Result<u64, StorageError>) + Send>;

/// Callback for write completion: (buffer returned, (ObjectId, crc32c) or error).
pub type WriteCallback = Box<dyn FnOnce(Buffer, Result<(ObjectId, u32), StorageError>) + Send>;

// ─── Error Types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum StorageError {
    IoError { code: i32 },
    PoolExhausted,
    ObjectTooLarge,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError { code } => write!(f, "I/O error (code {})", code),
            Self::PoolExhausted => write!(f, "buffer pool exhausted"),
            Self::ObjectTooLarge => write!(f, "object exceeds buffer size"),
        }
    }
}

// ─── NvmeEngine Trait ────────────────────────────────────────────────────────

/// NvmeEngine trait — abstraction over the io_uring submission path.
/// Implemented by UringNvmeEngine (production) and SyncNvmeEngine (tests).
pub trait NvmeEngine: Send + Sync {
    fn submit(&self, req: uring::IoRequest);
    /// Signal the engine to stop accepting work and exit its poller loop.
    /// Does not block — the poller thread exits asynchronously.
    fn signal_shutdown(&self);
}

// ─── Storage Trait (from interface doc) ──────────────────────────────────────

/// Storage trait — buffer pool + NVMe I/O.
/// All methods operate on ObjectId, never on Valkey keys.
pub trait Storage: Send + Sync {
    // ─── Buffer Pool ─────────────────────────────────────────────────────

    /// Get a buffer from the pool. Returns None if pool exhausted.
    /// The returned Buffer is owned — Drop returns it to the pool automatically.
    fn pool_get(&self) -> Option<Buffer>;

    /// Pool buffer size (all buffers are this fixed size).
    fn pool_buf_size(&self) -> usize;

    // ─── Registration ────────────────────────────────────────────────────

    /// Register pool buffers with io_uring (IORING_REGISTER_BUFFERS).
    fn register_buffers(&self) -> Result<(), StorageError>;

    /// Deregister pool buffers from io_uring.
    fn deregister_buffers(&self) -> Result<(), StorageError>;

    // ─── NVMe I/O ───────────────────────────────────────────────────────

    /// Read object from NVMe into buf. Async via io_uring ReadFixed.
    /// Takes Buffer by value (ownership transfers to storage during I/O).
    /// Returns (Buffer, bytes_read) in callback — caller gets buf back.
    fn read_into(&self, object_id: ObjectId, buf: Buffer, len: u64, on_complete: ReadCallback);

    /// Write buf to NVMe as a new object. Async via io_uring.
    /// Takes Buffer by value. Returns (Buffer, ObjectId, crc32c) via callback.
    fn write_new(&self, buf: Buffer, len: u64, on_complete: WriteCallback);

    /// Delete an object file from NVMe. Called on key deletion or eviction.
    fn delete(&self, object_id: ObjectId);
}

// ─── Global Storage Instance ─────────────────────────────────────────────────

use std::sync::OnceLock;
static STORAGE: OnceLock<engine::StorageEngine> = OnceLock::new();

pub fn get() -> &'static engine::StorageEngine {
    STORAGE.get().expect("storage not initialized")
}

/// Called by Buffer::drop() to return a buffer to the pool.
pub fn return_buffer(pinned: &'static engine::PinnedBuffer, idx: u16) {
    if let Some(storage) = STORAGE.get() {
        storage.buffer_pool().put_back(pinned, idx);
    }
}

/// Convenience: delete an object's NVMe file.
pub fn delete(object_id: crate::data_type::ObjectId) {
    get().delete_file(object_id);
}

pub fn init(buf_size: usize, buf_count: usize, data_dir: &str) {
    let storage = engine::StorageEngine::new(buf_size, buf_count, data_dir);
    STORAGE.set(storage).ok();
    // Fill the pool now that StorageEngine is in the static OnceLock.
    get().init_pool();
}

/// Shutdown: drain in-flight ops, close fds, clean up files.
/// Called from module deinit. TODO: implement when shutdown path is built.
pub fn shutdown() {
    // Signal the io_uring poller thread to drain pending ops and exit.
    // The poller checks the shutdown AtomicBool on each 100ms tick and exits
    // when set + no pending ops remain. This allows the process to terminate.
    if let Some(storage) = STORAGE.get() {
        storage.signal_shutdown();
    }
    // Future: FdPool closes all open fds.
    // Future: Orphan reconciliation (delete .dat files with no keyspace entry).
}

pub fn register_buffers() {
    let _ = get().register_buffers();
}

pub fn deregister_buffers() {
    let _ = get().deregister_buffers();
}

/// Return Buffer descriptors for transport layer to fi_mr_reg.
pub fn pinned_buffers() -> &'static [PinnedBuffer] {
    get().buffer_descriptors()
}

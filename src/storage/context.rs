//! ObjectContext, StreamingContext, and SegmentBuffer.
//!
//! These are the runtime companions to LoValue. Not serialized — rebuilt on load,
//! evicted independently of commands.
//!
//! - ObjectContext: lives in DRAMPool HashMap, long-lived, complete object.
//!   Owns all N buffers for the entire object.
//! - StreamingContext: lives on a tokio task, short-lived, rotating buffer window.
//!   Owns only X buffers; task-local ChunkIterator handles pagination.
//! - SegmentBuffer: a slice within a registered segment (DRAMPool or NVMePool).
//!
//! Neither context stores total_len or chunk counts — those are derived from
//! LoValue.len and crate::chunk_size() (immutable config). ChunkIterator (task-local)
//! handles all chunk geometry and iteration state.
//!
//! NOTE: CRC32c is NOT stored on either context struct. It is a local variable
//! in the tokio task that performs the SET. The task computes the rolling CRC as
//! chunks arrive (`let mut crc: u32 = 0`) and compares against the client-provided
//! value on completion. Neither ObjectContext nor StreamingContext needs CRC state.

use std::sync::atomic::{AtomicU8, Ordering};
use tokio::sync::watch;

use super::cache_policy::{now_minutes, AccessStats};

// ─── SegmentBuffer ───────────────────────────────────────────────────────────

/// A buffer that is a sub-allocation within a registered segment.
/// Segment-agnostic: works for both DRAMPool and NVMePool segments.
/// `segment_idx` identifies which registered iovec entry (io_uring buf_index).
#[derive(Debug)]
pub struct SegmentBuffer {
    /// Which segment this slice lives in (local index into the owning pool's segments vec).
    /// NOT the global io_uring iovec index - that is on Segment.iovec_index.
    pub segment_idx: u16,
    /// Byte offset within that segment.
    pub offset: u64,
    /// Requested allocation size in bytes: the user data length rounded up to IO_ALIGN
    /// via align_up(). Talc may internally allocate more due to its own metadata, but
    /// this field tracks only what we asked for. The user of this structure (e.g. Chunk)
    /// needs to track the exact bytes to read from every SegmentBuffer due to the
    /// alignment mentioned above.
    pub len: u32,
}

// ─── Object State ────────────────────────────────────────────────────────────

/// Atomic state for ObjectContext. `#[repr(u8)]` for use with AtomicU8.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    /// All buffers filled, object is servable.
    Ready = 0,
    /// Promotion in progress — NVMe ReadFixed filling buffers.
    Filling = 1,
}

/// Reason the promotion leader failed. Stored as AtomicU8 on ObjectContext.
/// Waiters check this after the watch channel sender is dropped to determine
/// the failure mode and reply with the correct error.
/// CRC mismatch is not represented — it panics the server immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionFailureReason {
    None = 0,
    ReadError = 1,
}

// ─── ObjectContext ───────────────────────────────────────────────────────────

/// Long-lived runtime state for a cached object in DRAMPool.
/// ALL N buffers for the entire object are allocated upfront from DRAMPool segment.
/// Stored in: `RwLock<HashMap<ObjectId, Arc<ObjectContext>>>`
/// Buffers are automatically returned to DRAMPool when the last Arc drops.
///
/// Does NOT store total_len or chunk counts. Callers derive object length from
/// LoValue.len and chunk geometry from crate::chunk_size() (immutable).
/// Each task creates its own ChunkIterator for iteration.
#[derive(Debug)]
pub struct ObjectContext {
    /// Ordered buffers. 1 for small objects, N for large.
    pub buffers: Vec<SegmentBuffer>,
    /// Filling->Ready transition via mark_ready() with Release ordering.
    /// Readers use is_ready() with Acquire ordering — guarantees visibility
    /// of the NVMe read data that was written before mark_ready().
    state: AtomicU8,
    /// Leader's watch channel for coalesced waiters. Waiters subscribe via
    /// Receiver. The leader sends `true` (mark_ready) or `false`
    /// (set_promotion_failure) before dropping the sender.
    /// Wrapped in std::sync::Mutex so mark_ready/set_promotion_failure can drop through &self.
    progress_tx: std::sync::Mutex<Option<watch::Sender<bool>>>,
    /// Set by leader on promotion failure before dropping progress_tx.
    /// Waiters check this on RecvError to distinguish failure mode.
    promotion_failure: AtomicU8,
    /// LFU access score for the cache policy.
    pub stats: AccessStats,
}

impl ObjectContext {
    /// Create a new ObjectContext in Ready state (e.g., DRAM-only SET).
    pub fn new_ready(buffers: Vec<SegmentBuffer>) -> Self {
        Self {
            buffers,
            state: AtomicU8::new(ObjectState::Ready as u8),
            progress_tx: std::sync::Mutex::new(None),
            promotion_failure: AtomicU8::new(PromotionFailureReason::None as u8),
            stats: AccessStats::new(now_minutes()),
        }
    }

    /// Create a new ObjectContext in Filling state (Tiered promotion path).
    /// Creates a watch channel so coalesced waiters can subscribe to progress.
    pub fn new_filling(buffers: Vec<SegmentBuffer>) -> Self {
        let (tx, _rx) = watch::channel(false);
        Self {
            buffers,
            state: AtomicU8::new(ObjectState::Filling as u8),
            progress_tx: std::sync::Mutex::new(Some(tx)),
            promotion_failure: AtomicU8::new(PromotionFailureReason::None as u8),
            stats: AccessStats::new(now_minutes()),
        }
    }

    /// Check if the object is fully ready for serving.
    /// Uses Acquire ordering: if this returns true, all data written
    /// before mark_ready() is guaranteed visible to this thread.
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == ObjectState::Ready as u8
    }

    /// Transition from Filling to Ready. Called by the promotion tokio task
    /// after all NVMe ReadFixed batches complete successfully. Sends `true` on
    /// the watch channel to signal success, then drops the sender.
    /// Uses Release ordering: all preceding writes (the NVMe read data
    /// in the buffer) are visible to any thread that later sees is_ready() == true.
    pub fn mark_ready(&self) {
        assert!(
            !self.is_ready(),
            "mark_ready called on an already Ready ObjectContext"
        );
        self.state
            .store(ObjectState::Ready as u8, Ordering::Release);
        let mut tx = self.progress_tx.lock().unwrap();
        if let Some(sender) = tx.as_ref() {
            let _ = sender.send(true);
        }
        *tx = None;
    }

    /// Subscribe to promotion completion. Returns None if the promotion has
    /// already concluded (sender dropped after mark_ready or set_promotion_failure).
    pub fn subscribe(&self) -> Option<watch::Receiver<bool>> {
        self.progress_tx
            .lock()
            .unwrap()
            .as_ref()
            .map(|tx| tx.subscribe())
    }

    /// Set promotion failure reason and drop the watch sender. Called by the leader
    /// on NVMe read error. Sends `false` on the channel to signal failure, then
    /// drops the sender.
    pub fn set_promotion_failure(&self, reason: PromotionFailureReason) {
        self.promotion_failure
            .store(reason as u8, Ordering::Release);
        let mut tx = self.progress_tx.lock().unwrap();
        if let Some(sender) = tx.as_ref() {
            let _ = sender.send(false);
        }
        *tx = None;
    }

    /// Get the promotion failure reason. Returns PromotionFailureReason::None if no failure.
    pub fn promotion_failure(&self) -> PromotionFailureReason {
        match self.promotion_failure.load(Ordering::Acquire) {
            1 => PromotionFailureReason::ReadError,
            _ => PromotionFailureReason::None,
        }
    }

    /// Wait for the promotion to complete. Returns Ok(()) when the object
    /// transitions to Ready, or Err(reason) on promotion failure.
    /// Called by coalesced waiters; the caller handles reply dispatch.
    pub async fn await_promotion(
        &self,
        mut rx: watch::Receiver<bool>,
    ) -> Result<(), PromotionFailureReason> {
        // Leader sends true (ready) or false (failure) before dropping the sender.
        let _ = rx.changed().await;
        if *rx.borrow() {
            Ok(())
        } else {
            Err(self.promotion_failure())
        }
    }
}

impl Drop for ObjectContext {
    fn drop(&mut self) {
        // Guard: pool may not be initialized in unit tests.
        if let Some(dram_pool) = super::DRAM_POOL.get() {
            for buf in &self.buffers {
                dram_pool.free(buf);
            }
        }
    }
}

impl ObjectContext {
    /// Deep-copy into a new DRAMPool allocation for COPY. `obj_len` is the
    /// object's user length (LoValue.len), which reproduces the source layout.
    /// Allocates the whole object at once so the copy stays in one segment.
    /// Returns None if the object is Filling (incomplete) or the pool is full.
    pub fn try_clone(&self, obj_len: u64) -> Option<Self> {
        // Cannot copy an object that is still being promoted (buffers incomplete).
        if !self.is_ready() {
            return None;
        }
        let pool = crate::storage::get_dram_pool();
        let dummy_context = valkey_module::Context::dummy();
        let new_buffers = pool.alloc_exact_expand_evict(&dummy_context, obj_len)?;
        let copy = Self::new_ready(new_buffers);
        for (src, dst) in self.buffers.iter().zip(&copy.buffers) {
            // SAFETY: src and dst are distinct live allocations of src.len bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    pool.buffer_ptr(src),
                    pool.buffer_ptr(dst),
                    src.len as usize,
                );
            }
        }
        Some(copy)
    }
}

// ─── StreamingContext ────────────────────────────────────────────────────────

/// Short-lived runtime state for a transient I/O operation on NVMePool.
/// Rotating window of X buffers, reused across batches.
/// Owned by a single tokio task — no Arc needed.
/// Buffers are automatically returned to NVMePool on drop.
///
/// Does NOT track progress (chunks_completed, total_chunks). The task-local
/// ChunkIterator handles all iteration state and progress tracking.
#[derive(Debug)]
pub struct StreamingContext {
    /// Rotating buffer window.
    pub buffers: Vec<SegmentBuffer>,
}

impl StreamingContext {
    /// Create a new StreamingContext for a transient NVMe I/O operation (GET or SET).
    /// The context owns only its rotating buffer window; the caller's task holds any
    /// `Arc<ObjectFile>` pin needed to keep the `ObjectFile` and fd alive for the transfer.
    pub fn new(buffers: Vec<SegmentBuffer>) -> Self {
        Self { buffers }
    }
}

impl Drop for StreamingContext {
    fn drop(&mut self) {
        // Guard: pool may not be initialized in unit tests.
        if let Some(nvme_pool) = super::NVME_POOL.get() {
            // Use free_n to free all buffers in one call and notify the queue.
            nvme_pool.free_n(&self.buffers);
        }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_context_ready() {
        let bufs = vec![
            SegmentBuffer {
                segment_idx: 0,
                offset: 0,
                len: 1024,
            },
            SegmentBuffer {
                segment_idx: 0,
                offset: 1024,
                len: 1024,
            },
        ];
        let ctx = ObjectContext::new_ready(bufs);
        assert!(ctx.is_ready());
    }

    #[test]
    fn test_object_context_filling_with_coalescing() {
        let bufs = vec![
            SegmentBuffer {
                segment_idx: 0,
                offset: 0,
                len: 8_000_000,
            },
            SegmentBuffer {
                segment_idx: 0,
                offset: 8_000_000,
                len: 8_000_000,
            },
            SegmentBuffer {
                segment_idx: 1,
                offset: 0,
                len: 8_000_000,
            },
        ];
        let ctx = ObjectContext::new_filling(bufs);
        assert!(!ctx.is_ready());
        assert_eq!(ctx.promotion_failure(), PromotionFailureReason::None);
        // Subscription should succeed on Filling object.
        assert!(ctx.subscribe().is_some());
        // mark_ready drops the sender.
        ctx.mark_ready();
        assert!(ctx.is_ready());
        assert!(ctx.subscribe().is_none());
    }

    #[test]
    fn test_object_context_failure_signaling() {
        let bufs = vec![SegmentBuffer {
            segment_idx: 0,
            offset: 0,
            len: 4096,
        }];
        let ctx = ObjectContext::new_filling(bufs);
        assert_eq!(ctx.promotion_failure(), PromotionFailureReason::None);
        ctx.set_promotion_failure(PromotionFailureReason::ReadError);
        assert_eq!(ctx.promotion_failure(), PromotionFailureReason::ReadError);
    }

    #[test]
    fn test_object_context_ready_no_subscription() {
        let bufs = vec![SegmentBuffer {
            segment_idx: 0,
            offset: 0,
            len: 1024,
        }];
        let ctx = ObjectContext::new_ready(bufs);
        assert!(ctx.is_ready());
        assert!(ctx.subscribe().is_none());
    }

    #[tokio::test]
    async fn test_failure_propagates_to_waiter() {
        let bufs = vec![
            SegmentBuffer {
                segment_idx: 0,
                offset: 0,
                len: 4096,
            },
            SegmentBuffer {
                segment_idx: 0,
                offset: 4096,
                len: 4096,
            },
        ];
        let ctx = std::sync::Arc::new(ObjectContext::new_filling(bufs));
        let rx = ctx
            .subscribe()
            .expect("subscribe should succeed on Filling");
        let waiter_ctx = ctx.clone();
        let waiter = tokio::spawn(async move { waiter_ctx.await_promotion(rx).await });
        // Leader fails the promotion.
        tokio::task::yield_now().await;
        ctx.set_promotion_failure(PromotionFailureReason::ReadError);
        let result = waiter.await.unwrap();
        assert_eq!(result, Err(PromotionFailureReason::ReadError));
    }

    #[tokio::test]
    async fn test_success_propagates_to_waiter() {
        let bufs = vec![SegmentBuffer {
            segment_idx: 0,
            offset: 0,
            len: 4096,
        }];
        let ctx = std::sync::Arc::new(ObjectContext::new_filling(bufs));
        let rx = ctx
            .subscribe()
            .expect("subscribe should succeed on Filling");
        let waiter_ctx = ctx.clone();
        let waiter = tokio::spawn(async move { waiter_ctx.await_promotion(rx).await });
        ctx.mark_ready();
        let result = waiter.await.unwrap();
        assert_eq!(result, Ok(()));
    }
}

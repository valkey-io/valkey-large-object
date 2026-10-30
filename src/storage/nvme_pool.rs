//! NVMePool — short-lived transient I/O buffers with async queueing.
//!
//! Thin wrapper around SegmentPool. No object map, no draining.
//! Sized at startup to ceil(disk-staging-size / segment-size) uniform segments,
//! fixed thereafter — never expanded or shrunk (unlike DRAMPool).
//! StreamingContexts allocate from here and free on request completion.
//!
//! Staging Queue:
//! A request that finds staging full waits in a FIFO queue and proceeds once
//! enough buffers free up. Overload becomes added latency instead of errors.
//! - No module timeout: the client's own timeout bounds the wait.
//! - Fairness: arrival order via tokio::sync::Mutex FIFO.
//! - Off the main thread: the wait happens in the spawned task.
//! - Bounded queue: safety net for clients with no timeout (default 1024).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::context::SegmentBuffer;
use super::segment_pool::SegmentPool;

/// Outcome of an async staging buffer allocation attempt.
pub enum AllocOutcome {
    /// Got the requested window of buffers.
    Got(Vec<SegmentBuffer>),
    /// The client disconnected while queued — nothing to reply to.
    Cancelled,
    /// Queue is at capacity — reject to apply backpressure.
    QueueFull,
}

pub struct NVMePool {
    pool: SegmentPool,
    /// Wakes the head-of-queue whenever buffers are freed.
    freed: tokio::sync::Notify,
    /// FIFO gate: tokio's Mutex grants the lock in arrival order.
    /// Only the holder can attempt alloc_window; everyone else waits.
    queue: tokio::sync::Mutex<()>,
    /// One cancel signal per client with queued request(s). A disconnect
    /// fires notify_waiters, waking all of that client's queued requests.
    /// The usize is the number of live waiters for this client; the entry is
    /// removed when it reaches zero.
    cancels: Mutex<HashMap<u64, (Arc<tokio::sync::Notify>, usize)>>,
    /// Current number of waiters in the queue. Also the INFO metric.
    queue_len: AtomicU64,
    /// Maximum queue depth. Rejects with QueueFull above this.
    queue_capacity: u64,
}

impl NVMePool {
    pub fn new(segment_count: usize, segment_size: usize, queue_capacity: u64) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size, super::uring::PoolType::Nvme),
            freed: tokio::sync::Notify::new(),
            queue: tokio::sync::Mutex::new(()),
            cancels: Mutex::new(HashMap::new()),
            queue_len: AtomicU64::new(0),
            queue_capacity,
        }
    }

    /// Current queue length (waiters). Used by INFO.
    pub fn queue_len(&self) -> u64 {
        self.queue_len.load(Ordering::Relaxed)
    }

    /// Free multiple buffers and notify waiting allocators.
    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers);
        self.freed.notify_waiters();
    }

    /// Synchronous allocation without queueing. Returns `None` if staging is full
    /// or if async requests are queued (FIFO fairness).
    pub fn alloc_window(
        &self,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        // Respect FIFO fairness: if async requests are queued, don't let sync
        // callers jump ahead. This prevents TCP from starving queued EFA requests.
        if self.queue_len.load(Ordering::Relaxed) > 0 {
            return None;
        }
        self.pool.alloc_window(size, max_buffers, min_buffers)
    }

    /// Async allocation with FIFO queueing and cancellation support.
    ///
    /// If staging is full, the request waits in a FIFO queue until buffers free up.
    /// A client disconnect cancels the wait. If the queue itself is full, rejects
    /// immediately with `QueueFull`.
    pub async fn alloc_window_cancellable(
        &self,
        client_id: u64,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
    ) -> AllocOutcome {
        // Step 1: Queue cap check (CAS loop for exact enforcement).
        // A simple load + fetch_add has a TOCTOU gap where multiple requests
        // overshoot. compare_exchange_weak retries on spurious failures.
        loop {
            let current = self.queue_len.load(Ordering::Relaxed);
            if current >= self.queue_capacity {
                return AllocOutcome::QueueFull;
            }
            if self
                .queue_len
                .compare_exchange_weak(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // Step 2: Register cancel signal for this client.
        let cancel = self.register_cancel(client_id);
        let _guard = CancelGuard {
            pool: self,
            client_id,
        };

        // Pre-register as a waiter so a disconnect mid-wait is not lost.
        let mut cancelled = std::pin::pin!(cancel.notified());
        cancelled.as_mut().enable();

        // Step 3: Fast path — try_lock to skip the queue if nobody is waiting.
        let lock_guard = match self.queue.try_lock() {
            Ok(lock) => {
                // Nobody waiting. Try immediate allocation.
                if let Some(buffers) = self.pool.alloc_window(size, max_buffers, min_buffers) {
                    return AllocOutcome::Got(buffers);
                }
                // No buffers available; we already hold the lock, proceed to alloc loop.
                lock
            }
            Err(_) => {
                // Others waiting — must queue. Wait for the FIFO lock.
                tokio::select! {
                    biased;
                    _ = &mut cancelled => {
                        // Cancelled while waiting for lock.
                        return AllocOutcome::Cancelled;
                    }
                    lock = self.queue.lock() => lock,
                }
            }
        };

        // Step 4: Alloc loop (only the lock holder runs this).
        loop {
            // Register notified BEFORE trying alloc to avoid lost wakes.
            let freed_notified = self.freed.notified();

            if let Some(buffers) = self.pool.alloc_window(size, max_buffers, min_buffers) {
                drop(lock_guard); // Release lock → next waiter proceeds.
                return AllocOutcome::Got(buffers);
            }

            // Wait for either buffers freed or cancellation.
            tokio::select! {
                biased;
                _ = &mut cancelled => {
                    return AllocOutcome::Cancelled;
                }
                _ = freed_notified => {
                    // Buffers may have been freed; loop back and retry.
                }
            }
        }
    }

    /// Cancel all queued requests for a client (called on disconnect).
    pub fn cancel_client(&self, client_id: u64) {
        let signal = self
            .cancels
            .lock()
            .expect("cancels lock unavailable")
            .get(&client_id)
            .map(|(n, _)| n.clone());
        if let Some(signal) = signal {
            signal.notify_waiters();
        }
    }

    /// Get or create the cancel signal for a client. Increments the waiter
    /// count so the entry stays alive until the last waiter unregisters.
    fn register_cancel(&self, client_id: u64) -> Arc<tokio::sync::Notify> {
        let mut cancels = self.cancels.lock().expect("cancels lock unavailable");
        let entry = cancels
            .entry(client_id)
            .or_insert_with(|| (Arc::new(tokio::sync::Notify::new()), 0));
        entry.1 += 1;
        entry.0.clone()
    }

    /// Decrement the waiter count for a client. Removes the entry when it
    /// reaches zero, so the map does not grow with connection churn.
    fn unregister_cancel(&self, client_id: u64) {
        let mut cancels = self.cancels.lock().expect("cancels lock unavailable");
        if let Some((_, count)) = cancels.get_mut(&client_id) {
            *count -= 1;
            if *count == 0 {
                cancels.remove(&client_id);
            }
        }
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
    }

    /// Whether the segment owning `buf` is registered in the io_uring kernel
    /// buffer table (picks fixed vs non-fixed I/O). See SegmentPool.
    pub fn is_buf_io_uring_registered(&self, buf: &SegmentBuffer) -> bool {
        self.pool.is_buf_io_uring_registered(buf)
    }

    /// Mark all current segments io_uring-registered (startup, post-register).
    pub fn mark_all_registered(&self) {
        self.pool.mark_all_registered();
    }

    /// Startup iovec snapshot for this pool's ring. See `SegmentPool::startup_iovecs`.
    pub fn startup_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.startup_iovecs()
    }

    /// See `SegmentPool::rebuild_dense_iovecs`. The NVMe pool never expands or
    /// shrinks, so its ring never actually re-registers; provided only so the poller's
    /// pool-generic rebuild path is total.
    pub fn rebuild_dense_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.rebuild_dense_iovecs()
    }

    /// See `SegmentPool::io_uring_registered_count`.
    pub fn io_uring_registered_count(&self) -> usize {
        self.pool.io_uring_registered_count()
    }

    /// See `SegmentPool::rdma_registered_count`.
    pub fn rdma_registered_count(&self) -> usize {
        self.pool.rdma_registered_count()
    }

    /// Counts of (total, draining, unused) segments; total includes draining. Used by INFO largeobj.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        self.pool.segment_counts()
    }

    /// Bytes allocated across the staging pool's segments: buffers held by in-flight I/O.
    pub fn allocated_bytes(&self) -> usize {
        self.pool.allocated_bytes()
    }

    /// Total free-gap count across the staging pool's segments (talc fragmentation signal).
    pub fn fragment_count(&self) -> usize {
        self.pool.fragment_count()
    }

    /// Call `f` with each segment's base pointer and size. Used for EFA registration.
    pub fn with_segment_slices<F>(&self, f: F)
    where
        F: FnMut(*const u8, usize),
    {
        self.pool.with_segment_slices(f);
    }
}

/// RAII guard that decrements queue_len and cleans up cancel registry on drop.
/// Covers both the success path (guard drops when the function returns with
/// buffers) and the cancellation path (guard drops when the function returns
/// Cancelled). No explicit disarm needed — Drop always fires exactly once.
struct CancelGuard<'a> {
    pool: &'a NVMePool,
    client_id: u64,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        self.pool.queue_len.fetch_sub(1, Ordering::Relaxed);
        self.pool.unregister_cancel(self.client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Below the 8 MiB default chunk-size, so every window is one buffer.
    const OBJ: usize = 1000;
    const CLIENT: u64 = 7;

    impl AllocOutcome {
        fn is_got(&self) -> bool {
            matches!(self, AllocOutcome::Got(_))
        }
        fn is_cancelled(&self) -> bool {
            matches!(self, AllocOutcome::Cancelled)
        }
    }

    /// A one-segment pool with every buffer allocated, plus the windows holding it full.
    fn exhausted_pool() -> (Arc<NVMePool>, Vec<Vec<SegmentBuffer>>) {
        let pool = Arc::new(NVMePool::new(1, 65536, 1024));
        let mut held = Vec::new();
        while let Some(window) = pool.pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        assert!(!held.is_empty(), "pool should fit at least one window");
        (pool, held)
    }

    /// Multi-thread runtime matching production (wait runs on a tokio worker,
    /// frees/cancels arrive from another thread).
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("test runtime")
    }

    /// Spin until the pool has at least `n` queued requests for `client_id`
    /// and the FIFO lock is held (the waiter has reached the alloc loop).
    fn wait_registered(pool: &NVMePool, client_id: u64, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let registered = pool
                .cancels
                .lock()
                .unwrap()
                .get(&client_id)
                .map(|(_, count)| *count >= n)
                .unwrap_or(false);
            let lock_held = pool.queue.try_lock().is_err();
            if registered && lock_held {
                return;
            }
            assert!(Instant::now() < deadline, "waiter(s) never registered");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn wait_empty_registry(pool: &NVMePool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pool.cancels.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "registry never drained");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    // ─── Mechanic tests ────────────────────────────────────────────────

    #[test]
    fn test_cancel_registration() {
        let pool = NVMePool::new(1, 1024 * 1024, 100);

        // Register a cancel signal — same client gets the same Arc.
        let cancel1 = pool.register_cancel(42);
        let cancel2 = pool.register_cancel(42);
        assert!(Arc::ptr_eq(&cancel1, &cancel2));

        // Waiter count should be 2 after two registrations.
        assert_eq!(pool.cancels.lock().unwrap().get(&42).unwrap().1, 2);

        // Different client gets different signal.
        let cancel3 = pool.register_cancel(43);
        assert!(!Arc::ptr_eq(&cancel1, &cancel3));
    }

    // ─── Async alloc flow tests ──────────────────────────────────────────

    /// A free unblocks a queued waiter, which then allocates the freed window.
    #[test]
    fn test_waiter_succeeds_when_another_op_frees() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);
            pool.free_n(&held.pop().expect("held window"));
            assert!(waiter.await.expect("waiter task").is_got());
            assert_eq!(pool.queue_len(), 0);
        });
    }

    /// A disconnect cancels a queued waiter: it leaves the queue allocating
    /// nothing, and its registry entry is cleaned up.
    #[test]
    fn test_disconnect_cancels_waiter() {
        let (pool, _held) = exhausted_pool();
        runtime().block_on(async {
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);
            pool.cancel_client(CLIENT);
            assert!(waiter.await.expect("waiter task").is_cancelled());
            wait_empty_registry(&pool);
            assert_eq!(pool.queue_len(), 0);
        });
    }

    /// Cancelling the head-of-queue waiter releases the FIFO lock so the next
    /// waiter behind it can proceed when buffers free up.
    #[test]
    fn test_cancel_head_unblocks_next_waiter() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            // Head enters the queue, acquires the FIFO lock, parks on freed/cancelled.
            let head = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);
            // Tail queues behind the head, waiting to acquire the lock.
            let tail = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT + 1, 1);
            // Cancel the head. Its lock_guard drops, tail acquires the lock.
            pool.cancel_client(CLIENT);
            assert!(head.await.expect("head task").is_cancelled());
            // Free a buffer. The tail — now the lock holder — takes it.
            pool.free_n(&held.pop().expect("held window"));
            assert!(tail.await.expect("tail task").is_got());
            assert_eq!(pool.queue_len(), 0);
        });
    }

    /// FIFO fairness: a newcomer cannot take a buffer freed for the waiting head.
    /// The head holds the queue lock while parked on `freed`, so the newcomer's
    /// try_lock fails and it enters the FIFO behind the head.
    #[test]
    fn test_newcomer_cannot_jump_the_queue() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            let head = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);
            // Free one buffer. The head holds the queue lock and will take it.
            pool.free_n(&held.pop().expect("held window"));
            // A newcomer (different client) queues behind the head.
            let newcomer = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT + 1, 1);
            // Cancel the newcomer so the test doesn't hang — it must NOT have allocated.
            pool.cancel_client(CLIENT + 1);
            assert!(newcomer.await.expect("newcomer task").is_cancelled());
            assert!(head.await.expect("head task").is_got());
        });
    }

    // ─── Sync vs Async allocation path tests ─────────────────────────────

    /// Sync `alloc_window` returns `None` immediately when staging is full.
    ///
    /// This is the path TCP uses — fail fast without queueing, because TCP
    /// holds an inline `Vec<u8>` copy that would consume unbounded memory if
    /// queued requests accumulated.
    #[test]
    fn test_sync_alloc_returns_none_when_full() {
        let (pool, _held) = exhausted_pool();
        // Sync path: returns None immediately, does not block or queue.
        let result = pool.alloc_window(OBJ, 1, 1);
        assert!(result.is_none(), "sync alloc should return None when full");
        // No waiters registered — sync path never touches the queue.
        assert_eq!(pool.queue_len(), 0);
        assert!(pool.cancels.lock().unwrap().is_empty());
    }

    /// Sync and async paths respect FIFO fairness: a sync alloc fails when
    /// async requests are queued, preventing starvation.
    #[test]
    fn test_sync_alloc_respects_queued_waiters() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            // An async waiter queues up.
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);

            // Free one buffer.
            pool.free_n(&held.pop().expect("held window"));

            // Sync alloc MUST fail — it cannot jump ahead of the queued waiter.
            let sync_result = pool.alloc_window(OBJ, 1, 1);
            assert!(
                sync_result.is_none(),
                "sync alloc must fail when async requests are queued"
            );

            // The queued waiter gets the buffer.
            assert!(waiter.await.expect("waiter task").is_got());
        });
    }
}

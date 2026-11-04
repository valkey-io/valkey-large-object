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
//! - Bounded by memory overhead: TCP SETs contribute their payload size to
//!   queue_overhead; EFA SETs and all GETs contribute 0 (no inline payload).
//!   Admission is rejected when overhead would exceed queue_overhead_limit
//!   (derived from disk-staging-size, ~1:1 ratio).

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
    /// Queue overhead limit exceeded — reject to apply backpressure.
    OverheadLimitExceeded,
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
    /// Current number of waiters in the queue. Observability metric only.
    queue_len: AtomicU64,
    /// Current memory overhead from queued TCP payloads (bytes). Admission gate.
    queue_overhead: AtomicU64,
    /// Maximum allowed queue overhead. Derived from disk-staging-size (1:1 ratio).
    queue_overhead_limit: u64,
}

impl NVMePool {
    pub fn new(segment_count: usize, segment_size: usize, queue_overhead_limit: u64) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size, super::uring::PoolType::Nvme),
            freed: tokio::sync::Notify::new(),
            queue: tokio::sync::Mutex::new(()),
            cancels: Mutex::new(HashMap::new()),
            queue_len: AtomicU64::new(0),
            queue_overhead: AtomicU64::new(0),
            queue_overhead_limit,
        }
    }

    /// Current queue length (waiters). Used by INFO.
    pub fn queue_len(&self) -> u64 {
        self.queue_len.load(Ordering::Relaxed)
    }

    /// Current queue overhead in bytes (from queued TCP payloads). Used by INFO.
    pub fn queue_overhead(&self) -> u64 {
        self.queue_overhead.load(Ordering::Relaxed)
    }

    /// Free multiple buffers and notify waiting allocators.
    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers);
        self.freed.notify_waiters();
    }

    /// Async allocation with FIFO queueing and cancellation support.
    ///
    /// If staging is full, the request waits in a FIFO queue until buffers free up.
    /// A client disconnect cancels the wait.
    ///
    /// `overhead_bytes` is the memory footprint this request adds to the queue while
    /// waiting (TCP: obj_len for the inline Vec<u8> payload; EFA: 0 since data
    /// transfers via RDMA). If accepting this request would push `queue_overhead`
    /// above `queue_overhead_limit`, rejects immediately with `OverheadLimitExceeded`.
    pub async fn alloc_window_cancellable(
        &self,
        client_id: u64,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
        overhead_bytes: usize,
    ) -> AllocOutcome {
        // Step 1: Fast path — allocate if staging is free and nobody is queued.
        if self.queue_len.load(Ordering::Relaxed) == 0 {
            if let Some(buffers) = self.pool.alloc_window(size, max_buffers, min_buffers) {
                return AllocOutcome::Got(buffers);
            }
        }
        // Step 2: Staging full — check overhead limit and register as waiter.
        // CAS loop for exact enforcement: avoids TOCTOU gaps where multiple
        // requests overshoot the limit.
        let overhead_bytes_u64 = overhead_bytes as u64;
        loop {
            let current = self.queue_overhead.load(Ordering::Relaxed);
            let new_overhead = current.saturating_add(overhead_bytes_u64);
            if new_overhead > self.queue_overhead_limit && overhead_bytes > 0 {
                return AllocOutcome::OverheadLimitExceeded;
            }
            if self
                .queue_overhead
                .compare_exchange_weak(current, new_overhead, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        // Increment queue_len for observability.
        self.queue_len.fetch_add(1, Ordering::Relaxed);
        // Step 3: Register cancel signal for this client.
        let cancel = self.register_cancel(client_id);
        let _guard = CancelGuard {
            pool: self,
            client_id,
            overhead_bytes: overhead_bytes_u64,
        };
        // Pre-register as a waiter so a disconnect mid-wait is not lost.
        let mut cancelled = std::pin::pin!(cancel.notified());
        cancelled.as_mut().enable();
        // Step 4: Wait for the FIFO lock.
        let lock_guard = tokio::select! {
            biased;
            _ = &mut cancelled => {
                return AllocOutcome::Cancelled;
            }
            lock = self.queue.lock() => lock,
        };
        // Step 5: Alloc loop (only the lock holder runs this).
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

/// RAII guard that decrements queue_len, queue_overhead and cleans up cancel
/// registry on drop. Covers both the success path (guard drops when the function
/// returns with buffers) and the cancellation path (guard drops when the function
/// returns Cancelled). No explicit disarm needed — Drop always fires exactly once.
struct CancelGuard<'a> {
    pool: &'a NVMePool,
    client_id: u64,
    overhead_bytes: u64,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        self.pool.queue_len.fetch_sub(1, Ordering::Relaxed);
        self.pool
            .queue_overhead
            .fetch_sub(self.overhead_bytes, Ordering::Relaxed);
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
        fn is_overhead_limit_exceeded(&self) -> bool {
            matches!(self, AllocOutcome::OverheadLimitExceeded)
        }
    }

    /// A one-segment pool with every buffer allocated, plus the windows holding it full.
    /// Uses a large overhead limit (1 MiB) so tests can focus on staging exhaustion.
    fn exhausted_pool() -> (Arc<NVMePool>, Vec<Vec<SegmentBuffer>>) {
        let pool = Arc::new(NVMePool::new(1, 65536, 1024 * 1024));
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
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 0).await }
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
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 0).await }
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
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 0).await }
            });
            wait_registered(&pool, CLIENT, 1);
            // Tail queues behind the head, waiting to acquire the lock.
            let tail = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move {
                    pool.alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1, 0)
                        .await
                }
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

    /// FIFO fairness under exhaustion: when two requests are queued and one
    /// buffer frees, the head gets it; the tail must wait for another free.
    #[test]
    fn test_fifo_order_under_exhaustion() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            // Both find staging full in step 1 and enter the FIFO queue.
            let head = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 0).await }
            });
            wait_registered(&pool, CLIENT, 1);
            let tail = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move {
                    pool.alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1, 0)
                        .await
                }
            });
            wait_registered(&pool, CLIENT + 1, 1);
            // Free one buffer — only the head should get it.
            pool.free_n(&held.pop().expect("held window"));
            assert!(head.await.expect("head task").is_got());
            // Tail is still queued, cancel it to avoid hanging.
            pool.cancel_client(CLIENT + 1);
            assert!(tail.await.expect("tail task").is_cancelled());
        });
    }

    // ─── Overhead limit tests ────────────────────────────────────────────

    /// When queue overhead would exceed the limit, TCP requests are rejected.
    #[test]
    fn test_overhead_limit_rejects_when_exceeded() {
        // Create pool with a tiny overhead limit (500 bytes).
        let pool = Arc::new(NVMePool::new(1, 65536, 500));
        // Exhaust the staging pool.
        let mut held = Vec::new();
        while let Some(window) = pool.pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        runtime().block_on(async {
            // First TCP request with 400 byte overhead should queue.
            let waiter1 = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 400).await }
            });
            wait_registered(&pool, CLIENT, 1);
            assert_eq!(pool.queue_overhead(), 400);

            // Second TCP request with 200 byte overhead would push total to 600 > 500.
            // Should be rejected immediately.
            let result = pool
                .alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1, 200)
                .await;
            assert!(
                result.is_overhead_limit_exceeded(),
                "should reject when overhead limit exceeded"
            );
            // Overhead unchanged — rejected request didn't add to it.
            assert_eq!(pool.queue_overhead(), 400);

            // Clean up: free a buffer so the first waiter completes.
            pool.free_n(&held.pop().expect("held window"));
            assert!(waiter1.await.expect("waiter task").is_got());
            assert_eq!(pool.queue_overhead(), 0);
        });
    }

    /// EFA requests (overhead_bytes=0) are never rejected by overhead limit,
    /// even when the limit is already reached.
    #[test]
    fn test_efa_ignores_overhead_limit() {
        // Create pool with small overhead limit (500 bytes).
        let pool = Arc::new(NVMePool::new(1, 65536, 500));
        // Exhaust the staging pool.
        let mut held = Vec::new();
        while let Some(window) = pool.pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        runtime().block_on(async {
            // First: TCP request pushes overhead to the limit.
            let tcp_waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 500).await }
            });
            wait_registered(&pool, CLIENT, 1);
            assert_eq!(pool.queue_overhead(), 500);

            // EFA request (overhead=0) should still queue even though limit is reached.
            // This exercises the `&& overhead_bytes > 0` guard.
            let efa_waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move {
                    pool.alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1, 0)
                        .await
                }
            });
            wait_registered(&pool, CLIENT + 1, 1);
            assert_eq!(pool.queue_overhead(), 500); // unchanged
            assert_eq!(pool.queue_len(), 2);

            // Free buffers so both complete.
            pool.free_n(&held.pop().expect("held window"));
            assert!(tcp_waiter.await.expect("tcp waiter").is_got());
            pool.free_n(&held.pop().expect("held window"));
            assert!(efa_waiter.await.expect("efa waiter").is_got());
        });
    }

    /// Overhead is correctly decremented when a request completes successfully.
    #[test]
    fn test_overhead_decrements_on_completion() {
        let pool = Arc::new(NVMePool::new(1, 65536, 10000));
        // Exhaust staging.
        let mut held = Vec::new();
        while let Some(window) = pool.pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        runtime().block_on(async {
            // TCP request with 500 byte overhead.
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 500).await }
            });
            wait_registered(&pool, CLIENT, 1);
            assert_eq!(pool.queue_overhead(), 500);

            // Free a buffer — waiter completes, overhead decrements.
            pool.free_n(&held.pop().expect("held window"));
            assert!(waiter.await.expect("waiter task").is_got());
            assert_eq!(pool.queue_overhead(), 0);
        });
    }

    /// Overhead is correctly decremented when a request is cancelled.
    #[test]
    fn test_overhead_decrements_on_cancellation() {
        let pool = Arc::new(NVMePool::new(1, 65536, 10000));
        // Exhaust staging.
        let mut held = Vec::new();
        while let Some(window) = pool.pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        let _ = held; // silence unused warning
        runtime().block_on(async {
            // TCP request with 500 byte overhead.
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 500).await }
            });
            wait_registered(&pool, CLIENT, 1);
            assert_eq!(pool.queue_overhead(), 500);

            // Cancel the client — overhead decrements.
            pool.cancel_client(CLIENT);
            assert!(waiter.await.expect("waiter task").is_cancelled());
            assert_eq!(pool.queue_overhead(), 0);
        });
    }

    /// Mixed TCP and EFA requests: overhead only tracks TCP.
    #[test]
    fn test_mixed_tcp_efa_overhead_accounting() {
        let pool = Arc::new(NVMePool::new(1, 65536, 10000));
        // Exhaust staging.
        let mut held = Vec::new();
        while let Some(window) = pool.pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        runtime().block_on(async {
            // First: TCP with 300 byte overhead.
            let tcp_waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1, 300).await }
            });
            wait_registered(&pool, CLIENT, 1);
            assert_eq!(pool.queue_overhead(), 300);
            assert_eq!(pool.queue_len(), 1);

            // Second: EFA with 0 overhead queues behind TCP.
            let efa_waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move {
                    pool.alloc_window_cancellable(CLIENT + 1, OBJ, 1, 1, 0)
                        .await
                }
            });
            wait_registered(&pool, CLIENT + 1, 1);
            // Overhead unchanged — EFA doesn't add.
            assert_eq!(pool.queue_overhead(), 300);
            assert_eq!(pool.queue_len(), 2);

            // Third: TCP with 200 byte overhead.
            let tcp_waiter2 = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move {
                    pool.alloc_window_cancellable(CLIENT + 2, OBJ, 1, 1, 200)
                        .await
                }
            });
            wait_registered(&pool, CLIENT + 2, 1);
            assert_eq!(pool.queue_overhead(), 500);
            assert_eq!(pool.queue_len(), 3);

            // Free buffers one at a time — FIFO order.
            pool.free_n(&held.pop().expect("held window"));
            assert!(tcp_waiter.await.expect("tcp waiter").is_got());
            assert_eq!(pool.queue_overhead(), 200); // 300 decremented

            pool.free_n(&held.pop().expect("held window"));
            assert!(efa_waiter.await.expect("efa waiter").is_got());
            assert_eq!(pool.queue_overhead(), 200); // EFA had 0

            pool.free_n(&held.pop().expect("held window"));
            assert!(tcp_waiter2.await.expect("tcp waiter 2").is_got());
            assert_eq!(pool.queue_overhead(), 0);
        });
    }
}

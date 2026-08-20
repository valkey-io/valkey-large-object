//! Transport Layer — Multi-Device EFA/libfabric RMA (one-sided RDMA).
//!
//! Architecture: Worker-per-device with shared MPMC queue.
//!
//!   ┌──────────────────────┐
//!   │   VALKEY MAIN THREAD  │
//!   │  DMA.HELLO / GET/SET  │
//!   └──────────┬───────────┘
//!              │ WorkItem
//!              ▼
//!   ┌──────────────────────┐
//!   │  crossbeam MPMC queue │
//!   └──┬──────┬──────┬─────┘
//!      │      │      │
//!      ▼      ▼      ▼
//!   Worker0 Worker1 Worker2 ...  (one per EFA device)
//!   (EP+AV+CQ)  (EP+AV+CQ)  (EP+AV+CQ)
//!
//! Lifecycle:
//!   init()              → discover EFA devices, create N endpoints, start N workers
//!   register_buffers()  → fi_mr_reg pool buffers on each endpoint
//!   Session::new()      → fi_av_insert peer on ALL N endpoints, store fi_addrs[N]
//!   submit_write/read() → push WorkItem to shared queue, worker picks up
//!   shutdown()          → signal workers to stop, join threads, close endpoints
//!
//! Transport never calls storage or data type.

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Instant;

#[cfg(not(no_efa))]
use std::sync::atomic::Ordering;
#[cfg(not(no_efa))]
use std::time::Duration;

use crossbeam_channel::Sender;
#[cfg(not(no_efa))]
use crossbeam_channel::{bounded, Receiver};

#[cfg(no_efa)]
#[allow(unused_imports)]
use crossbeam_channel::bounded;

use crate::storage::Buffer;

// ─── Conditional compilation ─────────────────────────────────────────────────

#[cfg(not(no_efa))]
mod efa;
#[cfg(not(no_efa))]
mod ffi;

// ─── EFA Types ───────────────────────────────────────────────────────────────

/// EFA endpoint address — 32 bytes, opaque to callers.
#[derive(Clone)]
pub struct EfaAddress(pub [u8; 32]);

// ─── Error Types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TransportError {
    DeviceNotFound,
    RegistrationFailed,
    SessionCreateFailed,
    WriteFailed { code: i32 },
    ReadFailed { code: i32 },
    Timeout,
    SessionClosed,
    Unavailable,
    QueueFull,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeviceNotFound => write!(f, "EFA device not found"),
            Self::RegistrationFailed => write!(f, "fi_mr_reg failed"),
            Self::SessionCreateFailed => write!(f, "session create failed"),
            Self::WriteFailed { code } => write!(f, "fi_write failed ({})", code),
            Self::ReadFailed { code } => write!(f, "fi_read failed ({})", code),
            Self::Timeout => write!(f, "CQ poll timeout"),
            Self::SessionClosed => write!(f, "session closed"),
            Self::Unavailable => write!(f, "EFA unavailable"),
            Self::QueueFull => write!(f, "work queue full"),
        }
    }
}

// ─── WorkItem ────────────────────────────────────────────────────────────────

/// Operation type for a work item.
pub enum RmaOp {
    /// fi_write: push server buffer → client memory (DMA.GET)
    Write,
    /// fi_read: pull client memory → server buffer (DMA.SET)
    Read,
}

/// A unit of work submitted to the EFA worker pool.
/// Contains everything a worker needs to perform one RMA operation.
pub struct WorkItem {
    /// Operation type
    pub op: RmaOp,

    /// Pre-resolved fi_addr_t for each EFA device (from session).
    /// Worker i uses fi_addrs[i].
    pub fi_addrs: Vec<u64>,

    /// Pool buffer (owns it during transfer)
    pub buf: Buffer,

    /// Number of bytes to transfer
    pub len: usize,

    /// Remote memory coordinates (from command args, per-request)
    pub remote_addr: u64,
    pub rkey: u64,

    /// Callback to fire on completion (unblocks the Valkey client)
    pub on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
}

// ─── Session ─────────────────────────────────────────────────────────────────

/// Per-client DMA session. Created during DMA.HELLO.
/// Stores the fi_addr_t handle for this client on each EFA device.
/// This is the ONLY per-client state the server maintains.
pub struct Session {
    /// fi_addr_t handle on each worker's AV (one per EFA device).
    /// Worker i uses fi_addrs[i] to target this client.
    pub fi_addrs: Vec<u64>,

    /// Metrics
    pub connected_at: Instant,
}

impl Session {
    /// Create a session: fi_av_insert peer on ALL N EFA devices.
    #[cfg(not(no_efa))]
    pub fn new(pool: &WorkerPool, peer_addr: &EfaAddress) -> Result<Self, TransportError> {
        let mut fi_addrs = Vec::with_capacity(pool.device_count());
        for worker in &pool.workers {
            let fi_addr = worker.endpoint.insert_peer(&peer_addr.0)?;
            fi_addrs.push(fi_addr);
        }
        Ok(Self {
            fi_addrs,
            connected_at: Instant::now(),
        })
    }

    #[cfg(no_efa)]
    pub fn new(_pool: &WorkerPool, _peer_addr: &EfaAddress) -> Result<Self, TransportError> {
        Ok(Self {
            fi_addrs: vec![],
            connected_at: Instant::now(),
        })
    }
}

// ─── Worker ──────────────────────────────────────────────────────────────────

/// One EFA worker: owns an endpoint and its registered memory regions.
/// The worker's thread drives the event loop (submit ops, poll CQ).
#[cfg(not(no_efa))]
pub struct Worker {
    /// Index of this worker (0..N)
    pub index: usize,

    /// The EFA endpoint owned by this worker (Fabric → Domain → AV → CQ → EP)
    pub endpoint: efa::EfaEndpoint,

    /// Registered local MRs (pool buffers registered on this worker's domain)
    pub local_mrs: Vec<efa::MemoryRegion>,
}

#[cfg(not(no_efa))]
impl Worker {
    fn new(index: usize, info: *mut ffi::fi_info) -> Result<Self, TransportError> {
        let endpoint = efa::EfaEndpoint::from_info(info)?;
        Ok(Self {
            index,
            endpoint,
            local_mrs: Vec::new(),
        })
    }

    fn register_buffers(&mut self, bufs: &[&[u8]]) -> Result<(), TransportError> {
        for buf in bufs {
            let mr = self
                .endpoint
                .register_local_buffer(buf.as_ptr() as *mut u8, buf.len())?;
            self.local_mrs.push(mr);
        }
        Ok(())
    }

    fn local_desc(&self, buf_idx: usize) -> Option<*mut libc::c_void> {
        self.local_mrs.get(buf_idx).map(|mr| mr.desc())
    }

    fn local_addr(&self) -> Result<EfaAddress, TransportError> {
        self.endpoint.get_local_addr().map(EfaAddress)
    }
}

// ─── WorkerPool ──────────────────────────────────────────────────────────────

/// The multi-device EFA worker pool.
/// Owns N Workers (one per EFA device) and a shared work queue.
/// Worker threads are spawned at init and stopped at shutdown.
#[allow(dead_code)]
pub struct WorkerPool {
    /// Workers (one per EFA device). Accessed at HELLO time for fi_av_insert.
    #[cfg(not(no_efa))]
    pub workers: Vec<Worker>,

    /// Send side of the shared MPMC work queue.
    tx: Sender<WorkItem>,

    /// Worker thread join handles.
    thread_handles: Vec<JoinHandle<()>>,

    /// Signal to stop worker threads.
    stop: Arc<AtomicBool>,

    /// Total number of workers (endpoints).
    device_count: usize,

    /// Number of distinct physical EFA devices discovered.
    /// On single-device instances (i8g, i8ge), this is 1 even if device_count > 1
    /// (additional workers share the same physical device for CQ isolation).
    discovered_device_count: usize,
}

/// Maximum number of EFA devices to attempt discovering.
/// Multi-EFA instances (p5: 32 cards, p4d: 4 cards, trn1: 8 cards) expose multiple
/// physical EFA devices. Storage-optimized instances (i8g.48xlarge, i8ge.48xlarge)
/// have a single network card with one EFA device (100 Gbps).
/// Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-acc-inst-types.html
/// (only p5, p4d, p5e, trn1, trn2 etc. are listed as multi-card EFA instances;
/// i8g/i8ge are EFA-capable but have 1 network card).
#[cfg(not(no_efa))]
const MAX_EFA_DEVICES: usize = 4;

/// Work queue capacity (bounded backpressure).
#[allow(dead_code)]
const QUEUE_CAPACITY: usize = 256;

/// Maximum in-flight RMA ops per worker before it stops dequeuing.
#[cfg(not(no_efa))]
const MAX_IN_FLIGHT: usize = 32;

/// CQ poll timeout.
#[cfg(not(no_efa))]
const CQ_TIMEOUT_SECS: u64 = 10;

#[cfg(not(no_efa))]
impl WorkerPool {
    /// Discover all EFA devices and create one worker per device.
    /// If transport-threads > discovered devices, creates additional workers
    /// on the last device (still useful for CQ isolation and parallelism).
    pub fn new() -> Result<Self, TransportError> {
        // Discover all unique EFA devices via fi_getinfo linked list traversal.
        // Each entry in device_infos represents a distinct physical EFA NIC
        // (deduplicated by domain name, e.g. "efa_0", "efa_1").
        let device_infos = efa::EfaEndpoint::discover_devices()?;
        let max_workers = crate::transport_threads().min(MAX_EFA_DEVICES);
        let discovered_devices = device_infos.len();

        let mut workers = Vec::new();

        // Create one worker per discovered device (up to max_workers)
        for (i, info) in device_infos.into_iter().enumerate() {
            if i >= max_workers {
                // Free unused fi_info nodes
                unsafe { ffi::fi_freeinfo(info) };
                continue;
            }
            match Worker::new(i, info) {
                Ok(worker) => workers.push(worker),
                Err(e) => {
                    if i == 0 {
                        return Err(e);
                    }
                    break;
                }
            }
        }

        // If we have fewer devices than requested workers, create additional
        // workers on new endpoints (same physical device, independent CQ/AV/EP).
        // This provides CQ isolation and parallelism even on single-device instances
        // like i8g.48xlarge and i8ge.48xlarge (which expose only one EFA device).
        while workers.len() < max_workers {
            let idx = workers.len();
            match efa::EfaEndpoint::new() {
                Ok(endpoint) => {
                    workers.push(Worker {
                        index: idx,
                        endpoint,
                        local_mrs: Vec::new(),
                    });
                }
                Err(_) => break,
            }
        }

        if workers.is_empty() {
            return Err(TransportError::DeviceNotFound);
        }

        let num_workers = workers.len();
        let (tx, _rx) = bounded(QUEUE_CAPACITY);

        Ok(Self {
            workers,
            tx,
            thread_handles: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
            device_count: num_workers,
            discovered_device_count: discovered_devices,
        })
    }

    /// Number of EFA devices (workers).
    pub fn device_count(&self) -> usize {
        self.device_count
    }

    /// Number of distinct physical EFA devices discovered via fi_getinfo.
    /// On multi-EFA instances (p5, p4d, trn1) this may be > 1.
    /// On single-EFA instances (i8g.48xlarge, i8ge.48xlarge) this is 1.
    pub fn discovered_device_count(&self) -> usize {
        self.discovered_device_count
    }

    /// Get all server EFA addresses (one per worker/device).
    pub fn server_addrs(&self) -> Vec<EfaAddress> {
        self.workers
            .iter()
            .filter_map(|w| w.local_addr().ok())
            .collect()
    }

    /// Register pool buffers on all workers' EFA domains.
    pub fn register_buffers(&mut self, bufs: &[&[u8]]) -> Result<(), TransportError> {
        for worker in &mut self.workers {
            worker.register_buffers(bufs)?;
        }
        Ok(())
    }

    /// Deregister all MRs on all workers.
    pub fn deregister_buffers(&mut self) {
        for worker in &mut self.workers {
            worker.local_mrs.clear();
        }
    }

    /// Remove a peer's AV entries from all workers.
    /// Called during session disconnect cleanup to free AV slots.
    /// fi_addrs is the per-worker fi_addr_t vector from the Session.
    pub fn remove_peer(&self, fi_addrs: &[u64]) {
        for (i, worker) in self.workers.iter().enumerate() {
            if let Some(&fi_addr) = fi_addrs.get(i) {
                worker.endpoint.remove_peer(fi_addr);
            }
        }
    }

    /// Start worker threads. Must be called after register_buffers().
    /// Workers will pull from the shared queue and execute RMA operations.
    pub fn start_workers(&mut self) {
        // Recreate the channel so workers get the Receiver end.
        let (tx, rx) = bounded(QUEUE_CAPACITY);
        self.tx = tx;
        self.stop.store(false, Ordering::Relaxed);

        for i in 0..self.workers.len() {
            let rx_clone = rx.clone();
            let stop_clone = self.stop.clone();
            let worker_index = i;

            // We need to pass the worker's endpoint and MR state to the thread.
            // Since Workers are in self.workers and we can't move them, we use
            // raw pointers. SAFETY: Workers live as long as the pool (static lifetime
            // via OnceLock). The thread is joined before the pool is dropped.
            let worker_ptr = &self.workers[i] as *const Worker as usize;

            let handle = std::thread::Builder::new()
                .name(format!("efa-worker-{}", i))
                .spawn(move || {
                    worker_event_loop(worker_ptr, worker_index, rx_clone, stop_clone);
                })
                .expect("failed to spawn EFA worker thread");

            self.thread_handles.push(handle);
        }
    }

    /// Submit a work item to the shared queue.
    pub fn submit(&self, item: WorkItem) -> Result<(), TransportError> {
        self.tx.try_send(item).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(item) => {
                // Return the buffer via the callback so it's not leaked
                (item.on_complete)(item.buf, Err(TransportError::QueueFull));
                TransportError::QueueFull
            }
            crossbeam_channel::TrySendError::Disconnected(item) => {
                (item.on_complete)(item.buf, Err(TransportError::Unavailable));
                TransportError::Unavailable
            }
        })
    }

    /// Shutdown: signal workers to stop and join threads.
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Drop sender to unblock workers waiting on recv()
        let (dead_tx, _dead_rx) = bounded(1);
        self.tx = dead_tx;
        for handle in self.thread_handles.drain(..) {
            let _ = handle.join();
        }
        for worker in &mut self.workers {
            worker.local_mrs.clear();
        }
    }
}

/// In-flight operation metadata. Stored while an RMA op is posted but not yet completed.
/// The context pointer passed to fi_write/fi_read points to a heap-allocated InFlightOp,
/// which is recovered from the CQ entry on completion.
#[cfg(not(no_efa))]
struct InFlightOp {
    buf: Buffer,
    on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
}

/// Worker event loop — runs on a dedicated thread.
/// Uses batched CQ polling: posts up to MAX_IN_FLIGHT RMA ops before reaping
/// completions. This saturates EFA bandwidth under load while maintaining
/// low latency when idle (single item is still processed immediately).
#[cfg(not(no_efa))]
fn worker_event_loop(
    worker_ptr: usize,
    worker_index: usize,
    rx: Receiver<WorkItem>,
    stop: Arc<AtomicBool>,
) {
    // SAFETY: The worker lives as long as the WorkerPool (static OnceLock).
    // This thread is joined before the pool is dropped.
    let worker = unsafe { &*(worker_ptr as *const Worker) };

    let mut in_flight: usize = 0;

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        // Phase 1: Post new ops until we hit MAX_IN_FLIGHT or queue is empty.
        // Use non-blocking try_recv when we already have in-flight ops to avoid stalling.
        // Use blocking recv_timeout when idle so we don't busy-spin.
        if in_flight == 0 {
            // Nothing in flight — block until work arrives (with timeout for stop check)
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(item) => {
                    post_work_item(worker, worker_index, item);
                    in_flight += 1;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Drain more items from the queue (non-blocking) up to MAX_IN_FLIGHT
        while in_flight < MAX_IN_FLIGHT {
            match rx.try_recv() {
                Ok(item) => {
                    post_work_item(worker, worker_index, item);
                    in_flight += 1;
                }
                Err(_) => break,
            }
        }

        // Phase 2: Poll CQ to reap completions.
        // Poll in a loop until at least one completion arrives or timeout.
        let deadline = Instant::now() + Duration::from_secs(CQ_TIMEOUT_SECS);
        loop {
            match worker.endpoint.poll_cq(in_flight) {
                Ok(contexts) if contexts.is_empty() => {
                    // No completions yet — check timeout
                    if Instant::now() > deadline {
                        // Timeout all remaining in-flight ops (shouldn't happen in practice)
                        // We can't recover the contexts here without additional tracking,
                        // so just reset count. The InFlightOp memory will leak in this
                        // pathological case (acceptable: indicates hardware failure).
                        in_flight = 0;
                        break;
                    }
                    std::hint::spin_loop();
                }
                Ok(contexts) => {
                    // Complete each reaped operation
                    for ctx_ptr in contexts {
                        // SAFETY: ctx_ptr was allocated by post_work_item via Box::into_raw
                        let op = unsafe { Box::from_raw(ctx_ptr as *mut InFlightOp) };
                        (op.on_complete)(op.buf, Ok(()));
                        in_flight -= 1;
                    }
                    // Break out of poll loop — we can try to post more work now
                    break;
                }
                Err(e) => {
                    // CQ error — one op failed. We don't know which one from the error
                    // alone (EFA CQ errors may not carry context). Decrement in_flight
                    // and continue. In practice EFA CQ errors are rare (hardware fault).
                    in_flight = in_flight.saturating_sub(1);
                    // Log the error but keep the worker alive
                    let _ = e;
                    break;
                }
            }
        }
    }

    // Drain: on shutdown, complete remaining in-flight ops with Unavailable.
    // We can't recover heap-allocated InFlightOps without the CQ reporting them,
    // so just do a final poll sweep.
    let _ = drain_remaining_completions(worker, &mut in_flight);
}

/// Final poll sweep on shutdown — try to reap any remaining completions.
#[cfg(not(no_efa))]
fn drain_remaining_completions(worker: &Worker, in_flight: &mut usize) {
    let deadline = Instant::now() + Duration::from_millis(500);
    while *in_flight > 0 && Instant::now() < deadline {
        match worker.endpoint.poll_cq(*in_flight) {
            Ok(contexts) => {
                for ctx_ptr in contexts {
                    let op = unsafe { Box::from_raw(ctx_ptr as *mut InFlightOp) };
                    (op.on_complete)(op.buf, Err(TransportError::Unavailable));
                    *in_flight -= 1;
                }
                if *in_flight == 0 {
                    break;
                }
            }
            Err(_) => {
                *in_flight = 0;
                break;
            }
        }
        std::hint::spin_loop();
    }
}

/// Post a single RMA operation (non-blocking). Allocates an InFlightOp on the heap
/// whose pointer serves as the CQ context, recovered on completion.
#[cfg(not(no_efa))]
fn post_work_item(worker: &Worker, worker_index: usize, item: WorkItem) {
    let WorkItem {
        op,
        fi_addrs,
        buf,
        len,
        remote_addr,
        rkey,
        on_complete,
    } = item;

    // Use the fi_addr for THIS worker's device
    let peer = fi_addrs[worker_index];

    // Get local descriptor for this buffer
    let local_desc = worker
        .local_desc(buf.idx() as usize)
        .unwrap_or(std::ptr::null_mut());

    // Extract the buffer pointer before moving buf into the InFlightOp
    let buf_raw_ptr = buf.ptr();

    // Allocate in-flight metadata on the heap — pointer becomes the CQ context
    let inflight = Box::new(InFlightOp { buf, on_complete });
    let ctx_ptr = Box::into_raw(inflight) as *mut libc::c_void;

    let result = match op {
        RmaOp::Write => worker.endpoint.post_write(
            peer,
            buf_raw_ptr as *const u8,
            len,
            local_desc,
            remote_addr,
            rkey,
            ctx_ptr,
        ),
        RmaOp::Read => worker.endpoint.post_read(
            peer,
            buf_raw_ptr,
            len,
            local_desc,
            remote_addr,
            rkey,
            ctx_ptr,
        ),
    };

    if let Err(e) = result {
        // Post failed — recover the InFlightOp and report error immediately
        let op = unsafe { Box::from_raw(ctx_ptr as *mut InFlightOp) };
        (op.on_complete)(op.buf, Err(e));
    }
}

// ─── no_efa stubs ────────────────────────────────────────────────────────────

#[cfg(no_efa)]
impl WorkerPool {
    pub fn new() -> Result<Self, TransportError> {
        Err(TransportError::DeviceNotFound)
    }

    pub fn device_count(&self) -> usize {
        self.device_count
    }
    pub fn discovered_device_count(&self) -> usize {
        self.discovered_device_count
    }
    pub fn server_addrs(&self) -> Vec<EfaAddress> {
        vec![]
    }
    pub fn register_buffers(&mut self, _bufs: &[&[u8]]) -> Result<(), TransportError> {
        Ok(())
    }
    pub fn deregister_buffers(&mut self) {}
    pub fn start_workers(&mut self) {}
    pub fn submit(&self, item: WorkItem) -> Result<(), TransportError> {
        (item.on_complete)(item.buf, Err(TransportError::Unavailable));
        Err(TransportError::Unavailable)
    }
    pub fn remove_peer(&self, _fi_addrs: &[u64]) {}
    pub fn shutdown(&mut self) {}
}

// ─── Global Transport State ──────────────────────────────────────────────────

use std::cell::UnsafeCell;

/// Wrapper to allow one-time mutation of the WorkerPool during init.
/// SAFETY: register_buffers/start_workers/shutdown are only called from
/// single-threaded module init/deinit paths, never concurrently with runtime access.
struct PoolCell(UnsafeCell<Option<WorkerPool>>);
unsafe impl Sync for PoolCell {}

static POOL: OnceLock<PoolCell> = OnceLock::new();

/// Whether EFA is available (set during init).
static EFA_AVAILABLE: OnceLock<bool> = OnceLock::new();

/// Initialize transport. Called once at module startup.
pub fn init() {
    let pool = match WorkerPool::new() {
        Ok(pool) => {
            EFA_AVAILABLE.set(true).ok();
            Some(pool)
        }
        Err(_) => {
            EFA_AVAILABLE.set(false).ok();
            None
        }
    };
    POOL.set(PoolCell(UnsafeCell::new(pool))).ok();
}

/// Check if EFA transport is available.
pub fn is_available() -> bool {
    *EFA_AVAILABLE.get().unwrap_or(&false)
}

/// Get the global WorkerPool (immutable reference for runtime use).
pub fn worker_pool() -> Option<&'static WorkerPool> {
    POOL.get()
        .and_then(|cell| unsafe { (*cell.0.get()).as_ref() })
}

/// Get the global WorkerPool (mutable reference — ONLY for init/deinit).
/// SAFETY: Only called from single-threaded init/deinit paths.
fn worker_pool_mut() -> Option<&'static mut WorkerPool> {
    POOL.get()
        .and_then(|cell| unsafe { (*cell.0.get()).as_mut() })
}

/// Register pool buffers with all EFA devices. Called once after storage::init().
pub fn register_buffers(bufs: &[&[u8]]) {
    if let Some(pool) = worker_pool_mut() {
        let _ = pool.register_buffers(bufs);
        pool.start_workers();
    }
}

pub fn deregister_buffers() {
    if let Some(pool) = worker_pool_mut() {
        pool.deregister_buffers();
    }
}

pub fn shutdown() {
    if let Some(pool) = worker_pool_mut() {
        pool.shutdown();
    }
}

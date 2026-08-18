//! Transport Layer — EFA/libfabric RMA (one-sided RDMA).
//!
//! Lifecycle:
//!   init()              → discover EFA, create fabric/domain/endpoint
//!   register_buffers()  → fi_mr_reg pool buffers for local access
//!   Session::new()      → fi_av_insert peer, store client regions
//!   Session::write()    → fi_write (server buf → client GPU)
//!   Session::read()     → fi_read  (client GPU → server buf)
//!   shutdown()          → close endpoint, deregister MRs
//!
//! Transport never calls storage or data type.
//! Module owns the tokio runtime; transport uses it for CQ progress tasks.

use std::sync::OnceLock;

use crate::storage::Buffer;

// ─── Conditional compilation ─────────────────────────────────────────────────
// When libfabric is not available (dev desktops), build.rs sets cfg(no_efa).
// In that case, ffi and efa modules are not compiled, and EfaContext reports
// unavailable.

#[cfg(not(no_efa))]
mod ffi;
#[cfg(not(no_efa))]
mod efa;

// ─── EFA Types ───────────────────────────────────────────────────────────────

/// EFA endpoint address — 32 bytes, opaque to callers.
/// Contains GID (16B) + QPN (2B) + pad (2B) + ConnID (4B) + reserved (8B).
/// Obtained via fi_getname(). Exchanged during LO.HELLO.
#[derive(Clone)]
pub struct EfaAddress(pub [u8; 32]);

/// Client-side memory region descriptor.
/// Received during LO.HELLO. One per GPU memory pool (1-8 total, NOT per object).
#[derive(Debug, Clone)]
pub struct ClientRegion {
    pub rkey: u64,
    pub remote_addr: u64,
    pub len: u64,
}

// ─── Error Types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TransportError {
    DeviceNotFound,
    RegistrationFailed,
    SessionCreateFailed,
    WriteFailed { code: i32 },
    ReadFailed { code: i32 },
    RegionOutOfBounds,
    Timeout,
    SessionClosed,
    Unavailable,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeviceNotFound => write!(f, "EFA device not found"),
            Self::RegistrationFailed => write!(f, "fi_mr_reg failed"),
            Self::SessionCreateFailed => write!(f, "session create failed"),
            Self::WriteFailed { code } => write!(f, "fi_write failed ({})", code),
            Self::ReadFailed { code } => write!(f, "fi_read failed ({})", code),
            Self::RegionOutOfBounds => write!(f, "region index out of bounds"),
            Self::Timeout => write!(f, "CQ poll timeout"),
            Self::SessionClosed => write!(f, "session closed"),
            Self::Unavailable => write!(f, "EFA unavailable"),
        }
    }
}

// ─── EfaContext ──────────────────────────────────────────────────────────────

/// Global EFA context — endpoint + registered MRs.
pub struct EfaContext {
    available: bool,
    #[cfg(not(no_efa))]
    endpoint: Option<efa::EfaEndpoint>,
    #[cfg(not(no_efa))]
    local_mrs: Vec<efa::MemoryRegion>,
}

// SAFETY: EfaContext is stored in a OnceLock and accessed from multiple threads.
// The underlying EfaEndpoint is thread-safe (FI_THREAD_SAFE domain).
unsafe impl Send for EfaContext {}
unsafe impl Sync for EfaContext {}

impl EfaContext {
    /// Discover EFA devices and create endpoint. Returns Ok with available=false
    /// if no EFA hardware is present (graceful TCP-only fallback).
    #[cfg(not(no_efa))]
    pub fn new() -> Self {
        match efa::EfaEndpoint::new() {
            Ok(endpoint) => {
                // NOTE: No CQ progress thread on the server side. The server
                // initiates fi_write/fi_read and drives progress via wait_cq()
                // polling during each operation. A progress thread would race
                // with wait_cq for CQ completions, causing stolen completions.
                //
                // Client-side progress (for receiving RMA operations) is handled
                // by the client application, not this module.
                EfaContext {
                    available: true,
                    endpoint: Some(endpoint),
                    local_mrs: Vec::new(),
                }
            }
            Err(_) => EfaContext {
                available: false,
                endpoint: None,
                local_mrs: Vec::new(),
            },
        }
    }

    /// Fallback constructor when EFA is not compiled in.
    #[cfg(no_efa)]
    pub fn new() -> Self {
        EfaContext { available: false }
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn device_count(&self) -> usize {
        if self.available { 1 } else { 0 }
    }

    /// Register pool buffers with EFA domain for local access (fi_mr_reg).
    /// Called during module init after storage allocates the buffer pool.
    #[cfg(not(no_efa))]
    pub fn register_buffers(&mut self, bufs: &[&[u8]]) -> Result<(), TransportError> {
        if !self.available {
            return Ok(());
        }
        let ep = self.endpoint.as_ref().ok_or(TransportError::Unavailable)?;
        for buf in bufs {
            let mr = ep.register_local_buffer(buf.as_ptr() as *mut u8, buf.len())?;
            self.local_mrs.push(mr);
        }
        Ok(())
    }

    #[cfg(no_efa)]
    pub fn register_buffers(&mut self, _bufs: &[&[u8]]) -> Result<(), TransportError> {
        Ok(())
    }

    pub fn deregister_buffers(&mut self) -> Result<(), TransportError> {
        #[cfg(not(no_efa))]
        {
            self.local_mrs.clear(); // Drop triggers fi_close(mr)
        }
        Ok(())
    }

    /// Get the local descriptor for a registered buffer by index.
    #[cfg(not(no_efa))]
    pub fn local_desc(&self, buf_idx: usize) -> Option<*mut libc::c_void> {
        self.local_mrs.get(buf_idx).map(|mr| mr.desc())
    }

    /// Get the EFA endpoint reference (for Session creation).
    #[cfg(not(no_efa))]
    pub fn endpoint(&self) -> Option<&efa::EfaEndpoint> {
        self.endpoint.as_ref()
    }

    /// Get the server's EFA address (for LO.HELLO reply).
    #[cfg(not(no_efa))]
    pub fn local_addr(&self) -> Option<EfaAddress> {
        self.endpoint.as_ref().and_then(|ep| {
            ep.get_local_addr().ok().map(EfaAddress)
        })
    }

    #[cfg(no_efa)]
    pub fn local_addr(&self) -> Option<EfaAddress> {
        None
    }

    pub fn shutdown(&mut self) {
        #[cfg(not(no_efa))]
        {
            self.local_mrs.clear();
            self.endpoint.take(); // Drop closes fabric resources
        }
    }
}

// ─── Session ─────────────────────────────────────────────────────────────────

/// Per-client DMA session. Created during LO.HELLO.
pub struct Session {
    pub client_regions: Vec<ClientRegion>,
    #[cfg(not(no_efa))]
    peer_fi_addr: u64,
}

impl Session {
    /// Create a session: fi_av_insert peer, store client regions.
    #[cfg(not(no_efa))]
    pub fn new(
        ctx: &EfaContext,
        peer_addr: &EfaAddress,
        client_regions: Vec<ClientRegion>,
    ) -> Result<Self, TransportError> {
        let ep = ctx.endpoint().ok_or(TransportError::Unavailable)?;
        let peer_fi_addr = ep.insert_peer(&peer_addr.0)?;
        Ok(Self {
            client_regions,
            peer_fi_addr,
        })
    }

    #[cfg(no_efa)]
    pub fn new(
        _ctx: &EfaContext,
        _peer_addr: &EfaAddress,
        client_regions: Vec<ClientRegion>,
    ) -> Result<Self, TransportError> {
        Ok(Self { client_regions })
    }

    /// Server EFA addresses to return in LO.HELLO reply.
    pub fn server_addrs(&self) -> Vec<EfaAddress> {
        // Return the global context's local addr
        if let Some(addr) = efa_context().local_addr() {
            vec![addr]
        } else {
            vec![]
        }
    }

    /// DMA write: server buffer → client region (fi_write).
    /// Takes Buffer ownership during DMA. Returns it in callback.
    /// `region_idx` selects which ClientRegion (rkey + base addr).
    #[cfg(not(no_efa))]
    pub fn write(
        &self,
        buf: Buffer,
        len: usize,
        region_idx: u32,
        remote_offset: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    ) {
        if region_idx as usize >= self.client_regions.len() {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }

        let region = &self.client_regions[region_idx as usize];

        // Bounds check: ensure write stays within declared region
        if remote_offset.saturating_add(len as u64) > region.len {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }

        let target_addr = region.remote_addr + remote_offset;

        // Get local descriptor for this buffer (use index 0 for now — all pool
        // buffers share a single large registration in production, but for the
        // POC we register each buffer individually).
        let ctx = efa_context();
        let local_desc = ctx.local_desc(buf.idx() as usize).unwrap_or(ptr::null_mut());

        let ep = match ctx.endpoint() {
            Some(ep) => ep,
            None => {
                on_complete(buf, Err(TransportError::Unavailable));
                return;
            }
        };

        let result = ep.rma_write(
            self.peer_fi_addr,
            buf.ptr(),
            len,
            local_desc,
            target_addr,
            region.rkey,
        );

        on_complete(buf, result);
    }

    #[cfg(no_efa)]
    pub fn write(
        &self,
        buf: Buffer,
        _len: usize,
        region_idx: u32,
        _remote_offset: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    ) {
        if region_idx as usize >= self.client_regions.len() {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }
        on_complete(buf, Err(TransportError::Unavailable));
    }

    /// DMA read: client region → server buffer (fi_read).
    /// Takes Buffer ownership. Returns it in callback.
    #[cfg(not(no_efa))]
    pub fn read(
        &self,
        buf: Buffer,
        len: usize,
        region_idx: u32,
        remote_offset: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    ) {
        if region_idx as usize >= self.client_regions.len() {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }

        let region = &self.client_regions[region_idx as usize];

        // Bounds check: ensure read stays within declared region
        if remote_offset.saturating_add(len as u64) > region.len {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }

        let source_addr = region.remote_addr + remote_offset;

        let ctx = efa_context();
        let local_desc = ctx.local_desc(buf.idx() as usize).unwrap_or(ptr::null_mut());

        let ep = match ctx.endpoint() {
            Some(ep) => ep,
            None => {
                on_complete(buf, Err(TransportError::Unavailable));
                return;
            }
        };

        let result = ep.rma_read(
            self.peer_fi_addr,
            buf.ptr(),
            len,
            local_desc,
            source_addr,
            region.rkey,
        );

        on_complete(buf, result);
    }

    #[cfg(no_efa)]
    pub fn read(
        &self,
        buf: Buffer,
        _len: usize,
        region_idx: u32,
        _remote_offset: u64,
        on_complete: Box<dyn FnOnce(Buffer, Result<(), TransportError>) + Send>,
    ) {
        if region_idx as usize >= self.client_regions.len() {
            on_complete(buf, Err(TransportError::RegionOutOfBounds));
            return;
        }
        on_complete(buf, Err(TransportError::Unavailable));
    }

    /// Tear down session.
    pub fn close(self) {
        // AV entries are cleaned up when the endpoint is dropped.
        // Future: remove AV entry for this peer specifically.
    }
}

// ─── Global Transport State ──────────────────────────────────────────────────

use std::cell::UnsafeCell;

/// Wrapper to allow one-time mutation of the EfaContext during init.
/// SAFETY: register_buffers/deregister_buffers/shutdown are only called from
/// single-threaded module init/deinit paths, never concurrently with runtime access.
struct EfaCtxCell(UnsafeCell<EfaContext>);
unsafe impl Sync for EfaCtxCell {}

static EFA_CTX: OnceLock<EfaCtxCell> = OnceLock::new();

/// Initialize transport. Called once at module startup.
pub fn init() {
    let ctx = EfaContext::new();
    EFA_CTX.set(EfaCtxCell(UnsafeCell::new(ctx))).ok();
}

/// Get the global EFA context (immutable reference for runtime use).
pub fn efa_context() -> &'static EfaContext {
    unsafe { &*EFA_CTX.get().expect("transport not initialized").0.get() }
}

/// Register pool buffers with EFA. Called once after storage::init().
/// SAFETY: Called from single-threaded module init, before any commands execute.
pub fn register_buffers(bufs: &[&[u8]]) {
    let cell = EFA_CTX.get().expect("transport not initialized");
    let ctx = unsafe { &mut *cell.0.get() };
    let _ = ctx.register_buffers(bufs);
}

pub fn deregister_buffers() {
    let cell = EFA_CTX.get().expect("transport not initialized");
    let ctx = unsafe { &mut *cell.0.get() };
    let _ = ctx.deregister_buffers();
}

pub fn shutdown() {
    let cell = EFA_CTX.get().expect("transport not initialized");
    let ctx = unsafe { &mut *cell.0.get() };
    ctx.shutdown();
}

// Re-export std::ptr for use in Session methods
#[cfg(not(no_efa))]
use std::ptr;

//! Transport Crate API (libefa-rs)
//!
//! This is a place holder mod which will be replaced by an actual dependency.
//! EFA/libfabric lifecycle, multi-device LB, completion handling.
//! Transport never calls storage or data type.
//! Module owns the tokio runtime; transport borrows the handle for CQ poller tasks.

use std::sync::OnceLock;

use crate::storage::Buffer;

// ─── EFA Types ───────────────────────────────────────────────────────────────

/// EFA endpoint address — 32 bytes, opaque to callers.
/// Contains GID (16B) + QPN (2B) + pad (2B) + QKEY (4B).
/// Obtained via fi_getname(). Exchanged during LO.HELLO.
#[derive(Clone)]
pub struct EfaAddress(pub [u8; 32]);

/// Client-side memory region descriptor.
/// Received during LO.HELLO. One per GPU memory pool (1-8 total, NOT per object).
#[derive(Debug, Clone)]
pub struct ClientRegion {
    pub rkey: u64,        // remote key (fi_write takes uint64_t key)
    pub remote_addr: u64, // base virtual address of the region on the client
    pub len: u64,         // total length of the region
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
    Unavailable, // EFA not present on this instance
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

/// Global EFA context — fabric + domain per device, registered MRs.
pub struct EfaContext {
    available: bool,
    device_count: usize,
    // TODO: fi_fabric, fi_domain, fi_eq handles per device
    // TODO: registered MR list
}

impl EfaContext {
    /// Discover EFA devices, create fabric + domain per device.
    /// Synchronous — no runtime needed.
    pub fn new() -> Result<Self, TransportError> {
        // TODO: Actual EFA discovery via fi_getinfo("efa", ...)
        //   1. fi_getinfo with hints (provider="efa", ep_type=FI_EP_RDM, caps=FI_RMA)
        //   2. fi_fabric() per returned info
        //   3. fi_domain() per fabric
        //
        // For now, return Unavailable (no EFA on dev desktop).
        Ok(Self {
            available: false,
            device_count: 0,
        })
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn device_count(&self) -> usize {
        self.device_count
    }

    /// Register pool buffers with all EFA domains (fi_mr_reg).
    pub fn register_buffers(&self, _bufs: &[&[u8]]) -> Result<(), TransportError> {
        if !self.available {
            return Ok(()); // No-op if no EFA
        }
        // TODO: fi_mr_reg each buffer across all domains.
        // Store MR descriptors for per-op fi_write/fi_read.
        Ok(())
    }

    pub fn deregister_buffers(&self) -> Result<(), TransportError> {
        if !self.available {
            return Ok(());
        }
        // TODO: fi_mr_dereg all registered MRs.
        Ok(())
    }

    pub fn shutdown(self) {
        // TODO: fi_close domains, fi_close fabrics.
    }
}

// ─── Session ─────────────────────────────────────────────────────────────────

/// Per-client DMA session. Created during LO.HELLO.
pub struct Session {
    pub client_regions: Vec<ClientRegion>,
    // TODO: fi_endpoint per EFA device, AV entries, LB state
}

impl Session {
    /// Create a session: fi_av_insert peer, store client regions.
    pub fn new(
        _ctx: &EfaContext,
        _peer_addr: &EfaAddress,
        client_regions: Vec<ClientRegion>,
    ) -> Result<Self, TransportError> {
        // TODO: fi_endpoint creation, fi_av_insert(peer_addr)
        Ok(Self { client_regions })
    }

    /// Server EFA addresses to return in LO.HELLO reply.
    pub fn server_addrs(&self) -> Vec<EfaAddress> {
        // TODO: fi_getname() on each endpoint
        vec![]
    }

    /// DMA write: server buffer → client region.
    /// Non-blocking. region_idx selects which ClientRegion (resolves to rkey + base addr).
    /// Takes Buffer ownership during DMA. Returns it in callback.
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
        // TODO: Post fi_writemsg, CQ poller fires on_complete.
        on_complete(buf, Ok(()));
    }

    /// DMA read: client region → server buffer.
    /// Non-blocking. Takes Buffer ownership. Returns it in callback.
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
        // TODO: Post fi_readmsg, CQ poller fires on_complete.
        on_complete(buf, Ok(()));
    }

    /// Tear down session. In-flight ops receive SessionClosed.
    pub fn close(self) {
        // TODO: fi_close endpoints, remove AV entries.
        // Signal in-flight ops with SessionClosed error.
    }
}

// ─── Global Transport State ──────────────────────────────────────────────────

static EFA_CTX: OnceLock<EfaContext> = OnceLock::new();

pub fn init() {
    match EfaContext::new() {
        Ok(ctx) => {
            EFA_CTX.set(ctx).ok();
        }
        Err(_) => {
            // EFA unavailable — module works in TCP-only mode.
            EFA_CTX
                .set(EfaContext {
                    available: false,
                    device_count: 0,
                })
                .ok();
        }
    }
}

pub fn efa_context() -> &'static EfaContext {
    EFA_CTX.get().expect("transport not initialized")
}

pub fn register_buffers(bufs: &[&[u8]]) {
    let _ = efa_context().register_buffers(bufs);
}

pub fn deregister_buffers() {
    let _ = efa_context().deregister_buffers();
}

pub fn shutdown() {
    // EfaContext::shutdown() consumes self — can't call on static ref.
    // TODO: Use Option<EfaContext> or OnceLock::take() when stabilized.
}

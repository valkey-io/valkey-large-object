//! `LO.HELLO` creates a session keyed by Valkey client id, dropped on disconnect.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use linkme::distributed_slice;

use crate::transport::{fabric, TransportError};

// ─── Session ─────────────────────────────────────────────────────────────────

/// Per-client DMA session created during LO.HELLO
pub struct Session {
    /// The client's fabric address from LO.HELLO.
    peer_address: Vec<u8>,
}

impl Session {
    pub fn new(peer_address: Vec<u8>) -> Self {
        Self { peer_address }
    }

    pub fn peer_address(&self) -> &[u8] {
        &self.peer_address
    }

    /// DMA write: Push server buffer into client memory at (rkey, remote_addr).
    /// Non-blocking, completion fires in on_complete.
    ///
    /// SAFETY: `buf_ptr` must remain valid until `on_complete` is called.
    /// The caller (tokio task) must hold the owning SegmentBuffer/StreamingContext alive
    /// until the callback fires. The transport does not own the buffer.
    pub fn write(
        &self,
        buf_ptr: *mut u8,
        _len: usize,
        _rkey: u64,
        _remote_addr: u64,
        on_complete: Box<dyn FnOnce(*mut u8, Result<(), TransportError>) + Send>,
    ) {
        // TODO:
        //   1. Pick device (least-loaded)
        //   2. fi_write(ep, buf.ptr(), len, desc, dest_fi_addr[device], remote_addr, rkey, ctx)
        //   3. CQ poller fires on_complete
        on_complete(buf_ptr, Ok(()));
    }

    /// DMA read: Pull client memory at (rkey, remote_addr) into server's buf_ptr.
    /// Non-blocking.
    ///
    /// SAFETY: `buf_ptr` must remain valid until `on_complete` is called.
    /// The caller (tokio task) must hold the owning SegmentBuffer/StreamingContext alive
    /// until the callback fires. The transport does not own the buffer.
    pub fn read(
        &self,
        buf_ptr: *mut u8,
        _len: usize,
        _rkey: u64,
        _remote_addr: u64,
        on_complete: Box<dyn FnOnce(*mut u8, Result<(), TransportError>) + Send>,
    ) {
        // TODO:
        //   1. Pick device (least-loaded)
        //   2. fi_read(ep, buf.ptr(), len, desc, dest_fi_addr[device], remote_addr, rkey, ctx)
        //   3. CQ poller fires on_complete
        on_complete(buf_ptr, Ok(()));
    }

    /// Tear down session. In-flight ops receive SessionClosed.
    pub fn close(self) {
        // TODO: fi_close endpoints, remove AV entries.
        // Signal in-flight ops with SessionClosed error.
    }
}

// ─── Per-Client Session Store ────────────────────────────────────────────────

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<u64, Arc<Session>>> = Mutex::new(HashMap::new());
}

fn sessions() -> MutexGuard<'static, HashMap<u64, Arc<Session>>> {
    SESSIONS.lock().expect("SESSIONS lock unavailable")
}

/// Bind a fresh session to the client that ran LO.HELLO, replacing on collision.
pub fn insert(client_id: u64, session: Session) {
    if sessions().insert(client_id, Arc::new(session)).is_some() {
        valkey_module::logging::log_debug(format!("replaced session for client_id: {client_id}"));
    }
}

pub fn lookup(client_id: u64) -> Option<Arc<Session>> {
    sessions().get(&client_id).cloned()
}

/// Remove a client's EFA session on disconnect, and let the fabric services drop its
/// address-vector entries once its transfers drain.
/// Valkey calls this on client disconnect.
#[distributed_slice(valkey_module::server_events::CLIENT_CHANGED_SERVER_EVENTS_LIST)]
fn on_client_change(
    ctx: &valkey_module::Context,
    subevent: valkey_module::server_events::ClientChangeSubevent,
) {
    if subevent == valkey_module::server_events::ClientChangeSubevent::Disconnected {
        if let Some(_removed) = sessions().remove(&ctx.get_client_id()) {
            ctx.log_debug(&format!(
                "rdma session terminating for client {}",
                ctx.get_client_id()
            ));
            if let Some(fabric) = fabric::fabric() {
                fabric.remove_peer(ctx.get_client_id());
            }
        }
    }
}

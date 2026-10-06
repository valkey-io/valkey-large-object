//! `BLOB.HELLO` creates a session keyed by Valkey client id, dropped on disconnect.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use dma_libfabric::asynchronous::Transfer;
use dma_libfabric::{Direction, TransferRequest};
use dma_libfabric_protocol::DmaError;
use linkme::distributed_slice;

use crate::transport::fabric;
use crate::transport::operand::PoolOperand;

// ─── Session ─────────────────────────────────────────────────────────────────

/// Per-client DMA session created during BLOB.HELLO
pub struct Session {
    /// The Valkey client id: the fabric keys address-vector entries and in-flight accounting by it.
    client_id: u64,
    /// The client's fabric address from BLOB.HELLO.
    peer_address: Vec<u8>,
}

impl Session {
    pub fn new(client_id: u64, peer_address: Vec<u8>) -> Self {
        Self {
            client_id,
            peer_address,
        }
    }

    pub fn peer_address(&self) -> &[u8] {
        &self.peer_address
    }

    /// DMA write: push `len` bytes at `buf_ptr` into client memory at (rkey, remote_addr).
    /// Resolves to the transfer's outcome. The caller holds the owning SegmentBuffer or
    /// StreamingContext alive until then, since the worker reads the buffer across the await.
    pub fn write(
        &self,
        buf_ptr: *mut u8,
        len: usize,
        rkey: u64,
        remote_addr: u64,
    ) -> Result<Transfer<PoolOperand>, DmaError> {
        self.transfer(Direction::ToPeer, buf_ptr, len, rkey, remote_addr, false)
    }

    /// DMA read: pull `len` bytes from client memory at (rkey, remote_addr) into `buf_ptr`, with
    /// the checksum of what landed taken on the fabric's CRC pool rather than the reply path.
    /// Same buffer-lifetime rule as `write`.
    pub fn read(
        &self,
        buf_ptr: *mut u8,
        len: usize,
        rkey: u64,
        remote_addr: u64,
    ) -> Result<Transfer<PoolOperand>, DmaError> {
        self.transfer(
            Direction::FromPeer { length: len },
            buf_ptr,
            len,
            rkey,
            remote_addr,
            true,
        )
    }

    fn transfer(
        &self,
        direction: Direction,
        buf_ptr: *mut u8,
        len: usize,
        rkey: u64,
        remote_addr: u64,
        want_checksum: bool,
    ) -> Result<Transfer<PoolOperand>, DmaError> {
        // A session only exists while a fabric does; it went away at shutdown.
        let fabric =
            fabric::fabric().ok_or_else(|| DmaError::Fabric("fabric is shut down".into()))?;
        fabric.transfer(TransferRequest {
            client_id: self.client_id,
            peer_address: self.peer_address.clone(),
            remote_key: rkey,
            remote_address: remote_addr,
            direction,
            want_checksum,
            caller_context: PoolOperand::new(buf_ptr, len),
            parent_id: None,
        })
    }
}

// ─── Per-Client Session Store ────────────────────────────────────────────────

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<u64, Arc<Session>>> = Mutex::new(HashMap::new());
}

fn sessions() -> MutexGuard<'static, HashMap<u64, Arc<Session>>> {
    SESSIONS.lock().expect("SESSIONS lock unavailable")
}

/// Bind a fresh session to the client that ran BLOB.HELLO, replacing on collision.
pub fn insert(client_id: u64, session: Session) {
    if sessions().insert(client_id, Arc::new(session)).is_some() {
        valkey_module::logging::log_debug(format!("replaced session for client_id: {client_id}"));
    }
}

pub fn lookup(client_id: u64) -> Option<Arc<Session>> {
    sessions().get(&client_id).cloned()
}

/// Live sessions: connections that ran BLOB.HELLO and haven't disconnected.
pub fn count() -> usize {
    sessions().len()
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

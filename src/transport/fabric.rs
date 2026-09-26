//! The fabric services: one `asynchronous::FabricService` per libfabric domain, started at module
//! load and dropped at shutdown. Pool segments are registered on them at load.
//! `BLOB.HELLO` inserts the client's address on all of them. On disconnect the client's entry on
//! each is released.

use std::sync::{Arc, Mutex, MutexGuard};

use dma_libfabric::asynchronous::{FabricService, Transfer};
use dma_libfabric::{discover_domains, Configuration, MemoryRegion, Pool, TransferRequest};
use dma_libfabric_protocol::DmaError;
use rand::RngExt;

use crate::transport::operand::PoolOperand;

pub struct Fabric {
    services: Vec<FabricService<PoolOperand>>,
    /// The registered pool segments, alive for the fabric's lifetime. Segment memory is stable for
    /// the module's lifetime.
    regions: Vec<MemoryRegion<&'static [u8]>>,
    /// Runs checksummed completions off the fabric workers. Held so it outlives every service.
    _pool: Arc<Pool>,
}

impl std::fmt::Debug for Fabric {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Fabric")
            .field("services", &self.services.len())
            .finish()
    }
}

impl Fabric {
    /// Open one service per domain: the configured allowlist, or every domain the provider
    /// exposes. Blocks until each endpoint is up, and one failing fails the lot.
    pub fn start(configuration: &Configuration) -> Result<Self, String> {
        let domains = if configuration.interfaces.is_empty() {
            discover_domains(configuration).map_err(|error| error.to_string())?
        } else {
            configuration.interfaces.clone()
        };
        if domains.is_empty() {
            return Err("no fabric domains found".to_string());
        }
        let threads = configuration.crc_pool_threads.unwrap_or(1);
        let pool = Arc::new(Pool::new(threads).map_err(|error| format!("crc pool: {error}"))?);
        let mut services = Vec::with_capacity(domains.len());
        for domain in domains {
            let pinned = Configuration {
                interfaces: vec![domain.clone()],
                ..configuration.clone()
            };
            let service = FabricService::start(&pinned, Arc::clone(&pool))
                .map_err(|error| format!("domain {domain}: {error}"))?;
            services.push(service);
        }
        Ok(Self {
            services,
            regions: Vec::new(),
            _pool: pool,
        })
    }

    pub fn service_count(&self) -> usize {
        self.services.len()
    }

    /// Submit on the best of two services picked at random, by transfers outstanding.
    pub fn transfer(
        &self,
        request: TransferRequest<PoolOperand>,
    ) -> Result<Transfer<PoolOperand>, DmaError> {
        let mut rng = rand::rng();
        let first = &self.services[rng.random_range(0..self.services.len())];
        let second = &self.services[rng.random_range(0..self.services.len())];
        let service = if second.outstanding() < first.outstanding() {
            second
        } else {
            first
        };
        service
            .transfer(request)
            .map_err(|_request| DmaError::Fabric("fabric worker is gone".into()))
    }

    /// Register every pool segment on every service ahead of any transfer, so any service can
    /// carry any transfer and the engine is free to balance across devices. Pins each segment once
    /// per device against `RLIMIT_MEMLOCK`.
    pub fn register_buffers(&mut self, segments: &[&'static [u8]]) -> Result<(), String> {
        for (service_index, service) in self.services.iter().enumerate() {
            for (segment_index, segment) in segments.iter().enumerate() {
                let region = service.register(*segment).map_err(|error| {
                    format!("segment {segment_index} on service {service_index}: {error}")
                })?;
                self.regions.push(region);
            }
        }
        Ok(())
    }

    /// Insert the client's address into every service's address vector at BLOB.HELLO, so its
    /// transfers post against a known peer and an unusable address fails the hello instead.
    pub fn add_peer(&self, client_id: u64, address: &[u8]) -> Result<(), String> {
        for (index, service) in self.services.iter().enumerate() {
            service
                .add_peer(client_id, address)
                .map_err(|error| format!("service {index}: {error}"))?;
        }
        Ok(())
    }

    /// One fabric address per service, in service order; what `BLOB.HELLO` returns.
    pub fn local_addresses(&self) -> impl Iterator<Item = &[u8]> {
        self.services.iter().map(FabricService::local_address)
    }

    /// Release the client's address-vector entry on every service once its transfers drain.
    pub fn remove_peer(&self, client_id: u64) {
        for service in &self.services {
            service.remove_peer(client_id);
        }
    }
}

// ─── Global Fabric ───────────────────────────────────────────────────────────

/// `None` before `commit` and after `shutdown`. `None` at commit means this instance has no
/// fabric: the module runs TCP-only and `BLOB.HELLO` reports EFA unavailable.
static FABRIC: Mutex<Option<Arc<Fabric>>> = Mutex::new(None);

fn slot() -> MutexGuard<'static, Option<Arc<Fabric>>> {
    FABRIC.lock().expect("FABRIC lock unavailable")
}

/// Store the fabric from `initialize`, once it has been set up.
pub fn commit(fabric: Option<Fabric>) {
    let mut slot = slot();
    assert!(slot.is_none(), "fabric already committed");
    *slot = fabric.map(Arc::new);
}

/// The running fabric, or `None` when this instance has none.
pub fn fabric() -> Option<Arc<Fabric>> {
    slot().clone()
}

/// Drop the services. Each closes its channel, drains its in-flight transfers, and joins its
/// worker.
pub fn shutdown() {
    let fabric = slot().take();
    drop(fabric);
}

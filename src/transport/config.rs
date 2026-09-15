//! Module args → `dma_libfabric::Configuration`. The `fabric-*` args are registered with Valkey in
//! `lib.rs` next to every other config; this owns the enum that registration needs and the mapping
//! onto the crate's own configuration.

use dma_libfabric::{Configuration, Provider};
use valkey_module::enum_configuration;

enum_configuration! {
    /// `fabric-provider`: which libfabric provider carries transfers. Immutable after load.
    /// Tcp runs anywhere and is what CI exercises; EfaDirect is the hardware path.
    #[derive(Debug, PartialEq, Eq, Copy)]
    pub enum FabricProvider {
        Tcp = 0,
        EfaDirect = 1,
    }
}

impl From<FabricProvider> for Provider {
    fn from(provider: FabricProvider) -> Self {
        match provider {
            FabricProvider::Tcp => Provider::Tcp,
            FabricProvider::EfaDirect => Provider::EfaDirect,
        }
    }
}

/// The configuration every server starts from. `interfaces` is the allowlist of domains to serve
/// on (empty: every domain discovered); `Fabric::start` pins each server to one of them.
pub fn configuration() -> Configuration {
    Configuration {
        providers: vec![crate::fabric_provider().into()],
        interfaces: crate::fabric_interfaces(),
        bind: None,
        max_in_flight: nonzero(crate::fabric_max_in_flight()),
        crc_pool_threads: Some(crate::fabric_crc_pool_threads()),
    }
}

/// 0 in a module arg means "the crate's default".
fn nonzero(value: usize) -> Option<usize> {
    (0 < value).then_some(value)
}

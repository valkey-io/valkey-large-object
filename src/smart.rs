//! NVMe SMART/Health snapshot cache for the INFO section.
//!
//! Reading and decoding the SMART log page is done by the `nvme-telem`
//! crate (a Get Log Page 0x02 admin ioctl on the controller node); this
//! module owns what the crate cannot: keeping the ioctl off the main
//! thread and serving INFO from a cached snapshot.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nvme_telem::nvme::{list_nvme_controllers, Device, NvmeSmartLog};

/// Critical Warning bit 3 (SMART log byte 0): all media placed in
/// read-only mode. Endurance exhaustion; every tiered write will fail.
pub const CW_MEDIA_READ_ONLY: u8 = 1 << 3;

pub fn media_read_only(critical_warning: u8) -> bool {
    critical_warning & CW_MEDIA_READ_ONLY != 0
}

/// Read one controller's SMART log. Blocking (admin ioctl);
pub fn read_smart_log(device: &str) -> io::Result<NvmeSmartLog> {
    Device::open(device)?.smart_log()
}

// ─── Cached snapshot for INFO (polled off-main-thread) ───
//
// INFO runs on the main event-loop thread and must never issue the ioctl.
// A background poller reads every controller once per POLL_INTERVAL and
// publishes a snapshot; INFO only ever reads the latest snapshot.

/// One device's latest reading; errors are kept and reported per device.
#[derive(Debug)]
pub struct DeviceHealth {
    pub device: String,
    pub health: io::Result<NvmeSmartLog>,
}

/// A published set of readings with its sample time.
pub struct SmartSnapshot {
    pub sampled_at: Instant,
    pub devices: Vec<DeviceHealth>,
}

impl SmartSnapshot {
    pub fn age(&self) -> Duration {
        self.sampled_at.elapsed()
    }
}

#[derive(Default)]
pub struct SmartCache {
    snapshot: Mutex<Option<Arc<SmartSnapshot>>>,
}

impl SmartCache {
    pub fn snapshot(&self) -> Option<Arc<SmartSnapshot>> {
        self.snapshot.lock().expect("smart cache poisoned").clone()
    }

    pub fn publish(&self, devices: Vec<DeviceHealth>) {
        *self.snapshot.lock().expect("smart cache poisoned") = Some(Arc::new(SmartSnapshot {
            sampled_at: Instant::now(),
            devices,
        }));
    }
}

lazy_static::lazy_static! {
    /// Module-global snapshot cache read by the INFO handler.
    pub static ref SMART_CACHE: SmartCache = SmartCache::default();
}

/// How often the poller re-reads the SMART logs.
pub const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Read every controller and publish one snapshot. This is blocking, callers run
/// it on the module runtime, never the main thread.
pub fn refresh_snapshot() {
    let mut names = list_nvme_controllers();
    names.sort();
    let devices = names
        .into_iter()
        .map(|name| {
            let device = format!("/dev/{name}");
            DeviceHealth {
                health: read_smart_log(&device),
                device,
            }
        })
        .collect();
    SMART_CACHE.publish(devices);
}

/// Spawn the background SMART poller: one read of every controller per
/// POLL_INTERVAL, forever.
pub fn start_poller() {
    crate::runtime_handle().spawn(async {
        loop {
            let _ = tokio::task::spawn_blocking(refresh_snapshot).await;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}

/// INFO-path entry point: the latest polled snapshot
pub fn snapshot_for_info() -> Option<Arc<SmartSnapshot>> {
    SMART_CACHE.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_read_only_bit() {
        assert!(media_read_only(CW_MEDIA_READ_ONLY));
        assert!(media_read_only(CW_MEDIA_READ_ONLY | 0b0111));
        assert!(!media_read_only(0));
        assert!(!media_read_only(0b0111));
    }

    #[test]
    fn snapshot_cache_starts_empty_and_publishes() {
        let cache = SmartCache::default();
        assert!(cache.snapshot().is_none());
        cache.publish(vec![DeviceHealth {
            device: "/dev/nvme0".into(),
            health: Err(io::Error::from_raw_os_error(13)),
        }]);
        let snap = cache.snapshot().expect("published");
        assert_eq!(snap.devices.len(), 1);
        assert!(snap.age() < Duration::from_secs(1));
    }
}

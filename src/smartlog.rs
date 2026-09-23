//! NVMe SMART log snapshot cache for the INFO section.
//!
//! Reading and decoding the SMART log page is done by the `nvme-telem`
//! crate (a Get Log Page 0x02 admin ioctl on the controller node); this
//! module owns what the crate cannot: keeping the ioctl off the main
//! thread and serving INFO from a cached snapshot.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nvme_telem::nvme::{list_nvme_controllers, Device, NvmeSmartLog};

/// Critical Warning bit 3 (SMART log byte 0): all media placed in
/// read-only mode. Endurance exhaustion; every tiered write will fail.
pub const CW_MEDIA_READ_ONLY: u8 = 1 << 3;

pub fn media_read_only(critical_warning: u8) -> bool {
    critical_warning & CW_MEDIA_READ_ONLY != 0
}

/// Read one controller's SMART log. Blocking (admin ioctl); never call
/// on the main event-loop thread.
pub fn read_smartlog(device: &str) -> io::Result<NvmeSmartLog> {
    Device::open(device)?.smart_log()
}

// ─── Cached snapshot for INFO (polled off-main-thread) ───
//
// INFO runs on the main event-loop thread and must never issue the ioctl.
// A background poller thread reads every controller once per interval and
// publishes a snapshot; INFO only ever reads the latest snapshot.

/// One device's latest reading; errors are kept and reported per device.
#[derive(Debug)]
pub struct DeviceHealth {
    pub device: String,
    pub health: io::Result<NvmeSmartLog>,
}

/// A published set of readings with its sample time.
pub struct SmartlogSnapshot {
    pub sampled_at: Instant,
    pub devices: Vec<DeviceHealth>,
}

impl SmartlogSnapshot {
    pub fn age(&self) -> Duration {
        self.sampled_at.elapsed()
    }
}

#[derive(Default)]
pub struct SmartlogCache {
    snapshot: Mutex<Option<Arc<SmartlogSnapshot>>>,
}

impl SmartlogCache {
    pub fn snapshot(&self) -> Option<Arc<SmartlogSnapshot>> {
        self.snapshot
            .lock()
            .expect("smartlog cache poisoned")
            .clone()
    }

    pub fn publish(&self, devices: Vec<DeviceHealth>) {
        *self.snapshot.lock().expect("smartlog cache poisoned") =
            Some(Arc::new(SmartlogSnapshot {
                sampled_at: Instant::now(),
                devices,
            }));
    }
}

lazy_static::lazy_static! {
    /// Module-global snapshot cache read by the INFO handler.
    pub static ref SMARTLOG_CACHE: SmartlogCache = SmartlogCache::default();
}

/// Set on graceful server shutdown; the poller exits its loop instead of
/// starting another device read.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Signal the poller thread to exit. Called from the shutdown handler;
/// nothing joins the thread, so this only stops new reads from starting.
pub fn signal_shutdown() {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

/// Read every controller and publish one snapshot. Blocking; runs on the
/// poller thread, never the main thread.
pub fn refresh_snapshot() {
    let mut names = list_nvme_controllers();
    names.sort();
    let devices = names
        .into_iter()
        .map(|name| {
            let device = format!("/dev/{name}");
            DeviceHealth {
                health: read_smartlog(&device),
                device,
            }
        })
        .collect();
    SMARTLOG_CACHE.publish(devices);
}

/// Spawn the background SMART log poller: a dedicated thread reading every
/// controller once per `interval` until shutdown. This mirrors the io_uring poller
pub fn start_poller(interval: Duration) {
    let spawned = std::thread::Builder::new()
        .name("lo-smartlog".into())
        .spawn(move || {
            while !SHUTDOWN.load(Ordering::Relaxed) {
                refresh_snapshot();
                std::thread::sleep(interval);
            }
        });
    if let Err(e) = spawned {
        valkey_module::logging::log_warning(format!(
            "largeobj: smartlog poller failed to start; INFO section will be absent: {e}"
        ));
    }
}

/// INFO-path entry point: the latest polled snapshot.
pub fn snapshot_for_info() -> Option<Arc<SmartlogSnapshot>> {
    SMARTLOG_CACHE.snapshot()
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
        let cache = SmartlogCache::default();
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

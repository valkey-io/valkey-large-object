use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Mutex;

use valkey_module::configuration::ConfigurationContext;
use valkey_module::enum_configuration;
use valkey_module::Context;
use valkey_module::ContextFlags;
use valkey_module::{ValkeyError, ValkeyGILGuard, ValkeyString};
use valkey_module_macros::config_changed_event_handler;

use crate::errors;
use crate::storage;
use crate::transport::config::FabricProvider;

// ─── Operating Mode ──────────────────────────────────────────────────────────

enum_configuration! {
    /// Operating mode — set via `operating-mode` module enum config.
    /// Dram (default): all objects live exclusively in DRAMPool. No NVMe.
    /// Tiered: objects persist on NVMe, DRAMPool is a read cache with promotion.
    #[derive(Debug, PartialEq, Eq, Copy)]

    pub enum OperatingMode {
        Dram = 0,
        Tiered = 1,
    }
}

// A separate module: each `enum_configuration!` expansion imports a trait by name.
mod fd_reserve_policy {
    use valkey_module::enum_configuration;

    enum_configuration! {
        /// What the module does when the fd limit does not cover the command reserve
        /// (`module start + maxclients`) — set via `fd-reserve`, Tiered only.
        /// Lenient (default): load anyway; placement and the fd cache turn off and the
        /// server keeps serving. A `max-cached-fds` that does not fit is clamped.
        /// Strict: the reserve and `max-cached-fds` must both fit. Otherwise refuse to
        /// load, reject a `CONFIG SET max-cached-fds` that does not fit, and abort if a
        /// later `CONFIG SET maxclients` breaks either.
        #[derive(Debug, PartialEq, Eq, Copy)]

        pub enum FdReserve {
            Lenient = 0,
            Strict = 1,
        }
    }
}
pub use fd_reserve_policy::FdReserve;

// ─── Config Defaults, Limits ─────────────────────────────────────────────────
//
// Named constants so the values are defined once and shared between the
// lazy_static initializers, the valkey_module! registration, and the test
// reset helpers.

// segment-size
pub const DEFAULT_SEGMENT_SIZE: i64 = 1024 * 1024 * 1024; // 1 GiB
/// Small enough for tests to fill a segment with a handful of 4 KiB objects. The config
/// constraints still require a segment to hold the largest object or promotion.
pub const MIN_SEGMENT_SIZE: i64 = 16 * 1024; // 16 KiB
pub const MAX_SEGMENT_SIZE: i64 = 1024 * 1024 * 1024; // 1 GiB

// disk-staging-size
pub const DEFAULT_DISK_STAGING_SIZE: i64 = 1024 * 1024 * 1024; // 1 GiB
pub const MIN_DISK_STAGING_SIZE: i64 = 1024 * 1024; // 1 MiB
pub const MAX_DISK_STAGING_SIZE: i64 = i64::MAX;
// disk-maxmemory
pub const DEFAULT_DISK_MAXMEMORY: i64 = 0; // unlimited
pub const MIN_DISK_MAXMEMORY: i64 = 0;
pub const MAX_DISK_MAXMEMORY: i64 = i64::MAX;
// worker-threads
pub const DEFAULT_WORKER_THREADS: i64 = 8;
pub const MIN_WORKER_THREADS: i64 = 1;
pub const MAX_WORKER_THREADS: i64 = 32;
// disk-dir-shards
pub const DEFAULT_DISK_DIR_SHARDS: i64 = 1024;
pub const MIN_DISK_DIR_SHARDS: i64 = 1;
pub const MAX_DISK_DIR_SHARDS: i64 = 65536;
// max-promote-size
pub const DEFAULT_MAX_PROMOTE_SIZE: i64 = 256 * 1024 * 1024; // 256 MiB
pub const MIN_MAX_PROMOTE_SIZE: i64 = 0;
pub const MAX_MAX_PROMOTE_SIZE: i64 = i64::MAX;
// scaling-poll-ms
pub const DEFAULT_SCALING_POLL_MS: i64 = 2500;
pub const MIN_SCALING_POLL_MS: i64 = 1_000;
pub const MAX_SCALING_POLL_MS: i64 = 60_000;
// reaper-scan-budget-us
pub const DEFAULT_REAPER_SCAN_BUDGET_US: i64 = 2000;
pub const MIN_REAPER_SCAN_BUDGET_US: i64 = 100;
pub const MAX_REAPER_SCAN_BUDGET_US: i64 = 100_000;
// reaper-poll-ms
pub const DEFAULT_REAPER_POLL_MS: i64 = 25;
pub const MIN_REAPER_POLL_MS: i64 = 10;
pub const MAX_REAPER_POLL_MS: i64 = 60_000;
// smartlog-poll-secs
pub const DEFAULT_SMARTLOG_POLL_SECS: i64 = 60;
pub const MIN_SMARTLOG_POLL_SECS: i64 = 0;
pub const MAX_SMARTLOG_POLL_SECS: i64 = 86_400;
// lfu-decay-minutes
pub const DEFAULT_LFU_DECAY_MINUTES: i64 = 1;
pub const MIN_LFU_DECAY_MINUTES: i64 = 0;
pub const MAX_LFU_DECAY_MINUTES: i64 = 65_535;
// promote-min-hits
pub const DEFAULT_PROMOTE_MIN_HITS: i64 = 2;
pub const MIN_PROMOTE_MIN_HITS: i64 = 1;
pub const MAX_PROMOTE_MIN_HITS: i64 = 255;
// victim-sample-size
pub const DEFAULT_VICTIM_SAMPLE_SIZE: i64 = 5;
pub const MIN_VICTIM_SAMPLE_SIZE: i64 = 1;
pub const MAX_VICTIM_SAMPLE_SIZE: i64 = 64;
// max-cached-fds
pub const DEFAULT_MAX_CACHED_FDS: i64 = 16_384;
pub const MIN_MAX_CACHED_FDS: i64 = 0;
pub const MAX_MAX_CACHED_FDS: i64 = 1_048_576;
// scaling-expand-watermark
pub const DEFAULT_SCALING_EXPAND_WATERMARK: i64 = 80; // stored as percent
pub const MIN_SCALING_EXPAND_WATERMARK: i64 = 50;
pub const MAX_SCALING_EXPAND_WATERMARK: i64 = 95;
// scaling-shrink-watermark
pub const DEFAULT_SCALING_SHRINK_WATERMARK: i64 = 90; // stored as percent
pub const MIN_SCALING_SHRINK_WATERMARK: i64 = 50;
pub const MAX_SCALING_SHRINK_WATERMARK: i64 = 95;
// chunk-size
pub const DEFAULT_CHUNK_SIZE: i64 = 1024 * 1024; // 1 MiB
pub const MIN_CHUNK_SIZE: i64 = 4096;
pub const MAX_CHUNK_SIZE: i64 = 256 * 1024 * 1024; // 256 MiB

// max-buffers-per-op
pub const DEFAULT_MAX_BUFFERS_PER_OP: i64 = 64;
pub const MIN_MAX_BUFFERS_PER_OP: i64 = 2;
pub const MAX_MAX_BUFFERS_PER_OP: i64 = 1024;
// min-buffers-per-op
pub const DEFAULT_MIN_BUFFERS_PER_OP: i64 = 2;
pub const MIN_MIN_BUFFERS_PER_OP: i64 = 1;
pub const MAX_MIN_BUFFERS_PER_OP: i64 = 1024;
// max-object-size
pub const DEFAULT_MAX_OBJECT_SIZE: i64 = 512 * 1024 * 1024; // 512 MiB
pub const MIN_MAX_OBJECT_SIZE: i64 = 1;
pub const MAX_MAX_OBJECT_SIZE: i64 = i64::MAX;
// max-rdma-registered-bytes
pub const DEFAULT_MAX_RDMA_REGISTERED_BYTES: i64 = 380 * 1024 * 1024 * 1024; // 380 GiB
pub const MIN_MAX_RDMA_REGISTERED_BYTES: i64 = 0; // no limit
pub const MAX_MAX_RDMA_REGISTERED_BYTES: i64 = i64::MAX;
// fabric-max-in-flight
pub const DEFAULT_FABRIC_MAX_IN_FLIGHT: i64 = 256;
pub const MIN_FABRIC_MAX_IN_FLIGHT: i64 = 0;
pub const MAX_FABRIC_MAX_IN_FLIGHT: i64 = 65_536;
// fabric-crc-pool-threads
pub const DEFAULT_FABRIC_CRC_POOL_THREADS: i64 = 2;
pub const MIN_FABRIC_CRC_POOL_THREADS: i64 = 1;
pub const MAX_FABRIC_CRC_POOL_THREADS: i64 = 1024;

pub const DEFAULT_MODULE_ENABLED: bool = true;
// ─── Test Hooks ──────────────────────────────────────────────────────────────
// test-pause-before-finalize-set-ms
pub const DEFAULT_TEST_PAUSE_BEFORE_FINALIZE_SET_MS: i64 = 0;
pub const MIN_TEST_PAUSE_BEFORE_FINALIZE_SET_MS: i64 = 0;
pub const MAX_TEST_PAUSE_BEFORE_FINALIZE_SET_MS: i64 = 60_000;
// test-pause-during-promotion-ms
pub const DEFAULT_TEST_PAUSE_DURING_PROMOTION_MS: i64 = 0;
pub const MIN_TEST_PAUSE_DURING_PROMOTION_MS: i64 = 0;
pub const MAX_TEST_PAUSE_DURING_PROMOTION_MS: i64 = 60_000;
// test-pause-after-evict-pick-ms
pub const DEFAULT_TEST_PAUSE_AFTER_EVICT_PICK_MS: i64 = 0;
pub const MIN_TEST_PAUSE_AFTER_EVICT_PICK_MS: i64 = 0;
pub const MAX_TEST_PAUSE_AFTER_EVICT_PICK_MS: i64 = 60_000;
// test-pause-after-promotion-failure-ms
pub const DEFAULT_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS: i64 = 0;
pub const MIN_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS: i64 = 0;
pub const MAX_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS: i64 = 60_000;

// ─── Runtime Config State (lazy_static) ──────────────────────────────────────

lazy_static::lazy_static! {
    /// Data directory for NVMe object files. Required. Immutable after load.
    /// String configs are registered as a `ValkeyGILGuard<ValkeyString>`, the type whose
    /// `CONFIG GET` path the crate does not leak. `on_string_config_set` copies the value
    /// into `DISK_DIR`, which is what the rest of the module reads.
    pub static ref CFG_DISK_DIR: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, ""));
    static ref DISK_DIR: Mutex<String> = Mutex::new(String::new());

    /// Total NVMe staging capacity (DRAM for I/O buffers). Default: 1GiB.
    /// Used in Tiered mode for read/write staging. Split into uniform
    /// `segment-size` segments: count = ceil(disk-staging-size / segment-size)
    /// (ceiling so actual staging is never less than requested). Immutable after load.
    pub static ref CFG_DISK_STAGING_SIZE: AtomicI64 = AtomicI64::new(DEFAULT_DISK_STAGING_SIZE);

    /// Uniform segment size for all pools (DRAMPool and NVMePool).
    /// Growth unit for DRAMPool; NVMe segment count = disk-staging-size / segment-size.
    /// Default: 1 GiB. Immutable after load.
    pub static ref CFG_SEGMENT_SIZE: AtomicI64 = AtomicI64::new(DEFAULT_SEGMENT_SIZE);

    /// Max disk usage in disk-dir. Default: 0 (unlimited).
    pub static ref CFG_DISK_MAXMEMORY: AtomicI64 = AtomicI64::new(DEFAULT_DISK_MAXMEMORY);

    /// Number of tokio worker threads for transport CQ polling. Immutable after load.
    pub static ref CFG_WORKER_THREADS: AtomicI64 = AtomicI64::new(DEFAULT_WORKER_THREADS);

    /// Subdirectories of disk-dir that object files are spread over (`ObjectId::file_path`).
    pub static ref CFG_DISK_DIR_SHARDS: AtomicI64 = AtomicI64::new(DEFAULT_DISK_DIR_SHARDS);

    /// Max object size eligible for DRAMPool promotion (Tiered mode).
    /// Objects larger than this skip promotion and are always served from NVMe.
    /// Must fit in one segment (object_fits_segment check). Default: 256 MiB.
    /// Supports memory notation (e.g., "256mb"). Refer to object_fits_segment().
    pub static ref CFG_MAX_PROMOTE_SIZE: AtomicI64 = AtomicI64::new(DEFAULT_MAX_PROMOTE_SIZE);

    /// Scaling cron poll interval in milliseconds. Controls how often the scaling
    /// timer fires to check utilization and memory pressure. Default: 2500ms.
    pub static ref CFG_SCALING_POLL_MS: AtomicI64 = AtomicI64::new(DEFAULT_SCALING_POLL_MS);

    /// Main-thread time, in microseconds, one scaling-cron tick may spend
    /// scanning for and deleting evicted keys. Default: 2000us.
    pub static ref CFG_REAPER_SCAN_BUDGET_US: AtomicI64 = AtomicI64::new(DEFAULT_REAPER_SCAN_BUDGET_US);

    /// Scaling cron poll interval, in milliseconds, while evicted keys wait to be
    /// deleted. Default: 25ms.
    pub static ref CFG_REAPER_POLL_MS: AtomicI64 = AtomicI64::new(DEFAULT_REAPER_POLL_MS);

    /// NVMe SMART poll interval in seconds (Tiered mode). 0 disables polling
    /// entirely: no background reads, and the INFO section never appears.
    /// Immutable after load — the poller either starts at init or not at all.
    pub static ref CFG_SMARTLOG_POLL_SECS: AtomicI64 = AtomicI64::new(DEFAULT_SMARTLOG_POLL_SECS);

    /// LFU score decay for ranking DRAM objects and cached fds for eviction:
    /// minutes per one-point decrement. 0 disables decay.
    pub static ref CFG_LFU_DECAY_MINUTES: AtomicI64 = AtomicI64::new(DEFAULT_LFU_DECAY_MINUTES);

    /// How many misses an object must accumulate in the admission filter before a GET
    /// promotes it into DRAMPool. Default: 2.
    pub static ref CFG_PROMOTE_MIN_HITS: AtomicI64 = AtomicI64::new(DEFAULT_PROMOTE_MIN_HITS);

    /// Entries sampled per victim search; the lowest LFU score goes.
    pub static ref CFG_VICTIM_SAMPLE_SIZE: AtomicI64 = AtomicI64::new(DEFAULT_VICTIM_SAMPLE_SIZE);

    /// Cap on read fds cached by the FdPool. When full, opening a new fd evicts
    /// the lowest LFU score among samples. The cache holds at most what fits in the
    /// module range; a larger value is clamped with a warning.
    pub static ref CFG_MAX_CACHED_FDS: AtomicI64 = AtomicI64::new(DEFAULT_MAX_CACHED_FDS);

    /// Proactive expand watermark (0.0–1.0). When DRAMPool utilization exceeds this
    /// ratio, a new segment is added ahead of time. Default: 0.80 (80%).
    pub static ref CFG_SCALING_EXPAND_WATERMARK: AtomicI64 = AtomicI64::new(DEFAULT_SCALING_EXPAND_WATERMARK);

    /// Shrink watermark (0.0–1.0). When used_memory/maxmemory exceeds this ratio,
    /// the scaling cron evicts the least-used DRAM segment. Default: 0.90 (90%).
    pub static ref CFG_SCALING_SHRINK_WATERMARK: AtomicI64 = AtomicI64::new(DEFAULT_SCALING_SHRINK_WATERMARK);

    /// Bench mode: BLOB.TCP_GET path replies with size integer instead of bulk value bytes.
    /// For benchmarking NVMe read throughput without TCP output buffer overhead.
    pub static ref CFG_BENCH_MODE: AtomicBool = AtomicBool::new(false);

    /// Direct I/O mode: when enabled, file opens use O_DIRECT to bypass the kernel page cache.
    ///
    /// WHY: Large objects (4KB-50MB) would thrash the page cache if buffered. O_DIRECT ensures
    /// NVMe reads/writes go straight to/from our pre-aligned pool buffers without kernel copies.
    ///
    /// WHEN TO DISABLE: Set to "no" when O_DIRECT writes fail with EINVAL on the target
    /// environment. Known case: ASAN builds where the sanitizer's allocator interacts
    /// differently with io_uring O_DIRECT buffer alignment validation.
    ///
    /// DEFAULT: yes (production path — always use O_DIRECT on XFS/NVMe instance store).
    pub static ref CFG_DIRECT_IO: AtomicBool = AtomicBool::new(true);

    /// When false, BLOB.* commands from clients are rejected. Existing keys are kept and
    /// replicated commands still apply. Default: true.
    pub static ref CFG_MODULE_ENABLED: AtomicBool = AtomicBool::new(DEFAULT_MODULE_ENABLED);

    /// Operating mode. Immutable after module load.
    /// - Dram (0): all objects live exclusively in DRAMPool. No NVMe. Fastest reads.
    /// - Tiered (1): objects persist on NVMe, DRAMPool is a read cache with promotion.
    pub static ref CFG_OPERATING_MODE: Mutex<OperatingMode> = Mutex::new(OperatingMode::Dram);

    /// fd-reserve policy. Immutable after module load.
    pub static ref CFG_FD_RESERVE: Mutex<FdReserve> = Mutex::new(FdReserve::Lenient);

    // ─── Fabric Configs ──────────────────────────────────────────────────

    /// libfabric provider for transfers. Emulated exercises DMA path over libfabric's tcp provider, EfaDirect needs EFA hardware.
    pub static ref CFG_FABRIC_PROVIDER: Mutex<FabricProvider> = Mutex::new(FabricProvider::EfaDirect);

    /// Comma-separated fabric domains to open a service on. Default: All domains.
    /// Registered like `CFG_DISK_DIR`; the module reads `FABRIC_INTERFACES`.
    pub static ref CFG_FABRIC_INTERFACES: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, ""));
    static ref FABRIC_INTERFACES: Mutex<String> = Mutex::new(String::new());

    /// Transfers each fabric service keeps in flight. Default: 256. 0 = the crate's provider-derived default.
    pub static ref CFG_FABRIC_MAX_IN_FLIGHT: AtomicI64 = AtomicI64::new(DEFAULT_FABRIC_MAX_IN_FLIGHT);

    /// Threads hashing checksummed transfers off the fabric workers. Default: one.
    pub static ref CFG_FABRIC_CRC_POOL_THREADS: AtomicI64 = AtomicI64::new(DEFAULT_FABRIC_CRC_POOL_THREADS);

    /// Cap on segment bytes EFA-registered (DRAM pool + staging), kept under the EFA
    /// device's registration limit. 0 = no limit. Default: 380 GiB.
    pub static ref CFG_MAX_RDMA_REGISTERED_BYTES: AtomicI64 = AtomicI64::new(DEFAULT_MAX_RDMA_REGISTERED_BYTES);

    // ─── Test Hooks ──────────────────────────────────────────────────────

    /// Test-only: pause the tiered SET path for this many milliseconds after
    /// writing data chunks but before calling commit_lo_value. 0 = disabled.
    /// Allows integration tests to inject a DEL in the mid-stream window
    /// and deterministically exercise the delete-during-SET race.
    pub static ref CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS: AtomicI64 = AtomicI64::new(0);

    /// Test-only: pause a Tiered eviction for this many milliseconds after each pick, before
    /// it claims the picked candidate. 0 = disabled. Lets an integration test take the
    /// picked candidate away first.
    pub static ref CFG_TEST_PAUSE_AFTER_EVICT_PICK_MS: AtomicI64 = AtomicI64::new(0);

    /// Test-only: pause the promotion leader for this many milliseconds after
    /// inserting the Filling ObjectContext but before starting NVMe reads.
    /// 0 = disabled. Allows integration tests to deterministically exercise
    /// coalesced-waiter registration during promotion.
    pub static ref CFG_TEST_PAUSE_DURING_PROMOTION_MS: AtomicI64 = AtomicI64::new(0);

    /// When true, the first multi-address EFA transfer injects a synthetic
    /// submit failure after the first successful post, exercising the inline
    /// drain path. Default: false. Hidden test hook.
    pub static ref CFG_TEST_RDMA_FAIL_PARTIAL: AtomicBool = AtomicBool::new(false);

    /// Test-only: fail the first NVMe source read during a DRAM promotion,
    /// triggering set_promotion_failure. Used with
    /// test-pause-after-promotion-failure-ms to deterministically exercise
    /// the late-subscriber race in serve_filling_or_became_ready.
    pub static ref CFG_TEST_FAIL_PROMOTION_READ: AtomicBool = AtomicBool::new(false);

    /// Test-only: pause for this many ms between set_promotion_failure and
    /// remove_object in the source-error path. Widens the race window so a
    /// concurrent GET sees the failed Filling entry before it is removed.
    /// 0 = disabled. Hidden test hook.
    pub static ref CFG_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS: AtomicI64 = AtomicI64::new(0);

    // ─── Streaming Configs ───────────────────────────────────────────────

    /// Chunk size for multi-buffer streaming I/O. Default: 1MiB.
    /// Determines allocation unit for all I/O operations.
    pub static ref CFG_CHUNK_SIZE: AtomicI64 = AtomicI64::new(DEFAULT_CHUNK_SIZE);

    /// Max buffers per streaming operation (batch size / pipeline depth). Default: 64.
    pub static ref CFG_MAX_BUFFERS_PER_OP: AtomicI64 = AtomicI64::new(DEFAULT_MAX_BUFFERS_PER_OP);

    /// Min buffers to start a streaming operation. Below this → reject. Default: 2.
    pub static ref CFG_MIN_BUFFERS_PER_OP: AtomicI64 = AtomicI64::new(DEFAULT_MIN_BUFFERS_PER_OP);

    /// Maximum allowed object size for BLOB.TCP_SET / BLOB.RDMA_SET. Rejects writes exceeding this limit.
    /// Default: 512 MiB. Must fit in one segment in Dram mode (object_fits_segment
    /// check). Supports memory notation (e.g., "512mb").
    pub static ref CFG_MAX_OBJECT_SIZE: AtomicI64 = AtomicI64::new(DEFAULT_MAX_OBJECT_SIZE);
}

// ─── Module Initialization Flag ──────────────────────────────────────────────

/// Set to true at the end of successful module initialization. Used by
/// validate_config_constraint to skip cross-config validation during module load
/// (config processing order is non-deterministic across type categories).
pub static MODULE_INITIALIZED: AtomicBool = AtomicBool::new(false);

// ─── Config Accessors ────────────────────────────────────────────────────────

pub fn disk_dir() -> String {
    DISK_DIR.lock().expect("DISK_DIR lock unavailable").clone()
}

/// Set handler for the string configs: copies the new value out of the Valkey-owned
/// string into the Rust-side `Mutex<String>` the accessors read.
pub fn on_string_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    // Reject invalid UTF-8: a lossy copy would make the mirror differ from what CONFIG GET reports.
    let value = val.lock(config_ctx).try_as_str()?.to_owned();
    let mirror = match name {
        "disk-dir" => &*DISK_DIR,
        "fabric-interfaces" => &*FABRIC_INTERFACES,
        _ => return Err(ValkeyError::Str("Unknown configuration parameter")),
    };
    *mirror.lock().expect("string config lock unavailable") = value;
    Ok(())
}

pub fn nvme_staging_size() -> usize {
    CFG_DISK_STAGING_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

/// The `segment-size` config, shared by the DRAM and NVMe staging pools.
pub fn segment_size() -> usize {
    CFG_SEGMENT_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn nvme_maxmemory() -> u64 {
    CFG_DISK_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn worker_threads() -> usize {
    CFG_WORKER_THREADS.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn disk_dir_shards() -> u64 {
    CFG_DISK_DIR_SHARDS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn max_promote_size() -> u64 {
    CFG_MAX_PROMOTE_SIZE.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn bench_mode() -> bool {
    CFG_BENCH_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn scaling_poll_ms() -> u64 {
    CFG_SCALING_POLL_MS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn reaper_scan_budget_us() -> u64 {
    CFG_REAPER_SCAN_BUDGET_US.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn reaper_poll_ms() -> u64 {
    CFG_REAPER_POLL_MS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn smartlog_poll_secs() -> u64 {
    CFG_SMARTLOG_POLL_SECS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn lfu_decay_minutes() -> u64 {
    CFG_LFU_DECAY_MINUTES.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn promote_min_hits() -> u8 {
    CFG_PROMOTE_MIN_HITS.load(std::sync::atomic::Ordering::Relaxed) as u8
}

pub fn victim_sample_size() -> usize {
    CFG_VICTIM_SAMPLE_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_cached_fds() -> usize {
    CFG_MAX_CACHED_FDS.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn scaling_expand_watermark() -> f64 {
    CFG_SCALING_EXPAND_WATERMARK.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0
}

pub fn scaling_shrink_watermark() -> f64 {
    CFG_SCALING_SHRINK_WATERMARK.load(std::sync::atomic::Ordering::Relaxed) as f64 / 100.0
}

/// Read the server-wide memory figures from Valkey via `INFO memory`.
/// Returns `(used_memory, maxmemory)` in bytes. `maxmemory == 0` means the
/// server has no configured limit (unbounded). This is the SERVER-scoped
/// signal (all data types + overhead), not the module's own pool usage —
/// used by both the scaling cron's shrink check and the expand OOM guard so
/// the two decisions share one source of truth. Must be called on the main
/// event-loop thread (module API).
pub fn server_memory(ctx: &Context) -> (u64, u64) {
    let info = ctx.server_info("memory");
    let used = info.field_unsigned("used_memory").unwrap_or(0);
    let maxmemory = info.field_unsigned("maxmemory").unwrap_or(0);
    (used, maxmemory)
}

/// Whether the server may evict keys: `maxmemory` is set and `maxmemory-policy`
/// is not `noeviction`. Not set on a replica that ignores maxmemory (the default),
/// which leaves eviction to its primary. Dram SET eviction deletes keys, so it runs
/// only when this holds.
pub fn eviction_allowed(ctx: &Context) -> bool {
    ctx.get_flags().contains(ContextFlags::EVICTED)
}

/// Whether Tiered eviction may evict object files: `disk-maxmemory` is set,
/// `maxmemory-policy` is not `noeviction`, and this is not a replica, which leaves
/// eviction to its primary. The server's `maxmemory` is not involved: it bounds DRAM,
/// not the disk.
pub fn tiered_eviction_allowed(ctx: &Context) -> bool {
    nvme_maxmemory() != 0
        && !storage::eviction_candidates::noeviction()
        && !ctx.get_flags().contains(ContextFlags::SLAVE)
}

/// Whether allocating `extra_bytes` more would push server memory to/over the
/// shrink watermark fraction of `maxmemory`. Returns `false` when `maxmemory`
/// is 0 (no server limit configured — the caller falls back to allocation
/// success as the only bound). Used to gate expand so we never grow into
/// memory the shrink path would immediately try to reclaim.
pub fn would_cross_memory_watermark(ctx: &Context, extra_bytes: u64) -> bool {
    let (used, maxmemory) = server_memory(ctx);
    if maxmemory == 0 {
        return false;
    }
    let ceiling = (maxmemory as f64 * scaling_shrink_watermark()) as u64;
    used.saturating_add(extra_bytes) >= ceiling
}

pub fn direct_io() -> bool {
    CFG_DIRECT_IO.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn module_enabled() -> bool {
    CFG_MODULE_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn operating_mode() -> OperatingMode {
    *CFG_OPERATING_MODE
        .lock()
        .expect("CFG_OPERATING_MODE lock unavailable")
}

pub fn fd_reserve() -> FdReserve {
    *CFG_FD_RESERVE
        .lock()
        .expect("CFG_FD_RESERVE lock unavailable")
}

pub fn chunk_size() -> usize {
    CFG_CHUNK_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_buffers_per_op() -> usize {
    CFG_MAX_BUFFERS_PER_OP.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn min_buffers_per_op() -> usize {
    CFG_MIN_BUFFERS_PER_OP.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn test_pause_before_finalize_set_ms() -> u64 {
    CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn test_pause_after_evict_pick_ms() -> u64 {
    CFG_TEST_PAUSE_AFTER_EVICT_PICK_MS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn test_pause_during_promotion_ms() -> u64 {
    CFG_TEST_PAUSE_DURING_PROMOTION_MS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn test_rdma_fail_partial() -> bool {
    CFG_TEST_RDMA_FAIL_PARTIAL.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn test_fail_promotion_read() -> bool {
    CFG_TEST_FAIL_PROMOTION_READ.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn test_pause_after_promotion_failure_ms() -> u64 {
    CFG_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn fabric_provider() -> FabricProvider {
    *CFG_FABRIC_PROVIDER
        .lock()
        .expect("CFG_FABRIC_PROVIDER lock unavailable")
}

pub fn fabric_interfaces() -> Vec<String> {
    FABRIC_INTERFACES
        .lock()
        .expect("FABRIC_INTERFACES lock unavailable")
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn fabric_max_in_flight() -> usize {
    CFG_FABRIC_MAX_IN_FLIGHT.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn fabric_crc_pool_threads() -> usize {
    CFG_FABRIC_CRC_POOL_THREADS.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_rdma_registered_bytes() -> u64 {
    CFG_MAX_RDMA_REGISTERED_BYTES.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn max_object_size() -> u64 {
    CFG_MAX_OBJECT_SIZE.load(std::sync::atomic::Ordering::Relaxed) as u64
}

// ─── Config Constraints ──────────────────────────────────────────────────────
//
// List of constraints between configs. Each constraint enforces
// parent >= child when its condition is true, unless it supplies a custom
// validator predicate.
//
// At module load (before MODULE_INITIALIZED is set), per-config callbacks skip
// validation (config processing order is non-deterministic across type categories).
// validate_all_constraints() runs in initialize() after all configs are finalized.
//
// At runtime (CONFIG SET), the framework stores the new value into the atomic
// before calling the validation callback. validate_config_constraint() then runs
// validate_all_constraints() over the same atomics — no value substitution needed.

/// A constraint between two configs, enforced when enforce_condition()
/// is true. By default the constraint is `parent >= child`; a constraint may
/// override it with a `validator` predicate taking (parent_value, child_value).
struct ConfigConstraint {
    parent: &'static AtomicI64,
    child: &'static AtomicI64,
    /// Whether to enforce this constraint. Some constraints only apply in
    /// certain operating modes (e.g. Tiered-only); returns false to skip.
    enforce_condition: fn() -> bool,
    /// Returns true when the constraint holds. `None` means the default
    /// `parent >= child`. A validator may also read other configs.
    validator: Option<fn(u64, u64) -> bool>,
    error_msg: &'static str,
}

fn config_constraints() -> &'static [ConfigConstraint] {
    use std::sync::LazyLock;
    static CONSTRAINTS: LazyLock<Vec<ConfigConstraint>> = LazyLock::new(|| {
        vec![
            ConfigConstraint {
                parent: &CFG_DISK_MAXMEMORY,
                child: &CFG_MAX_OBJECT_SIZE,
                enforce_condition: || operating_mode() == OperatingMode::Tiered,
                // 0 = unlimited; skip the check.
                validator: Some(|p, c| p == 0 || p >= c),
                error_msg: errors::ERR_NVME_GE_MAX_OBJ,
            },
            ConfigConstraint {
                parent: &CFG_DISK_STAGING_SIZE,
                child: &CFG_SEGMENT_SIZE,
                enforce_condition: || operating_mode() == OperatingMode::Tiered,
                validator: None,
                error_msg: errors::ERR_STAGING_GE_SEGMENT,
            },
            ConfigConstraint {
                parent: &CFG_SEGMENT_SIZE,
                child: &CFG_CHUNK_SIZE,
                enforce_condition: || true,
                validator: None,
                error_msg: errors::ERR_SEGMENT_GE_CHUNK,
            },
            ConfigConstraint {
                parent: &CFG_SEGMENT_SIZE,
                child: &CFG_MAX_OBJECT_SIZE,
                enforce_condition: || operating_mode() == OperatingMode::Dram,
                validator: Some(object_fits_segment),
                error_msg: errors::ERR_SEGMENT_GE_MAX_OBJ,
            },
            ConfigConstraint {
                parent: &CFG_SEGMENT_SIZE,
                child: &CFG_MAX_PROMOTE_SIZE,
                enforce_condition: || operating_mode() == OperatingMode::Tiered,
                validator: Some(object_fits_segment),
                error_msg: errors::ERR_SEGMENT_GE_PROMOTE,
            },
            ConfigConstraint {
                parent: &CFG_MAX_BUFFERS_PER_OP,
                child: &CFG_MIN_BUFFERS_PER_OP,
                enforce_condition: || true,
                validator: None,
                error_msg: errors::ERR_MAX_BUF_GE_MIN_BUF,
            },
        ]
    });
    &CONSTRAINTS
}

/// Constraint validator: an object of `object_size` must fit in one empty segment of
/// `segment_size` after talc's per-chunk metadata, since `alloc_exact`
/// co-locates all of an object's chunks in a single segment.
fn object_fits_segment(segment_size: u64, object_size: u64) -> bool {
    let chunk_size = CFG_CHUNK_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize;
    storage::object_fits_segment(segment_size as usize, object_size as usize, chunk_size)
}

/// Validate all config constraints. Returns the first violated constraint or Ok(()).
/// Called in initialize() after all configs are finalized, and by
/// validate_config_constraint() at runtime (CONFIG SET) after the framework has
/// already stored the new value into the atomic.
pub fn validate_all_constraints() -> Result<(), String> {
    // Chunk i is written as align_up(len) bytes at i * chunk_size, so an
    // unaligned chunk_size makes each write overlap the next chunk.
    if !(CFG_CHUNK_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize)
        .is_multiple_of(storage::IO_ALIGN)
    {
        return Err(errors::ERR_CHUNK_NOT_ALIGNED.into());
    }
    for constraint in config_constraints() {
        if !(constraint.enforce_condition)() {
            continue;
        }
        let p = constraint.parent.load(std::sync::atomic::Ordering::Relaxed) as u64;
        let c = constraint.child.load(std::sync::atomic::Ordering::Relaxed) as u64;
        let holds = match constraint.validator {
            Some(validator) => validator(p, c),
            None => p >= c,
        };
        if !holds {
            return Err(constraint.error_msg.into());
        }
    }
    if operating_mode() == OperatingMode::Tiered && !staging_holds_min_window() {
        return Err(errors::ERR_STAGING_HOLDS_WINDOW.into());
    }
    Ok(())
}

/// Tiered: whether the staging pool can hold the buffers one write needs. Every Tiered SET
/// and streamed GET stages its chunks in `chunk-size` buffers (`alloc_window`), so a
/// staging pool that cannot hold them fails every such command with "staging exhausted",
/// however idle the server is.
/// - One buffer of the largest chunk an object can have must fit a segment.
/// - An object of several chunks needs `min-buffers-per-op` of them (fewer if it has
///   fewer chunks), which may sit in different segments.
///
/// talc charges one `IO_ALIGN` block per allocation and one per segment, as in
/// `storage::object_fits_segment`.
fn staging_holds_min_window() -> bool {
    let load = |cfg: &AtomicI64| cfg.load(std::sync::atomic::Ordering::Relaxed) as usize;
    let segment = load(&CFG_SEGMENT_SIZE);
    let chunk = load(&CFG_CHUNK_SIZE);
    let max_object = load(&CFG_MAX_OBJECT_SIZE);
    if chunk == 0 || !storage::object_fits_segment(segment, chunk.min(max_object), chunk) {
        return false;
    }
    let chunks = max_object.div_ceil(chunk);
    if chunks <= 1 {
        return true;
    }
    // The last chunk's buffer is optional (`alloc_window`), so a window is at most chunks - 1.
    let window = load(&CFG_MIN_BUFFERS_PER_OP).min(chunks - 1).max(1);
    let per_segment = segment.saturating_sub(storage::IO_ALIGN) / (chunk + storage::IO_ALIGN);
    let segments = load(&CFG_DISK_STAGING_SIZE).div_ceil(segment).max(1);
    per_segment.saturating_mul(segments) >= window
}

// ─── Load Arguments ──────────────────────────────────────────────────────────
//
// `module_args_as_configuration` hands the `loadmodule` args to the valkey-module crate,
// which looks each registered config up by name and uses the value it finds as that
// config's default. It never looks at a name it does not know, and core never checks a
// default against the config's range. `validate_load_args` closes both gaps.

/// Every numeric config with its range, as registered in `valkey_module!` (lib.rs).
/// Keep the two in step: a config missing here is refused as an unknown load argument.
fn numeric_configs() -> [(&'static str, &'static AtomicI64, i64, i64); 27] {
    [
        (
            "segment-size",
            &CFG_SEGMENT_SIZE,
            MIN_SEGMENT_SIZE,
            MAX_SEGMENT_SIZE,
        ),
        (
            "disk-staging-size",
            &CFG_DISK_STAGING_SIZE,
            MIN_DISK_STAGING_SIZE,
            MAX_DISK_STAGING_SIZE,
        ),
        (
            "disk-maxmemory",
            &CFG_DISK_MAXMEMORY,
            MIN_DISK_MAXMEMORY,
            MAX_DISK_MAXMEMORY,
        ),
        (
            "worker-threads",
            &CFG_WORKER_THREADS,
            MIN_WORKER_THREADS,
            MAX_WORKER_THREADS,
        ),
        (
            "disk-dir-shards",
            &CFG_DISK_DIR_SHARDS,
            MIN_DISK_DIR_SHARDS,
            MAX_DISK_DIR_SHARDS,
        ),
        (
            "max-promote-size",
            &CFG_MAX_PROMOTE_SIZE,
            MIN_MAX_PROMOTE_SIZE,
            MAX_MAX_PROMOTE_SIZE,
        ),
        (
            "chunk-size",
            &CFG_CHUNK_SIZE,
            MIN_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
        ),
        (
            "max-buffers-per-op",
            &CFG_MAX_BUFFERS_PER_OP,
            MIN_MAX_BUFFERS_PER_OP,
            MAX_MAX_BUFFERS_PER_OP,
        ),
        (
            "min-buffers-per-op",
            &CFG_MIN_BUFFERS_PER_OP,
            MIN_MIN_BUFFERS_PER_OP,
            MAX_MIN_BUFFERS_PER_OP,
        ),
        (
            "test-pause-before-finalize-set-ms",
            &CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS,
            MIN_TEST_PAUSE_BEFORE_FINALIZE_SET_MS,
            MAX_TEST_PAUSE_BEFORE_FINALIZE_SET_MS,
        ),
        (
            "test-pause-after-evict-pick-ms",
            &CFG_TEST_PAUSE_AFTER_EVICT_PICK_MS,
            MIN_TEST_PAUSE_AFTER_EVICT_PICK_MS,
            MAX_TEST_PAUSE_AFTER_EVICT_PICK_MS,
        ),
        (
            "test-pause-during-promotion-ms",
            &CFG_TEST_PAUSE_DURING_PROMOTION_MS,
            MIN_TEST_PAUSE_DURING_PROMOTION_MS,
            MAX_TEST_PAUSE_DURING_PROMOTION_MS,
        ),
        (
            "test-pause-after-promotion-failure-ms",
            &CFG_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS,
            MIN_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS,
            MAX_TEST_PAUSE_AFTER_PROMOTION_FAILURE_MS,
        ),
        (
            "scaling-poll-ms",
            &CFG_SCALING_POLL_MS,
            MIN_SCALING_POLL_MS,
            MAX_SCALING_POLL_MS,
        ),
        (
            "reaper-scan-budget-us",
            &CFG_REAPER_SCAN_BUDGET_US,
            MIN_REAPER_SCAN_BUDGET_US,
            MAX_REAPER_SCAN_BUDGET_US,
        ),
        (
            "reaper-poll-ms",
            &CFG_REAPER_POLL_MS,
            MIN_REAPER_POLL_MS,
            MAX_REAPER_POLL_MS,
        ),
        (
            "smartlog-poll-secs",
            &CFG_SMARTLOG_POLL_SECS,
            MIN_SMARTLOG_POLL_SECS,
            MAX_SMARTLOG_POLL_SECS,
        ),
        (
            "scaling-expand-watermark",
            &CFG_SCALING_EXPAND_WATERMARK,
            MIN_SCALING_EXPAND_WATERMARK,
            MAX_SCALING_EXPAND_WATERMARK,
        ),
        (
            "max-object-size",
            &CFG_MAX_OBJECT_SIZE,
            MIN_MAX_OBJECT_SIZE,
            MAX_MAX_OBJECT_SIZE,
        ),
        (
            "scaling-shrink-watermark",
            &CFG_SCALING_SHRINK_WATERMARK,
            MIN_SCALING_SHRINK_WATERMARK,
            MAX_SCALING_SHRINK_WATERMARK,
        ),
        (
            "lfu-decay-minutes",
            &CFG_LFU_DECAY_MINUTES,
            MIN_LFU_DECAY_MINUTES,
            MAX_LFU_DECAY_MINUTES,
        ),
        (
            "promote-min-hits",
            &CFG_PROMOTE_MIN_HITS,
            MIN_PROMOTE_MIN_HITS,
            MAX_PROMOTE_MIN_HITS,
        ),
        (
            "victim-sample-size",
            &CFG_VICTIM_SAMPLE_SIZE,
            MIN_VICTIM_SAMPLE_SIZE,
            MAX_VICTIM_SAMPLE_SIZE,
        ),
        (
            "max-cached-fds",
            &CFG_MAX_CACHED_FDS,
            MIN_MAX_CACHED_FDS,
            MAX_MAX_CACHED_FDS,
        ),
        (
            "fabric-max-in-flight",
            &CFG_FABRIC_MAX_IN_FLIGHT,
            MIN_FABRIC_MAX_IN_FLIGHT,
            MAX_FABRIC_MAX_IN_FLIGHT,
        ),
        (
            "fabric-crc-pool-threads",
            &CFG_FABRIC_CRC_POOL_THREADS,
            MIN_FABRIC_CRC_POOL_THREADS,
            MAX_FABRIC_CRC_POOL_THREADS,
        ),
        (
            "max-rdma-registered-bytes",
            &CFG_MAX_RDMA_REGISTERED_BYTES,
            MIN_MAX_RDMA_REGISTERED_BYTES,
            MAX_MAX_RDMA_REGISTERED_BYTES,
        ),
    ]
}

/// The string, bool and enum configs registered in `valkey_module!` (lib.rs).
const OTHER_CONFIGS: [&str; 10] = [
    "disk-dir",
    "fabric-interfaces",
    "bench-mode",
    "test-rdma-fail-partial",
    "test-fail-promotion-read",
    "direct-io",
    "enabled",
    "operating-mode",
    "fabric-provider",
    "fd-reserve",
];

/// Check the `loadmodule` arguments (`name value` pairs) and the numeric values the
/// crate has already stored from them.
/// - A name that is not a config is refused: a misspelled limit would otherwise load as
///   if it had not been given.
/// - A numeric config outside its range is refused, as `CONFIG SET` and the config file
///   already do.
pub fn validate_load_args(args: &[&[u8]]) -> Result<(), String> {
    if !args.len().is_multiple_of(2) {
        return Err("module arguments must be name value pairs".into());
    }
    let numeric = numeric_configs();
    for name in args.iter().step_by(2) {
        let known = numeric.iter().any(|(n, ..)| n.as_bytes() == *name)
            || OTHER_CONFIGS.iter().any(|n| n.as_bytes() == *name);
        if !known {
            return Err(format!(
                "unknown module argument '{}'",
                String::from_utf8_lossy(name)
            ));
        }
    }
    for (name, value, min, max) in numeric {
        let value = value.load(std::sync::atomic::Ordering::Relaxed);
        if !(min..=max).contains(&value) {
            return Err(format!(
                "largeobj.{name} {value} must be between {min} and {max} inclusive"
            ));
        }
    }
    Ok(())
}

/// Shared validation callback for mutable configs participating in the config
/// constraints. At initial load, defers to validate_all_constraints() in initialize().
/// At runtime (CONFIG SET), the framework has already stored the new value, so
/// we just re-check all constraints against current atomics.
pub fn validate_config_constraint(
    _ctx: &valkey_module::configuration::ConfigurationContext,
    _name: &str,
    _val: &'static AtomicI64,
) -> Result<(), valkey_module::ValkeyError> {
    if !MODULE_INITIALIZED.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    validate_all_constraints().map_err(valkey_module::ValkeyError::String)
}

/// The one listener for changes to Valkey's own configs (`CONFIG SET`), fired after the
/// change is applied. Each subsystem owns its handling; this only routes, so the order
/// of reactions to a change is visible here. The module's own configs do not come
/// through here: they use their per-config `on_set` / `on_changed` callbacks.
#[config_changed_event_handler]
fn on_server_config_changed(ctx: &Context, changed: &[&str]) {
    if changed.contains(&"maxclients") {
        storage::fd_placement::on_maxclients_changed(ctx);
    }
    if changed.contains(&"maxmemory-policy") {
        storage::eviction_candidates::refresh_policy(ctx);
    }
}

/// `max-cached-fds` changed: the cache is trimmed to the new limit right away
/// (nothing when placement is off), and a value that does not fit is logged. No pool
/// yet at load.
pub fn on_max_cached_fds_changed(
    _ctx: &valkey_module::configuration::ConfigurationContext,
    _name: &str,
    val: &'static AtomicI64,
) {
    if let Some(pool) = storage::FD_POOL.get() {
        pool.shrink_to(storage::fd_placement::fd_cache_max_entries());
        storage::fd_placement::warn_if_cache_limited(
            val.load(std::sync::atomic::Ordering::Relaxed) as u64,
        );
    }
}

/// `max-cached-fds` CONFIG SET under `fd-reserve Strict`: the new value must fit in the
/// fd limit, or the command fails naming the limit that would fit. Lenient accepts any
/// value and clamps. The framework has already stored the new value; on Err core
/// restores the old one. No pool yet at load, where `raise_module_start` runs the check.
pub fn validate_max_cached_fds(
    _ctx: &valkey_module::configuration::ConfigurationContext,
    _name: &str,
    val: &'static AtomicI64,
) -> Result<(), valkey_module::ValkeyError> {
    if storage::FD_POOL.get().is_none() {
        return Ok(());
    }
    storage::fd_placement::check_cache_fits_if_strict(
        val.load(std::sync::atomic::Ordering::Relaxed) as u64,
    )
    .map_err(valkey_module::ValkeyError::String)
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    // ─── Test Infrastructure ─────────────────────────────────────────────

    /// Reset all constrained configs to their compile-time defaults. Other tests
    /// (e.g. segment_pool) may mutate shared statics without restoring them.
    fn reset_constrained_defaults() {
        CFG_DISK_MAXMEMORY.store(DEFAULT_DISK_MAXMEMORY, Relaxed);
        CFG_DISK_STAGING_SIZE.store(DEFAULT_DISK_STAGING_SIZE, Relaxed);
        CFG_SEGMENT_SIZE.store(DEFAULT_SEGMENT_SIZE, Relaxed);
        CFG_CHUNK_SIZE.store(DEFAULT_CHUNK_SIZE, Relaxed);
        CFG_MAX_OBJECT_SIZE.store(DEFAULT_MAX_OBJECT_SIZE, Relaxed);
        CFG_MAX_PROMOTE_SIZE.store(DEFAULT_MAX_PROMOTE_SIZE, Relaxed);
        CFG_MAX_BUFFERS_PER_OP.store(DEFAULT_MAX_BUFFERS_PER_OP, Relaxed);
        CFG_MIN_BUFFERS_PER_OP.store(DEFAULT_MIN_BUFFERS_PER_OP, Relaxed);
    }

    fn set_cfgs(cfgs: &[(&AtomicI64, i64)]) {
        for &(cfg, val) in cfgs {
            cfg.store(val, Relaxed);
        }
    }
    fn set_mode(mode: OperatingMode) {
        *CFG_OPERATING_MODE.lock().unwrap_or_else(|e| e.into_inner()) = mode;
    }

    /// All configs that participate in config constraints, paired with
    /// their current value. Used to snapshot defaults and restore between cases.
    fn all_constrained_configs() -> Vec<(&'static AtomicI64, i64)> {
        vec![
            (&CFG_DISK_MAXMEMORY, CFG_DISK_MAXMEMORY.load(Relaxed)),
            (&CFG_DISK_STAGING_SIZE, CFG_DISK_STAGING_SIZE.load(Relaxed)),
            (&CFG_SEGMENT_SIZE, CFG_SEGMENT_SIZE.load(Relaxed)),
            (&CFG_CHUNK_SIZE, CFG_CHUNK_SIZE.load(Relaxed)),
            (&CFG_MAX_OBJECT_SIZE, CFG_MAX_OBJECT_SIZE.load(Relaxed)),
            (&CFG_MAX_PROMOTE_SIZE, CFG_MAX_PROMOTE_SIZE.load(Relaxed)),
            (
                &CFG_MAX_BUFFERS_PER_OP,
                CFG_MAX_BUFFERS_PER_OP.load(Relaxed),
            ),
            (
                &CFG_MIN_BUFFERS_PER_OP,
                CFG_MIN_BUFFERS_PER_OP.load(Relaxed),
            ),
        ]
    }

    // ─── validate_all_constraints: parametrized test data ─────────────
    //
    // Each entry: (label, mode, config overrides, expected error substring or None).
    // The runner restores defaults before each case, then applies mode + overrides,
    // then asserts Ok or Err containing the substring.
    //
    // Default values are read from the atomics once at test entry (before any
    // mutation) and passed in so override expressions like `segment - 1` use
    // the real defaults without duplicating their numeric values.

    type ConstraintTestCase = (
        &'static str,
        OperatingMode,
        Vec<(&'static AtomicI64, i64)>,
        Option<&'static str>,
    );

    fn constraint_test_cases(
        segment: i64,
        chunk: i64,
        max_obj: i64,
        max_buf: i64,
        min_buf: i64,
    ) -> Vec<ConstraintTestCase> {
        vec![
            // ── Happy paths ──────────────────────────────────────────────
            ("defaults_pass", OperatingMode::Dram, vec![], None),
            (
                "equal_buffers_ok",
                OperatingMode::Dram,
                vec![(&CFG_MIN_BUFFERS_PER_OP, max_buf)],
                None,
            ),
            (
                "tiered_max_obj_within_nvme_ok",
                OperatingMode::Tiered,
                vec![],
                None,
            ),
            (
                "mode_conditional_constraint_skipped_when_inactive",
                OperatingMode::Dram,
                vec![(&CFG_DISK_MAXMEMORY, 1)],
                None,
            ),
            (
                "tiered_max_obj_above_segment_ok",
                OperatingMode::Tiered,
                vec![(&CFG_MAX_OBJECT_SIZE, segment + 1)],
                None,
            ),
            // ── Violation per constraint ──────────────────────────────────
            (
                "segment_lt_chunk_rejected",
                OperatingMode::Dram,
                vec![
                    (&CFG_SEGMENT_SIZE, chunk - 1),
                    (&CFG_DISK_STAGING_SIZE, chunk - 1),
                    (&CFG_MAX_OBJECT_SIZE, chunk - 1),
                ],
                Some("largeobj.segment-size must be >= largeobj.chunk-size"),
            ),
            (
                "segment_lt_max_obj_rejected",
                OperatingMode::Dram,
                vec![
                    (&CFG_SEGMENT_SIZE, max_obj - 1),
                    (&CFG_DISK_STAGING_SIZE, max_obj - 1),
                ],
                Some("largeobj.segment-size is insufficient for largeobj.max-object-size"),
            ),
            (
                "segment_lt_promote_rejected",
                OperatingMode::Tiered,
                vec![(&CFG_MAX_PROMOTE_SIZE, segment + 1)],
                Some("largeobj.segment-size is insufficient for largeobj.max-promote-size"),
            ),
            (
                "staging_lt_segment_rejected",
                OperatingMode::Tiered,
                vec![(&CFG_DISK_STAGING_SIZE, segment - 1)],
                Some("largeobj.disk-staging-size must be >= largeobj.segment-size"),
            ),
            (
                "buffers_max_lt_min_rejected",
                OperatingMode::Dram,
                vec![(&CFG_MAX_BUFFERS_PER_OP, min_buf - 1)],
                Some("largeobj.max-buffers-per-op must be >= largeobj.min-buffers-per-op"),
            ),
            (
                "tiered_mode_nvme_lt_max_obj_rejected",
                OperatingMode::Tiered,
                vec![(&CFG_DISK_MAXMEMORY, max_obj - 1)],
                Some("largeobj.disk-maxmemory must be >= largeobj.max-object-size in Tiered mode"),
            ),
            (
                "tiered_segment_cannot_hold_one_chunk_buffer_rejected",
                OperatingMode::Tiered,
                vec![
                    (&CFG_SEGMENT_SIZE, chunk),
                    (&CFG_DISK_STAGING_SIZE, 8 * chunk),
                    (&CFG_MAX_PROMOTE_SIZE, 0),
                ],
                Some("cannot hold the largeobj.chunk-size buffers a write needs"),
            ),
            (
                // Objects are all smaller than a chunk, so no chunk-size buffer is needed.
                "tiered_segment_equal_to_chunk_ok_for_smaller_objects",
                OperatingMode::Tiered,
                vec![
                    (&CFG_SEGMENT_SIZE, chunk),
                    (&CFG_DISK_STAGING_SIZE, chunk),
                    (&CFG_MAX_PROMOTE_SIZE, 0),
                    (&CFG_MAX_OBJECT_SIZE, chunk / 2),
                ],
                None,
            ),
            (
                // One buffer fits each segment, but a window is two.
                "tiered_staging_cannot_hold_min_window_rejected",
                OperatingMode::Tiered,
                vec![
                    (&CFG_SEGMENT_SIZE, chunk + 8192),
                    (&CFG_DISK_STAGING_SIZE, chunk + 8192),
                    (&CFG_MAX_PROMOTE_SIZE, 0),
                ],
                Some("cannot hold the largeobj.chunk-size buffers a write needs"),
            ),
            (
                "tiered_min_window_spread_over_two_segments_ok",
                OperatingMode::Tiered,
                vec![
                    (&CFG_SEGMENT_SIZE, chunk + 8192),
                    (&CFG_DISK_STAGING_SIZE, 2 * (chunk + 8192)),
                    (&CFG_MAX_PROMOTE_SIZE, 0),
                ],
                None,
            ),
            (
                "dram_segment_equal_to_chunk_unaffected",
                OperatingMode::Dram,
                vec![
                    (&CFG_SEGMENT_SIZE, chunk),
                    (&CFG_MAX_OBJECT_SIZE, chunk / 2),
                ],
                None,
            ),
            (
                "chunk_not_4k_multiple_rejected",
                OperatingMode::Dram,
                vec![(&CFG_CHUNK_SIZE, 5000)],
                Some("largeobj.chunk-size must be a multiple of 4096"),
            ),
        ]
    }

    // Shares the config statics with `test_validate_all_constraints`, so both run under
    // this lock.
    static CONFIG_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_validate_load_args() {
        let _g = CONFIG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        reset_constrained_defaults();
        let check = |args: &[&str]| {
            let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
            validate_load_args(&args)
        };
        assert_eq!(check(&[]), Ok(()));
        assert_eq!(
            check(&[
                "operating-mode",
                "Tiered",
                "segment-size",
                "1048576",
                "enabled",
                "no"
            ]),
            Ok(())
        );
        // A value is never taken for a name, even when it spells one.
        assert_eq!(check(&["disk-dir", "segment-size"]), Ok(()));
        assert_eq!(
            check(&["operating-mode", "Dram", "dram-maxmemory", "1073741824"]),
            Err("unknown module argument 'dram-maxmemory'".into())
        );
        assert_eq!(
            check(&["segment-size"]),
            Err("module arguments must be name value pairs".into())
        );
        // The crate has stored the argument's value by the time this runs.
        for (cfg, value, message) in [
            (
                &*CFG_WORKER_THREADS,
                0,
                "largeobj.worker-threads 0 must be between 1 and 32 inclusive",
            ),
            (
                &*CFG_SEGMENT_SIZE,
                MAX_SEGMENT_SIZE + 1,
                "largeobj.segment-size 1073741825 must be between 16384 and 1073741824 inclusive",
            ),
        ] {
            let default = cfg.swap(value, Relaxed);
            assert_eq!(check(&[]), Err(message.into()));
            cfg.store(default, Relaxed);
        }
        assert_eq!(check(&[]), Ok(()));
    }

    #[test]
    fn test_validate_all_constraints() {
        let _g = CONFIG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // Restore compile-time defaults in case prior tests mutated statics.
        reset_constrained_defaults();
        // Capture defaults before any mutation.
        let defaults = all_constrained_configs();
        let segment = CFG_SEGMENT_SIZE.load(Relaxed);
        let chunk = CFG_CHUNK_SIZE.load(Relaxed);
        let max_obj = CFG_MAX_OBJECT_SIZE.load(Relaxed);
        let max_buf = CFG_MAX_BUFFERS_PER_OP.load(Relaxed);
        let min_buf = CFG_MIN_BUFFERS_PER_OP.load(Relaxed);
        for (label, mode, overrides, expected_err) in
            constraint_test_cases(segment, chunk, max_obj, max_buf, min_buf)
        {
            set_cfgs(&defaults);
            set_mode(OperatingMode::Dram);
            set_mode(mode);
            set_cfgs(&overrides);
            let result = validate_all_constraints();
            match expected_err {
                None => assert!(result.is_ok(), "{label}: expected Ok, got {result:?}"),
                Some(substr) => {
                    let err = result.expect_err(&format!("{label}: expected Err"));
                    assert!(err.contains(substr), "{label}: '{err}' missing '{substr}'");
                }
            }
        }
    }
}

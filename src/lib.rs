//! ValkeyLargeObj: Large Object Module + Transport Crate
//!
//! Architecture (from interface doc):
//!   Data Type (commands, LoValue, keyspace)
//!       ↓ calls
//!   Storage (buffer pool, io_uring, NVMe files)
//!       ↓ passes buffers to
//!   Transport (EFA, fi_write/fi_read)
//!
//! Commands: LO.HELLO, LO.GET, LO.SET
//! Deletion: native Valkey DEL triggers module free callback.

// ─── Initialization Order ────────────────────────────────────────────────────
//
// Module init proceeds in strict order. Commands are safe to call ONLY after
// all steps complete:
//
//   1. Fabric::start()         — one libfabric service per domain. On missing
//                                fabric, the EFA path is unavailable and LO.HELLO
//                                gives an error.
//   2. storage::init(mode, nvme_dir)
//                              — validate config, allocate pool segments, create
//                                io_uring engine and start the smartlog poller
//                                (Tiered only). All resources are created as
//                                locals; OnceLock statics are set only after
//                                everything succeeds. On failure, locals
//                                drop naturally — module load retryable.
//   3. transport::register_buffers()
//                              — fi_mr_reg pool buffers with EFA domains.
//   4. RUNTIME.set(rt)         — commit tokio runtime last (only used by commands).
//
// After step 4, commands (LO.GET, LO.SET, LO.HELLO) may execute safely.
// ─────────────────────────────────────────────────────────────────────────────

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Mutex;

use dma_libfabric_protocol::encode_hex;
use valkey_module::configuration::ConfigurationFlags;
use valkey_module::{valkey_module, Context, InfoContext, Status, ValkeyResult, ValkeyString};
use valkey_module_macros::shutdown_event_handler;

use tokio::runtime::Runtime;

pub mod commands;
pub mod data_type;
pub mod engine;
pub mod errors;
pub mod info;
pub mod smartlog;
pub mod storage;
mod stream;
pub mod transport;

use info::lo_info;

use crate::data_type::LO_TYPE;
use crate::transport::config::FabricProvider;

use valkey_module::enum_configuration;

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

pub const MODULE_NAME: &str = "largeobj";
pub const MODULE_VERSION: i32 = 1;

// ─── Module Configurations (ValkeyModule Config API) ─────────────────────────

lazy_static::lazy_static! {
    /// Data directory for NVMe object files. Required. Immutable after load.
    static ref CFG_NVME_DIR: Mutex<String> = Mutex::new(String::new());

    /// Total NVMe staging capacity (DRAM for I/O buffers). Default: 64MB.
    /// Used in Tiered mode for read/write staging. Split into uniform
    /// `segment-size` segments: count = ceil(nvme-staging-size / segment-size)
    /// (ceiling so actual staging is never less than requested). Immutable after load.
    static ref CFG_NVME_STAGING_SIZE: AtomicI64 = AtomicI64::new(64 * 1024 * 1024);

    /// Total DRAM budget for cached objects. 0 = no limit (grow on demand).
    /// In Dram mode: all objects live here. In Tiered mode: promotion cache.
    static ref CFG_DRAM_MAXMEMORY: AtomicI64 = AtomicI64::new(0);

    /// Uniform segment size for all pools (DRAMPool and NVMePool).
    /// Growth unit for DRAMPool; NVMe segment count = nvme-staging-size / segment-size.
    /// Default: 64MB. Immutable after load.
    static ref CFG_SEGMENT_SIZE: AtomicI64 = AtomicI64::new(64 * 1024 * 1024);

    /// Max disk usage in nvme-dir. Default: 10GB.
    static ref CFG_NVME_MAXMEMORY: AtomicI64 = AtomicI64::new(10 * 1024 * 1024 * 1024);

    /// Number of tokio worker threads for transport CQ polling. Immutable after load.
    static ref CFG_WORKER_THREADS: AtomicI64 = AtomicI64::new(2);

    /// Max object size eligible for DRAMPool promotion (Tiered mode).
    /// Objects larger than this skip promotion and are always served from NVMe.
    /// Must be < segment-size (an object is staged as one contiguous buffer in one
    /// segment). Default: 64MB, matching the default segment-size. Supports memory
    /// notation (e.g., "64mb").
    static ref CFG_MAX_PROMOTE_SIZE: AtomicI64 = AtomicI64::new(64 * 1024 * 1024);

    /// Scaling cron poll interval in milliseconds. Controls how often the scaling
    /// timer fires to check utilization and memory pressure. Default: 5000ms.
    static ref CFG_SCALING_POLL_MS: AtomicI64 = AtomicI64::new(5000);

    /// NVMe SMART poll interval in seconds (Tiered mode). 0 disables polling
    /// entirely: no background reads, and the INFO section never appears.
    /// Immutable after load — the poller either starts at init or not at all.
    static ref CFG_SMARTLOG_POLL_SECS: AtomicI64 = AtomicI64::new(60);

    /// LFU counter decay: minutes per one-point decrement. 0 disables decay.
    static ref CFG_TIERED_DECAY_TIME: AtomicI64 = AtomicI64::new(1);

    /// How many misses an object must accumulate in the ghost table before a GET
    /// promotes it into DRAMPool. Default: 2.
    static ref CFG_PROMOTE_MIN_HITS: AtomicI64 = AtomicI64::new(2);

    /// Entries sampled per demotion round; the lowest LFU score is demoted.
    static ref CFG_DEMOTE_SAMPLE_SIZE: AtomicI64 = AtomicI64::new(5);

    /// Cap on read fds cached by the FdPool. When full, opening a new fd demotes
    /// the lowest LFU score among samples. 0 means unlimited.
    static ref CFG_MAX_OPEN_FDS: AtomicI64 = AtomicI64::new(1024);

    /// Proactive expand watermark (0.0–1.0). When DRAMPool utilization exceeds this
    /// ratio, a new segment is added ahead of time. Default: 0.80 (80%).
    static ref CFG_SCALING_EXPAND_WATERMARK: AtomicI64 = AtomicI64::new(80); // stored as percent

    /// Shrink watermark (0.0–1.0). When used_memory/maxmemory exceeds this ratio,
    /// the scaling cron evicts the least-used DRAM segment. Default: 0.90 (90%).
    static ref CFG_SCALING_SHRINK_WATERMARK: AtomicI64 = AtomicI64::new(90); // stored as percent

    /// Bench mode: LO.GET TCP path replies with size integer instead of bulk value bytes.
    /// For benchmarking NVMe read throughput without TCP output buffer overhead.
    static ref CFG_BENCH_MODE: AtomicBool = AtomicBool::new(false);

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
    static ref CFG_DIRECT_IO: AtomicBool = AtomicBool::new(true);

    /// Operating mode. Immutable after module load.
    /// - Dram (0): all objects live exclusively in DRAMPool. No NVMe. Fastest reads.
    /// - Tiered (1): objects persist on NVMe, DRAMPool is a read cache with promotion.
    static ref CFG_OPERATING_MODE: Mutex<OperatingMode> = Mutex::new(OperatingMode::Dram);

    // ─── Fabric Configs ──────────────────────────────────────────────────

    /// libfabric provider for transfers. Emulated exercises DMA path over libfabric's tcp provider, EfaDirect needs EFA hardware.
    static ref CFG_FABRIC_PROVIDER: Mutex<FabricProvider> = Mutex::new(FabricProvider::Emulated);

    /// Comma-separated fabric domains to open a service on. Default: All domains.
    static ref CFG_FABRIC_INTERFACES: Mutex<String> = Mutex::new(String::new());

    /// Transfers each fabric service keeps in flight. Default: the crate's provider-derived default.
    static ref CFG_FABRIC_MAX_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);

    /// Threads hashing checksummed transfers off the fabric workers. Default: one.
    static ref CFG_FABRIC_CRC_POOL_THREADS: AtomicI64 = AtomicI64::new(1);

    // ─── Test Hooks ──────────────────────────────────────────────────────

    /// Test-only: pause the tiered SET path for this many milliseconds after
    /// writing data chunks but before calling commit_lo_value. 0 = disabled.
    /// Allows integration tests to inject a DEL in the mid-stream window
    /// and deterministically exercise the delete-during-SET race.
    static ref CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS: AtomicI64 = AtomicI64::new(0);

    // ─── Streaming Configs ───────────────────────────────────────────────

    /// Chunk size for multi-buffer streaming I/O. Default: 8MB.
    /// Determines allocation unit for all I/O operations.
    static ref CFG_CHUNK_SIZE: AtomicI64 = AtomicI64::new(8 * 1024 * 1024);

    /// Max buffers per streaming operation (batch size / pipeline depth). Default: 8.
    static ref CFG_MAX_BUFFERS_PER_OP: AtomicI64 = AtomicI64::new(8);

    /// Min buffers to start a streaming operation. Below this → reject. Default: 2.
    static ref CFG_MIN_BUFFERS_PER_OP: AtomicI64 = AtomicI64::new(2);
}

// ─── Global Runtime ──────────────────────────────────────────────────────────

/// Tokio runtime — owned by the module, handle passed to transport crate.
/// Set once at the end of successful init. Never taken back.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn runtime_handle() -> &'static tokio::runtime::Handle {
    RUNTIME.get().expect("runtime not initialized").handle()
}

/// The Valkey main event-loop thread, captured at module load as its OS thread
/// handle. We use `pthread_self` rather than `std::thread::current()` on purpose:
/// `current()` lazily allocates a `Thread` handle into thread-local storage that
/// LSan reports as a leak at graceful shutdown, and this predicate is called on
/// every teardown thread.
static MAIN_THREAD: OnceLock<libc::pthread_t> = OnceLock::new();

/// True iff the caller runs on the Valkey main event-loop thread.
pub fn is_main_thread() -> bool {
    MAIN_THREAD
        .get()
        // SAFETY: pthread_self/pthread_equal take no pointers and always succeed.
        .is_some_and(|&main| unsafe { libc::pthread_equal(libc::pthread_self(), main) != 0 })
}

// ─── Config Accessors ────────────────────────────────────────────────────────

pub fn nvme_dir() -> String {
    CFG_NVME_DIR
        .lock()
        .expect("CFG_NVME_DIR lock unavailable")
        .clone()
}

pub fn nvme_staging_size() -> usize {
    CFG_NVME_STAGING_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn dram_maxmemory() -> u64 {
    CFG_DRAM_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn dram_segment_size() -> usize {
    CFG_SEGMENT_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

/// Alias for symmetry — both pools use the same segment size.
pub fn segment_size() -> usize {
    CFG_SEGMENT_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn nvme_maxmemory() -> u64 {
    CFG_NVME_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn worker_threads() -> usize {
    CFG_WORKER_THREADS.load(std::sync::atomic::Ordering::Relaxed) as usize
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

pub fn smartlog_poll_secs() -> u64 {
    CFG_SMARTLOG_POLL_SECS.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn tiered_decay_time() -> u64 {
    CFG_TIERED_DECAY_TIME.load(std::sync::atomic::Ordering::Relaxed) as u64
}

pub fn promote_min_hits() -> u8 {
    CFG_PROMOTE_MIN_HITS.load(std::sync::atomic::Ordering::Relaxed) as u8
}

pub fn demote_sample_size() -> usize {
    CFG_DEMOTE_SAMPLE_SIZE.load(std::sync::atomic::Ordering::Relaxed) as usize
}

pub fn max_open_fds() -> usize {
    CFG_MAX_OPEN_FDS.load(std::sync::atomic::Ordering::Relaxed) as usize
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

pub fn operating_mode() -> OperatingMode {
    *CFG_OPERATING_MODE
        .lock()
        .expect("CFG_OPERATING_MODE lock unavailable")
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

pub fn fabric_provider() -> FabricProvider {
    *CFG_FABRIC_PROVIDER
        .lock()
        .expect("CFG_FABRIC_PROVIDER lock unavailable")
}

pub fn fabric_interfaces() -> Vec<String> {
    CFG_FABRIC_INTERFACES
        .lock()
        .expect("CFG_FABRIC_INTERFACES lock unavailable")
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

// ─── Module Lifecycle ────────────────────────────────────────────────────────

fn initialize(ctx: &Context, _args: &[ValkeyString]) -> Status {
    // Any panic on any thread (main, tokio, io_uring poller) aborts the server.
    // We are a no-panic codebase — a panic means a bug, not a recoverable condition.
    std::panic::set_hook(Box::new(|info| {
        valkey_module::logging::log_warning(format!(
            "largeobj: fatal panic — aborting server: {}",
            info
        ));
        std::process::abort();
    }));

    // Record the main event-loop thread. SAFETY: pthread_self takes no arguments
    // and always succeeds; we store the opaque handle for later pthread_equal.
    let _ = MAIN_THREAD.set(unsafe { libc::pthread_self() });

    // Configs are already populated by the valkey_module! macro via module_args_as_configuration.
    let mode = operating_mode();
    let dir = nvme_dir();

    // Reset nvme-dir before use (Tiered mode only; a no-op in Dram, which never
    // touches disk): reclaim any object files a previous run left behind after an
    // unclean exit — a hard crash / SIGKILL never reaches our shutdown handler.
    // If the reset fails we can't guarantee a clean slate, so refuse to load
    // rather than start dirty.
    if let Err(e) = storage::validate_and_clean_nvme_dir(mode, &dir) {
        ctx.log_warning(&format!(
            "largeobj: startup failed to reset nvme-dir {}: {}; aborting module load",
            dir, e
        ));
        return Status::Err;
    }

    // Step 0: Create tokio runtime as a local. Set in OnceLock only after all init succeeds.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads())
        .thread_name("largeobj")
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    // Step 1: open the fabric.
    let mut fabric = match transport::Fabric::start(&transport::config::configuration()) {
        Ok(fabric) => Some(fabric),
        Err(error) => {
            ctx.log_warning(&format!(
                "largeobj: fabric unavailable, EFA path disabled: {error}. Only Large Object Commands of the TCP variant will be supported"
            ));
            None
        }
    };

    // Step 2: Initialize storage layer (pools, io_uring engine, validation).
    // All pool/engine OnceLocks are set inside init() only after everything succeeds.
    // On failure, locals drop naturally — no cleanup needed.
    let storage_summary = match storage::init(mode, &dir) {
        Ok(summary) => summary,
        Err(e) => {
            ctx.log_warning(&format!("largeobj: storage init failed: {}", e));
            return Status::Err;
        }
    };

    // Step 3: Fabric::register_buffers() — fi_mr_reg per segment per server.
    if let Some(fabric) = &mut fabric {
        if let Err(e) = fabric.register_buffers(&storage::all_segment_slices()) {
            ctx.log_warning(&format!("largeobj: EFA buffer registration failed: {}", e));
            // storage::init() already committed pools/engine to OnceLock.
            // EFA registration failure after storage commit is fatal.
            // The user must fix the EFA environment and restart.
            panic!(
                "largeobj: EFA buffer registration failed after storage init: {}",
                e
            );
        }
    }
    let fabric_services = fabric.as_ref().map_or(0, transport::Fabric::service_count);
    if let Some(fabric) = &fabric {
        for (index, address) in fabric.local_addresses().enumerate() {
            ctx.log_notice(&format!(
                "largeobj: fabric service {index} address {}",
                encode_hex(address)
            ));
        }
    }
    transport::commit(fabric);

    // All init succeeded — commit runtime to OnceLock.
    if RUNTIME.set(rt).is_err() {
        panic!("Runtime already initialized");
    }

    ctx.log_notice(&format!(
        "largeobj: initialized {storage_summary}, fabric services: {fabric_services}"
    ));

    // Start the scaling cron (both modes — handles expand and shrink based on mode).
    storage::scaling::rearm_scaling_cron(ctx, scaling_poll_ms());

    Status::Ok
}

/// MODULE UNLOAD entry point.
///
/// NOTE: modules that define data types cannot be unloaded by Valkey Core, so
/// this function is not called (this module defines LO_TYPE). All teardown
/// therefore lives in `on_server_shutdown`, which fires on graceful server
/// shutdown.
fn deinitialize(_ctx: &Context) -> Status {
    Status::Ok
}

/// Clean up on graceful server shutdown (SIGINT / SIGTERM / SHUTDOWN command):
/// signal the SMART log poller to stop, drop the fabric services, signal the
/// io_uring poller to stop, and — in Tiered
/// mode — wipe nvme-dir so object files don't accumulate across server lifetimes.
/// Process exit frees all remaining resources (pools, runtime, transport).
/// A hard crash (SIGKILL / SIGSEGV / power loss) never reaches this handler;
/// those leftovers are reclaimed by the startup reset in `initialize`.
#[shutdown_event_handler]
fn on_server_shutdown(ctx: &Context, _subevent: u64) {
    smartlog::signal_shutdown();
    transport::shutdown();
    let dir = nvme_dir();
    if let Err(e) = storage::validate_and_clean_nvme_dir(operating_mode(), &dir) {
        ctx.log_warning(&format!(
            "largeobj: shutdown cleanup failed to reset nvme-dir {}: {}",
            dir, e
        ));
    }
}

valkey_module! {
    name: MODULE_NAME,
    version: MODULE_VERSION,
    allocator: (valkey_module::alloc::ValkeyAlloc, valkey_module::alloc::ValkeyAlloc),
    data_types: [LO_TYPE],
    init: initialize,
    deinit: deinitialize,
    info: lo_info,
    commands: [
        ["LO.HELLO", commands::lo_hello, "write", 0, 0, 0],
        ["LO.GET", commands::lo_get, "readonly", 1, 1, 1],
        ["LO.SET", commands::lo_set, "write deny-oom", 1, 1, 1],
        ["LO.INFO", commands::lo_info, "readonly fast", 1, 1, 1],
    ],
    configurations: [
        i64: [
            ["dram-maxmemory", &*CFG_DRAM_MAXMEMORY, 0, 0, i64::MAX,
             ConfigurationFlags::MEMORY, None, None],
            ["segment-size", &*CFG_SEGMENT_SIZE, 67_108_864, 1_048_576, 1_073_741_824,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["nvme-staging-size", &*CFG_NVME_STAGING_SIZE, 67_108_864, 1_048_576, 1_073_741_824,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["nvme-maxmemory", &*CFG_NVME_MAXMEMORY, 10_737_418_240, 1_048_576, i64::MAX,
             ConfigurationFlags::MEMORY, None, None],
            ["worker-threads", &*CFG_WORKER_THREADS, 2, 1, 32,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["max-promote-size", &*CFG_MAX_PROMOTE_SIZE, 67_108_864, 0, 1_099_511_627_776,
             ConfigurationFlags::MEMORY, None, None],
            ["chunk-size", &*CFG_CHUNK_SIZE, 8_388_608, 4096, 268_435_456,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["max-buffers-per-op", &*CFG_MAX_BUFFERS_PER_OP, 8, 2, 64,
             ConfigurationFlags::DEFAULT, None,
             Some(Box::new(|_ctx, _name, new_val| {
                 let max = new_val.load(std::sync::atomic::Ordering::Relaxed);
                 let min = CFG_MIN_BUFFERS_PER_OP.load(std::sync::atomic::Ordering::Relaxed);
                 if min > max {
                     Err(valkey_module::ValkeyError::Str("ERR max-buffers-per-op must be >= min-buffers-per-op"))
                 } else {
                     Ok(())
                 }
             }))],
            ["min-buffers-per-op", &*CFG_MIN_BUFFERS_PER_OP, 2, 1, 64,
             ConfigurationFlags::DEFAULT, None,
             Some(Box::new(|_ctx, _name, new_val| {
                 let min = new_val.load(std::sync::atomic::Ordering::Relaxed);
                 let max = CFG_MAX_BUFFERS_PER_OP.load(std::sync::atomic::Ordering::Relaxed);
                 if min > max {
                     Err(valkey_module::ValkeyError::Str("ERR min-buffers-per-op must be <= max-buffers-per-op"))
                 } else {
                     Ok(())
                 }
             }))],
            ["test-pause-before-finalize-set-ms", &*CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS, 0, 0, 60_000,
             ConfigurationFlags::HIDDEN, None, None],
            ["scaling-poll-ms", &*CFG_SCALING_POLL_MS, 5_000, 1_000, 60_000,
             ConfigurationFlags::DEFAULT, None, None],
            ["smartlog-poll-secs", &*CFG_SMARTLOG_POLL_SECS, 60, 0, 86_400,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["scaling-expand-watermark", &*CFG_SCALING_EXPAND_WATERMARK, 80, 50, 95,
             ConfigurationFlags::DEFAULT, None, None],
            ["scaling-shrink-watermark", &*CFG_SCALING_SHRINK_WATERMARK, 90, 50, 95,
             ConfigurationFlags::DEFAULT, None, None],
            ["tiered-decay-time", &*CFG_TIERED_DECAY_TIME, 1, 0, 65_535,
             ConfigurationFlags::DEFAULT, None, None],
            ["promote-min-hits", &*CFG_PROMOTE_MIN_HITS, 2, 1, 255,
             ConfigurationFlags::DEFAULT, None, None],
            ["demote-sample-size", &*CFG_DEMOTE_SAMPLE_SIZE, 5, 1, 64,
             ConfigurationFlags::DEFAULT, None, None],
            ["max-open-fds", &*CFG_MAX_OPEN_FDS, 1024, 0, 1_048_576,
             ConfigurationFlags::DEFAULT, None, None],
            ["fabric-max-in-flight", &*CFG_FABRIC_MAX_IN_FLIGHT, 0, 0, 65_536,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["fabric-crc-pool-threads", &*CFG_FABRIC_CRC_POOL_THREADS, 1, 1, 1024,
             ConfigurationFlags::IMMUTABLE, None, None],
        ],
        string: [
            ["nvme-dir", &*CFG_NVME_DIR, "", ConfigurationFlags::IMMUTABLE, None],
            ["fabric-interfaces", &*CFG_FABRIC_INTERFACES, "", ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [
            ["bench-mode", &*CFG_BENCH_MODE, false, ConfigurationFlags::HIDDEN, None],
            ["direct-io", &*CFG_DIRECT_IO, true, ConfigurationFlags::IMMUTABLE, None],
        ],
        enum: [
            ["operating-mode", &*CFG_OPERATING_MODE, OperatingMode::Dram,
             ConfigurationFlags::IMMUTABLE, None],
            ["fabric-provider", &*CFG_FABRIC_PROVIDER, FabricProvider::Emulated,
             ConfigurationFlags::IMMUTABLE, None],
        ],
        module_args_as_configuration: true,
    ]
}

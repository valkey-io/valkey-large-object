//! ValkeyLargeObj: Large Object Module + Transport Crate
//!
//! Architecture (from interface doc):
//!   Data Type (commands, LoValue, keyspace)
//!       ↓ calls
//!   Storage (buffer pool, io_uring, NVMe files)
//!       ↓ passes buffers to
//!   Transport (EFA, fi_write/fi_read)
//!
//! Commands: BLOB.HELLO, BLOB.GET, BLOB.SET
//! Deletion: native Valkey DEL triggers module free callback.

// ─── Initialization Order ────────────────────────────────────────────────────
//
// Module init proceeds in strict order. Commands are safe to call ONLY after
// all steps complete:
//
//   1. Fabric::start()         — one libfabric service per domain. On missing
//                                fabric, the EFA path is unavailable and BLOB.HELLO
//                                gives an error.
//   2. storage::init(mode, nvme_dir)
//                              — validate config, allocate pool segments, create
//                                the per-pool io_uring engines and start the
//                                smartlog poller (Tiered only). All resources are
//                                created as locals; OnceLock statics are set only
//                                after everything succeeds. On failure, locals
//                                drop naturally — module load retryable.
//   3. fabric.register_segment() per startup segment
//                              — fi_mr_reg pool buffers with EFA domains.
//   4. RUNTIME.set(rt)         — commit tokio runtime last (only used by commands).
//
// After step 4, commands (BLOB.GET, BLOB.SET, BLOB.HELLO) may execute safely.
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
    static ref CFG_NVME_STAGING_SIZE: AtomicI64 = AtomicI64::new(1024 * 1024 * 1024);

    /// Uniform segment size for all pools (DRAMPool and NVMePool).
    /// Growth unit for DRAMPool; NVMe segment count = nvme-staging-size / segment-size.
    /// Default: 1 GiB. Immutable after load.
    static ref CFG_SEGMENT_SIZE: AtomicI64 = AtomicI64::new(1024 * 1024 * 1024);

    /// Max disk usage in nvme-dir. Default: 0 (unlimited).
    static ref CFG_NVME_MAXMEMORY: AtomicI64 = AtomicI64::new(0);

    /// Number of tokio worker threads for transport CQ polling. Immutable after load.
    static ref CFG_WORKER_THREADS: AtomicI64 = AtomicI64::new(2);

    /// Max object size eligible for DRAMPool promotion (Tiered mode).
    /// Objects larger than this skip promotion and are always served from NVMe.
    /// Must fit in one segment (object_fits_segment check). Default: 256 MiB.
    /// Supports memory notation (e.g., "256mb"). Refer to object_fits_segment().
    static ref CFG_MAX_PROMOTE_SIZE: AtomicI64 = AtomicI64::new(256 * 1024 * 1024);

    /// Scaling cron poll interval in milliseconds. Controls how often the scaling
    /// timer fires to check utilization and memory pressure. Default: 5000ms.
    static ref CFG_SCALING_POLL_MS: AtomicI64 = AtomicI64::new(5000);

    /// NVMe SMART poll interval in seconds (Tiered mode). 0 disables polling
    /// entirely: no background reads, and the INFO section never appears.
    /// Immutable after load — the poller either starts at init or not at all.
    static ref CFG_SMARTLOG_POLL_SECS: AtomicI64 = AtomicI64::new(60);

    /// Proactive expand watermark (0.0–1.0). When DRAMPool utilization exceeds this
    /// ratio, a new segment is added ahead of time. Default: 0.80 (80%).
    static ref CFG_SCALING_EXPAND_WATERMARK: AtomicI64 = AtomicI64::new(80); // stored as percent

    /// Shrink watermark (0.0–1.0). When used_memory/maxmemory exceeds this ratio,
    /// the scaling cron evicts the least-used DRAM segment. Default: 0.90 (90%).
    static ref CFG_SCALING_SHRINK_WATERMARK: AtomicI64 = AtomicI64::new(90); // stored as percent

    /// Bench mode: BLOB.GET TCP path replies with size integer instead of bulk value bytes.
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

    /// Maximum allowed object size for BLOB.SET. Rejects writes exceeding this limit.
    /// Default: 512 MiB. Must fit in one segment in Dram mode (object_fits_segment
    /// check). Supports memory notation (e.g., "512mb").
    static ref CFG_MAX_OBJECT_SIZE: AtomicI64 = AtomicI64::new(512 * 1024 * 1024);
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

/// Register an expanded segment's memory with EFA, if a fabric is up (no-op otherwise). The
/// Register a segment with EFA on every fabric service; no-op when no fabric is up. A failure is
/// fatal and panics: an unregistered segment can't be served over EFA (the per-transfer fallback
/// fails the same way) and the failure isn't transient, so a retry won't help. Single owner of
/// this fatal-on-failure policy — startup and `try_expand` both call it.
pub fn efa_register_segment(slice: &'static [u8]) {
    let Some(fabric) = transport::fabric::fabric() else {
        return; // TCP-only: no fabric, nothing to register.
    };
    if let Err(e) = fabric.register_segment(slice) {
        panic!(
            "largeobj: EFA registration of segment at {:p} failed (fatal): {e}",
            slice.as_ptr()
        );
    }
}

/// Release a segment's EFA registration before its memory is freed; no-op when no fabric is up or
/// the base was never registered. Called from `Segment::drop`.
pub fn efa_release_segment(base: usize) {
    if let Some(fabric) = transport::fabric::fabric() {
        fabric.release_segment(base);
    }
}

/// Count of EFA-registered segments (0 when no fabric). Equals the live segment count when the
/// fabric is up (every live segment is registered); surfaced in INFO to assert that invariant.
pub fn efa_registered_segment_count() -> usize {
    transport::fabric::registered_segment_count()
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

pub fn max_object_size() -> u64 {
    CFG_MAX_OBJECT_SIZE.load(std::sync::atomic::Ordering::Relaxed) as u64
}

// ─── Config Constraints ──────────────────────────────────────────────────────
//
// List of constraints between configs. Each constraint enforces
// parent >= child when its condition is true, unless it supplies a custom
// validator predicate.
//
// At module load (before RUNTIME is set), per-config callbacks skip validation
// (config processing order is non-deterministic across type categories).
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

// SAFETY: All AtomicI64 references are to lazy_static statics with 'static lifetime.
// The fn pointers and &str are inherently Send+Sync.
unsafe impl Sync for ConfigConstraint {}

fn config_constraints() -> &'static [ConfigConstraint] {
    use std::sync::LazyLock;
    static CONSTRAINTS: LazyLock<Vec<ConfigConstraint>> = LazyLock::new(|| {
        vec![
            ConfigConstraint {
                parent: &CFG_NVME_MAXMEMORY,
                child: &CFG_MAX_OBJECT_SIZE,
                enforce_condition: || operating_mode() == OperatingMode::Tiered,
                // 0 = unlimited; skip the check.
                validator: Some(|p, c| p == 0 || p >= c),
                error_msg: errors::ERR_NVME_GE_MAX_OBJ,
            },
            ConfigConstraint {
                parent: &CFG_NVME_STAGING_SIZE,
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
fn validate_all_constraints() -> Result<(), String> {
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
    Ok(())
}

/// Shared validation callback for mutable configs participating in the config
/// constraints. At initial load, defers to validate_all_constraints() in initialize().
/// At runtime (CONFIG SET), the framework has already stored the new value, so
/// we just re-check all constraints against current atomics.
fn validate_config_constraint(
    _ctx: &valkey_module::configuration::ConfigurationContext,
    _name: &str,
    _val: &'static AtomicI64,
) -> Result<(), valkey_module::ValkeyError> {
    if RUNTIME.get().is_none() {
        return Ok(());
    }
    validate_all_constraints().map_err(valkey_module::ValkeyError::String)
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

    // Validate cross-config constraints now that all configs are finalized.
    // Per-config callbacks skip validation at load time (non-deterministic processing
    // order across config types); this is the single enforcement point at startup.
    if let Err(e) = validate_all_constraints() {
        ctx.log_warning(&format!("largeobj: config validation failed: {e}"));
        return Status::Err;
    }

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
    let fabric = match transport::Fabric::start(&transport::config::configuration()) {
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

    let fabric_services = fabric.as_ref().map_or(0, transport::Fabric::service_count);
    if let Some(fabric) = &fabric {
        for (index, address) in fabric.local_addresses().enumerate() {
            ctx.log_notice(&format!(
                "largeobj: fabric service {index} address {}",
                encode_hex(address)
            ));
        }
    }
    // Commit BEFORE registering: efa_register_segment reads the committed global fabric.
    transport::commit(fabric);

    // Register every startup segment with EFA (fatal on failure — see efa_register_segment).
    for segment in storage::all_segment_slices() {
        efa_register_segment(segment);
    }

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
/// signal the SMART log poller to stop, drop the fabric services, and — in Tiered
/// mode — wipe nvme-dir so object files don't accumulate across server lifetimes.
/// The io_uring engines live in process-lifetime statics and are not torn down here.
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
        ["BLOB.HELLO", commands::lo_hello, "write", 0, 0, 0],
        ["BLOB.GET", commands::lo_get, "readonly", 1, 1, 1],
        ["BLOB.SET", commands::lo_set, "write deny-oom", 1, 1, 1],
        ["BLOB.INFO", commands::lo_info, "readonly fast", 1, 1, 1],
    ],
    configurations: [
        i64: [
            ["segment-size", &*CFG_SEGMENT_SIZE, 1_073_741_824, 1_048_576, 1_073_741_824,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["nvme-staging-size", &*CFG_NVME_STAGING_SIZE, 1_073_741_824, 1_048_576, 1_073_741_824,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["nvme-maxmemory", &*CFG_NVME_MAXMEMORY, 0, 0, i64::MAX,
             ConfigurationFlags::MEMORY, None, Some(Box::new(validate_config_constraint))],
            ["worker-threads", &*CFG_WORKER_THREADS, 2, 1, 32,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["max-promote-size", &*CFG_MAX_PROMOTE_SIZE, 268_435_456, 0, 1_099_511_627_776,
             ConfigurationFlags::MEMORY, None, Some(Box::new(validate_config_constraint))],
            ["chunk-size", &*CFG_CHUNK_SIZE, 8_388_608, 4096, 268_435_456,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["max-buffers-per-op", &*CFG_MAX_BUFFERS_PER_OP, 8, 2, 64,
             ConfigurationFlags::DEFAULT, None, Some(Box::new(validate_config_constraint))],
            ["min-buffers-per-op", &*CFG_MIN_BUFFERS_PER_OP, 2, 1, 64,
             ConfigurationFlags::DEFAULT, None, Some(Box::new(validate_config_constraint))],
            ["test-pause-before-finalize-set-ms", &*CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS, 0, 0, 60_000,
             ConfigurationFlags::HIDDEN, None, None],
            ["scaling-poll-ms", &*CFG_SCALING_POLL_MS, 5_000, 1_000, 60_000,
             ConfigurationFlags::DEFAULT, None, None],
            ["smartlog-poll-secs", &*CFG_SMARTLOG_POLL_SECS, 60, 0, 86_400,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["scaling-expand-watermark", &*CFG_SCALING_EXPAND_WATERMARK, 80, 50, 95,
             ConfigurationFlags::DEFAULT, None, None],
            ["max-object-size", &*CFG_MAX_OBJECT_SIZE, 536_870_912, 1, i64::MAX,
             ConfigurationFlags::MEMORY, None, Some(Box::new(validate_config_constraint))],
            ["scaling-shrink-watermark", &*CFG_SCALING_SHRINK_WATERMARK, 90, 50, 95,
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

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::Relaxed;

    // ─── Test Infrastructure ─────────────────────────────────────────────

    /// Reset all constrained configs to their compile-time defaults. Other tests
    /// (e.g. segment_pool) may mutate shared statics without restoring them.
    fn reset_constrained_defaults() {
        CFG_NVME_MAXMEMORY.store(0, Relaxed); // 0 = unlimited
        CFG_NVME_STAGING_SIZE.store(1024 * 1024 * 1024, Relaxed); // 1 GiB
        CFG_SEGMENT_SIZE.store(1024 * 1024 * 1024, Relaxed); // 1 GiB
        CFG_CHUNK_SIZE.store(8 * 1024 * 1024, Relaxed); // 8 MiB
        CFG_MAX_OBJECT_SIZE.store(512 * 1024 * 1024, Relaxed); // 512 MiB
        CFG_MAX_PROMOTE_SIZE.store(256 * 1024 * 1024, Relaxed); // 256 MiB
        CFG_MAX_BUFFERS_PER_OP.store(8, Relaxed);
        CFG_MIN_BUFFERS_PER_OP.store(2, Relaxed);
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
            (&CFG_NVME_MAXMEMORY, CFG_NVME_MAXMEMORY.load(Relaxed)),
            (&CFG_NVME_STAGING_SIZE, CFG_NVME_STAGING_SIZE.load(Relaxed)),
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
                vec![(&CFG_NVME_MAXMEMORY, 1)],
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
                    (&CFG_NVME_STAGING_SIZE, chunk - 1),
                    (&CFG_MAX_OBJECT_SIZE, chunk - 1),
                ],
                Some("segment-size must be >= chunk-size"),
            ),
            (
                "segment_lt_max_obj_rejected",
                OperatingMode::Dram,
                vec![
                    (&CFG_SEGMENT_SIZE, max_obj - 1),
                    (&CFG_NVME_STAGING_SIZE, max_obj - 1),
                ],
                Some("segment-size is insufficient for max-object-size"),
            ),
            (
                "segment_lt_promote_rejected",
                OperatingMode::Tiered,
                vec![(&CFG_MAX_PROMOTE_SIZE, segment + 1)],
                Some("segment-size is insufficient for max-promote-size"),
            ),
            (
                "staging_lt_segment_rejected",
                OperatingMode::Tiered,
                vec![(&CFG_NVME_STAGING_SIZE, segment - 1)],
                Some("nvme-staging-size must be >= segment-size"),
            ),
            (
                "buffers_max_lt_min_rejected",
                OperatingMode::Dram,
                vec![(&CFG_MAX_BUFFERS_PER_OP, min_buf - 1)],
                Some("max-buffers-per-op must be >= min-buffers-per-op"),
            ),
            (
                "tiered_mode_nvme_lt_max_obj_rejected",
                OperatingMode::Tiered,
                vec![(&CFG_NVME_MAXMEMORY, max_obj - 1)],
                Some("nvme-maxmemory must be >= max-object-size in Tiered mode"),
            ),
        ]
    }

    #[test]
    fn test_validate_all_constraints() {
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

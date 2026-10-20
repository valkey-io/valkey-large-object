//! valkey-large-object: Large Object Module + Transport Crate
//!
//! Architecture (from interface doc):
//!   Data Type (commands, LoValue, keyspace)
//!       ↓ calls
//!   Storage (buffer pool, io_uring, NVMe files)
//!       ↓ passes buffers to
//!   Transport (EFA, fi_write/fi_read)
//!
//! Commands: BLOB.RDMA_HELLO, BLOB.TCP_GET, BLOB.RDMA_GET, BLOB.TCP_SET, BLOB.RDMA_SET
//! Deletion: native Valkey DEL triggers module free callback.

// ─── Initialization Order ────────────────────────────────────────────────────
//
// Module init proceeds in strict order. Commands are safe to call ONLY after
// all steps complete:
//
//   1. Fabric::start()         — one libfabric service per domain. On missing
//                                fabric, the EFA path is unavailable and BLOB.RDMA_HELLO
//                                gives an error.
//   2. storage::init(ctx, mode, disk_dir)
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
// After step 4, commands (BLOB.TCP_GET, BLOB.RDMA_GET, BLOB.TCP_SET, BLOB.RDMA_SET, BLOB.RDMA_HELLO) may execute safely.
// ─────────────────────────────────────────────────────────────────────────────

use dma_libfabric_protocol::encode_hex;
use valkey_module::configuration::ConfigurationFlags;
use valkey_module::{valkey_module, Context, InfoContext, Status, ValkeyResult, ValkeyString};
use valkey_module_macros::shutdown_event_handler;

use tokio::runtime::Runtime;

pub mod commands;
pub mod configs;
pub mod data_type;
pub mod engine;
pub mod errors;
pub mod info;
pub mod smartlog;
pub mod storage;
mod stream;
pub mod transport;

pub use configs::*;

use info::lo_info;

use crate::data_type::LO_TYPE;
use crate::transport::config::FabricProvider;

pub const MODULE_NAME: &str = "largeobj";
pub const MODULE_VERSION: i32 = 1;

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

// ─── EFA Registration Helpers ────────────────────────────────────────────────

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
    let dir = disk_dir();

    // Validate cross-config constraints now that all configs are finalized.
    // Per-config callbacks skip validation at load time (non-deterministic processing
    // order across config types); this is the single enforcement point at startup.
    if let Err(e) = validate_all_constraints() {
        ctx.log_warning(&format!("largeobj: config validation failed: {e}"));
        return Status::Err;
    }

    // Reset disk-dir before use (Tiered mode only; a no-op in Dram, which never
    // touches disk): reclaim any object files a previous run left behind after an
    // unclean exit — a hard crash / SIGKILL never reaches our shutdown handler.
    // If the reset fails we can't guarantee a clean slate, so refuse to load
    // rather than start dirty.
    if let Err(e) = storage::validate_and_clean_disk_dir(mode, &dir) {
        ctx.log_warning(&format!(
            "largeobj: startup failed to reset disk-dir {}: {}; aborting module load",
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
                "largeobj: fabric unavailable, RDMA path disabled: {error}. Only Large Object Commands of the TCP variant will be supported"
            ));
            None
        }
    };

    // Step 2: Initialize storage layer (pools, io_uring engine, validation).
    // All pool/engine OnceLocks are set inside init() only after everything succeeds.
    // On failure, locals drop naturally — no cleanup needed.
    let storage_summary = match storage::init(ctx, mode, &dir) {
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
                "largeobj: fabric service {index} rdma address {}",
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

    // Mark module as initialized so runtime CONFIG SET validation is enabled.
    MODULE_INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);

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
/// mode — wipe disk-dir so object files don't accumulate across server lifetimes.
/// The io_uring engines live in process-lifetime statics and are not torn down here.
/// Process exit frees all remaining resources (pools, runtime, transport).
/// A hard crash (SIGKILL / SIGSEGV / power loss) never reaches this handler;
/// those leftovers are reclaimed by the startup reset in `initialize`.
#[shutdown_event_handler]
fn on_server_shutdown(ctx: &Context, _subevent: u64) {
    smartlog::signal_shutdown();
    transport::shutdown();
    let dir = disk_dir();
    if let Err(e) = storage::validate_and_clean_disk_dir(operating_mode(), &dir) {
        ctx.log_warning(&format!(
            "largeobj: shutdown cleanup failed to reset disk-dir {}: {}",
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
    acl_categories: [
        "largeobj",
    ]
    commands: [
        ["BLOB.TCP_GET", commands::lo_tcp_get, "readonly fast", 1, 1, 1, "read largeobj fast"],
        ["BLOB.TCP_SET", commands::lo_tcp_set, "write deny-oom fast", 1, 1, 1, "write largeobj fast"],
        ["BLOB.RDMA_HELLO", commands::lo_rdma_hello, "fast", 0, 0, 0, "connection largeobj fast"],
        ["BLOB.RDMA_GET", commands::lo_rdma_get, "readonly fast", 1, 1, 1, "read largeobj fast"],
        ["BLOB.RDMA_SET", commands::lo_rdma_set, "write deny-oom fast", 1, 1, 1, "write largeobj fast"],
        ["BLOB.INFO", commands::lo_info, "readonly fast", 1, 1, 1, "read largeobj fast"],
    ],
    configurations: [
        i64: [
            ["segment-size", &*CFG_SEGMENT_SIZE, DEFAULT_SEGMENT_SIZE, MIN_SEGMENT_SIZE, MAX_SEGMENT_SIZE,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["disk-staging-size", &*CFG_DISK_STAGING_SIZE, DEFAULT_DISK_STAGING_SIZE, MIN_DISK_STAGING_SIZE, MAX_DISK_STAGING_SIZE,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["disk-maxmemory", &*CFG_DISK_MAXMEMORY, DEFAULT_DISK_MAXMEMORY, MIN_DISK_MAXMEMORY, MAX_DISK_MAXMEMORY,
             ConfigurationFlags::MEMORY, None, Some(Box::new(validate_config_constraint))],
            ["worker-threads", &*CFG_WORKER_THREADS, DEFAULT_WORKER_THREADS, MIN_WORKER_THREADS, MAX_WORKER_THREADS,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["max-promote-size", &*CFG_MAX_PROMOTE_SIZE, DEFAULT_MAX_PROMOTE_SIZE, MIN_MAX_PROMOTE_SIZE, MAX_MAX_PROMOTE_SIZE,
             ConfigurationFlags::MEMORY, None, Some(Box::new(validate_config_constraint))],
            ["chunk-size", &*CFG_CHUNK_SIZE, DEFAULT_CHUNK_SIZE, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE,
             ConfigurationFlags::IMMUTABLE | ConfigurationFlags::MEMORY, None, None],
            ["max-buffers-per-op", &*CFG_MAX_BUFFERS_PER_OP, DEFAULT_MAX_BUFFERS_PER_OP, MIN_MAX_BUFFERS_PER_OP, MAX_MAX_BUFFERS_PER_OP,
             ConfigurationFlags::DEFAULT, None, Some(Box::new(validate_config_constraint))],
            ["min-buffers-per-op", &*CFG_MIN_BUFFERS_PER_OP, DEFAULT_MIN_BUFFERS_PER_OP, MIN_MIN_BUFFERS_PER_OP, MAX_MIN_BUFFERS_PER_OP,
             ConfigurationFlags::DEFAULT, None, Some(Box::new(validate_config_constraint))],
            ["test-pause-before-finalize-set-ms", &*CFG_TEST_PAUSE_BEFORE_FINALIZE_SET_MS, DEFAULT_TEST_PAUSE_BEFORE_FINALIZE_SET_MS, MIN_TEST_PAUSE_BEFORE_FINALIZE_SET_MS, MAX_TEST_PAUSE_BEFORE_FINALIZE_SET_MS,
             ConfigurationFlags::HIDDEN | ConfigurationFlags::IMMUTABLE, None, None],
            ["scaling-poll-ms", &*CFG_SCALING_POLL_MS, DEFAULT_SCALING_POLL_MS, MIN_SCALING_POLL_MS, MAX_SCALING_POLL_MS,
             ConfigurationFlags::DEFAULT, None, None],
            ["reaper-scan-budget-us", &*CFG_REAPER_SCAN_BUDGET_US, DEFAULT_REAPER_SCAN_BUDGET_US, MIN_REAPER_SCAN_BUDGET_US, MAX_REAPER_SCAN_BUDGET_US,
             ConfigurationFlags::DEFAULT, None, None],
            ["reaper-poll-ms", &*CFG_REAPER_POLL_MS, DEFAULT_REAPER_POLL_MS, MIN_REAPER_POLL_MS, MAX_REAPER_POLL_MS,
             ConfigurationFlags::DEFAULT, None, None],
            ["smartlog-poll-secs", &*CFG_SMARTLOG_POLL_SECS, DEFAULT_SMARTLOG_POLL_SECS, MIN_SMARTLOG_POLL_SECS, MAX_SMARTLOG_POLL_SECS,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["scaling-expand-watermark", &*CFG_SCALING_EXPAND_WATERMARK, DEFAULT_SCALING_EXPAND_WATERMARK, MIN_SCALING_EXPAND_WATERMARK, MAX_SCALING_EXPAND_WATERMARK,
             ConfigurationFlags::DEFAULT, None, None],
            ["max-object-size", &*CFG_MAX_OBJECT_SIZE, DEFAULT_MAX_OBJECT_SIZE, MIN_MAX_OBJECT_SIZE, MAX_MAX_OBJECT_SIZE,
             ConfigurationFlags::MEMORY, None, Some(Box::new(validate_config_constraint))],
            ["scaling-shrink-watermark", &*CFG_SCALING_SHRINK_WATERMARK, DEFAULT_SCALING_SHRINK_WATERMARK, MIN_SCALING_SHRINK_WATERMARK, MAX_SCALING_SHRINK_WATERMARK,
             ConfigurationFlags::DEFAULT, None, None],
            ["blob-decay-time", &*CFG_BLOB_DECAY_TIME, DEFAULT_BLOB_DECAY_TIME, MIN_BLOB_DECAY_TIME, MAX_BLOB_DECAY_TIME,
             ConfigurationFlags::DEFAULT, None, None],
            ["promote-min-hits", &*CFG_PROMOTE_MIN_HITS, DEFAULT_PROMOTE_MIN_HITS, MIN_PROMOTE_MIN_HITS, MAX_PROMOTE_MIN_HITS,
             ConfigurationFlags::DEFAULT, None, None],
            ["victim-sample-size", &*CFG_VICTIM_SAMPLE_SIZE, DEFAULT_VICTIM_SAMPLE_SIZE, MIN_VICTIM_SAMPLE_SIZE, MAX_VICTIM_SAMPLE_SIZE,
             ConfigurationFlags::DEFAULT, None, None],
            ["max-cached-fds", &*CFG_MAX_CACHED_FDS, DEFAULT_MAX_CACHED_FDS, MIN_MAX_CACHED_FDS, MAX_MAX_CACHED_FDS,
             ConfigurationFlags::DEFAULT, Some(Box::new(on_max_cached_fds_changed)), Some(Box::new(validate_max_cached_fds))],
            ["fabric-max-in-flight", &*CFG_FABRIC_MAX_IN_FLIGHT, DEFAULT_FABRIC_MAX_IN_FLIGHT, MIN_FABRIC_MAX_IN_FLIGHT, MAX_FABRIC_MAX_IN_FLIGHT,
             ConfigurationFlags::IMMUTABLE, None, None],
            ["fabric-crc-pool-threads", &*CFG_FABRIC_CRC_POOL_THREADS, DEFAULT_FABRIC_CRC_POOL_THREADS, MIN_FABRIC_CRC_POOL_THREADS, MAX_FABRIC_CRC_POOL_THREADS,
             ConfigurationFlags::IMMUTABLE, None, None],
        ],
        string: [
            ["disk-dir", &*CFG_DISK_DIR, "", ConfigurationFlags::IMMUTABLE, None, Some(Box::new(on_string_config_set))],
            ["fabric-interfaces", &*CFG_FABRIC_INTERFACES, "", ConfigurationFlags::IMMUTABLE, None, Some(Box::new(on_string_config_set))],
        ],
        bool: [
            ["bench-mode", &*CFG_BENCH_MODE, false, ConfigurationFlags::HIDDEN | ConfigurationFlags::IMMUTABLE, None],
            ["test-rdma-fail-partial", &*CFG_TEST_RDMA_FAIL_PARTIAL, false, ConfigurationFlags::HIDDEN | ConfigurationFlags::IMMUTABLE, None],
            ["test-fail-unlink", &*CFG_TEST_FAIL_UNLINK, false, ConfigurationFlags::HIDDEN | ConfigurationFlags::IMMUTABLE, None],
            ["direct-io", &*CFG_DIRECT_IO, true, ConfigurationFlags::IMMUTABLE, None],
        ],
        enum: [
            ["operating-mode", &*CFG_OPERATING_MODE, OperatingMode::Dram,
             ConfigurationFlags::IMMUTABLE, None],
            ["fabric-provider", &*CFG_FABRIC_PROVIDER, FabricProvider::EfaDirect,
             ConfigurationFlags::IMMUTABLE, None],
            ["fd-reserve", &*CFG_FD_RESERVE, FdReserve::Lenient,
             ConfigurationFlags::IMMUTABLE, None],
        ],
        module_args_as_configuration: true,
    ]
}

//! INFO largeobj — pool and error statistics exposed via `INFO largeobj`.
//!
//! Add new subsections by adding a `fn *_section(ctx) -> ValkeyResult<()>` and
//! calling it from `info_sections`. Each section is a discrete group of fields.

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::{InfoContext, ValkeyResult};

use crate::smartlog::{snapshot_for_info, CRITICAL_WARNING_BITS};
use crate::storage;
use crate::{operating_mode, OperatingMode};

// ─── Error Metrics ───────────────────────────────────────────────────────────

pub static NVME_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static NVME_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static EFA_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static EFA_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static DRAM_POOL_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static NVME_BUFFER_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static NVME_CAPACITY_EXCEEDED: AtomicU64 = AtomicU64::new(0);
pub static SET_FINALIZE_STALE: AtomicU64 = AtomicU64::new(0);
pub static SET_VALUE_FAILURES: AtomicU64 = AtomicU64::new(0);

// ─── Core Metrics ────────────────────────────────────────────────────────────

/// Live `LoValue` instances: +1 in `LoValue::new`, −1 in its `Drop`. Equals the
/// LargeObject keys in the keyspace, plus a SET's value briefly before commit.
pub static LARGE_OBJECT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Main INFO handler, registered in `valkey_module!` as `info: lo_info`.
pub fn lo_info(ctx: &InfoContext, _for_crash_report: bool) {
    if let Err(e) = info_sections(ctx) {
        valkey_module::logging::log_warning(format!("lo_info: failed to emit INFO: {e}"));
    }
}

fn info_sections(ctx: &InfoContext) -> ValkeyResult<()> {
    core_metrics_section(ctx)?;
    dram_pool_section(ctx)?;
    nvme_staging_section(ctx)?;
    fd_pool_section(ctx)?;
    smartlog_section(ctx)?;
    error_metrics_section(ctx)?;
    Ok(())
}

/// Module-wide stats, independent of operating mode (same shape as valkey-bloom's
/// `bloom_core_metrics`).
fn core_metrics_section(ctx: &InfoContext) -> ValkeyResult<()> {
    ctx.builder()
        .add_section("core_metrics")
        .field(
            "num_objects",
            LARGE_OBJECT_COUNT.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "pending_reclaims",
            storage::reclaim::RECLAIM_LIST.len() as i64,
        )?
        .field(
            "reclaims",
            storage::get_dram_pool().reclaims.load(Ordering::Relaxed) as i64,
        )?
        .build_section()?
        .build_info()?;
    Ok(())
}

/// Tiered only: absent in Dram mode, where no FdPool exists.
fn fd_pool_section(ctx: &InfoContext) -> ValkeyResult<()> {
    let Some(fds) = storage::FD_POOL.get() else {
        return Ok(());
    };
    ctx.builder()
        .add_section("fd")
        .field("open_fds", fds.len() as i64)?
        .field(
            "fd_reclaims",
            fds.reclaims.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

fn dram_pool_section(ctx: &InfoContext) -> ValkeyResult<()> {
    let dram = storage::DRAM_POOL
        .get()
        .expect("DRAM_POOL not initialized — lo_info called before module init");

    let (live, draining, unused) = dram.segment_counts();
    let seg_size = crate::dram_segment_size();
    let capacity = live * seg_size;
    let allocated = dram.allocated_bytes();
    let util_pct = (allocated * 100).checked_div(capacity).unwrap_or(0) as i64;

    let mut section = ctx
        .builder()
        .add_section("dram")
        .field("dram_live_segments", live as i64)?
        .field("draining_segments", draining as i64)?
        .field("dram_unused_segments", unused as i64)?
        .field("allocated_bytes", allocated as i64)?
        .field("dram_fragment_count", dram.fragment_count() as i64)?
        .field("capacity_bytes", capacity as i64)?
        .field("utilization_pct", util_pct)?
        .field("cached_objects", dram.object_count() as i64)?;
    // Cache counters exist only in Tiered mode.
    if let Some(cache) = &dram.cache {
        section = section
            .field(
                "cache_hits",
                cache.stats.hits.load(Ordering::Relaxed) as i64,
            )?
            .field(
                "cache_misses",
                cache.stats.misses.load(Ordering::Relaxed) as i64,
            )?
            .field(
                "promotions",
                cache.stats.promotions.load(Ordering::Relaxed) as i64,
            )?
            .field(
                "admission_rejects",
                cache.admission.rejects.load(Ordering::Relaxed) as i64,
            )?
            .field(
                "demotions",
                cache.stats.demotions.load(Ordering::Relaxed) as i64,
            )?;
    }
    section
        .field(
            "scaling_expands",
            dram.expand_count.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .field(
            "scaling_shrinks",
            dram.shrink_count.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .field("dram_segment_size_bytes", seg_size as i64)?
        .field(
            "efa_registered_segments",
            crate::efa_registered_segment_count() as i64,
        )?
        .field(
            "dram_uring_registered_segments",
            dram.io_uring_registered_count() as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

fn nvme_staging_section(ctx: &InfoContext) -> ValkeyResult<()> {
    let Some(nvme) = storage::NVME_POOL.get() else {
        return Ok(());
    };

    let (live, _draining, unused) = nvme.segment_counts();

    ctx.builder()
        .add_section("nvme_staging")
        .field("nvme_live_segments", live as i64)?
        .field("nvme_unused_segments", unused as i64)?
        .field("nvme_fragment_count", nvme.fragment_count() as i64)?
        .field("staging_size_bytes", crate::nvme_staging_size() as i64)?
        .field("nvme_segment_size_bytes", crate::dram_segment_size() as i64)?
        .field(
            "nvme_uring_registered_segments",
            nvme.io_uring_registered_count() as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

/// NVMe SMART log health, Tiered mode only, aggregated across all
/// controllers. Emits two sections from one snapshot: `smartlog_usage`
/// (lifetime counters summed, percentages averaged over devices that
/// read successfully) and `smartlog_critical_warnings` (each spec bit set
/// if ANY device reports it).
fn smartlog_section(ctx: &InfoContext) -> ValkeyResult<()> {
    if operating_mode() != OperatingMode::Tiered {
        return Ok(());
    }
    let Some(snap) = snapshot_for_info() else {
        return Ok(());
    };

    // One pass over the devices. Sums stay u128 (the crate's counter
    // width) and saturate to u64 only at render time.
    let mut read_ok: u64 = 0;
    let mut read_failed: u64 = 0;
    let mut data_units_read: u128 = 0;
    let mut data_units_written: u128 = 0;
    let mut media_errors: u128 = 0;
    let mut unsafe_shutdowns: u128 = 0;
    let mut percent_used: u64 = 0;
    let mut avail_spare: u64 = 0;
    let mut critical_warning: u8 = 0;
    for d in &snap.devices {
        let Ok(h) = &d.health else {
            read_failed += 1;
            continue;
        };
        read_ok += 1;
        data_units_read = data_units_read.saturating_add(h.data_units_read);
        data_units_written = data_units_written.saturating_add(h.data_units_written);
        media_errors = media_errors.saturating_add(h.media_errors);
        unsafe_shutdowns = unsafe_shutdowns.saturating_add(h.unsafe_shutdowns);
        percent_used += u64::from(h.percent_used);
        avail_spare += u64::from(h.avail_spare);
        critical_warning |= h.critical_warning;
    }
    let to_u64 = |v: u128| u64::try_from(v).unwrap_or(u64::MAX);
    // With no device read the averages have no denominator; render 0 so
    // the field set is identical whether or not the read succeeded.
    let avg = |sum: u64| sum.checked_div(read_ok).unwrap_or(0);

    // Every field is emitted on every call so consumers can depend on a
    // fixed key set.
    let builder = ctx
        .builder()
        .add_section("smartlog_usage")
        .field("snapshot_age_seconds", snap.age().as_secs())?
        .field("devices", read_ok + read_failed)?
        .field("devices_read_failed", read_failed)?
        .field("data_units_read", to_u64(data_units_read))?
        .field("data_units_written", to_u64(data_units_written))?
        .field("percentage_used_avg", avg(percent_used))?
        .field("available_spare_pct_avg", avg(avail_spare))?
        .field("media_errors", to_u64(media_errors))?
        .field("unsafe_shutdowns", to_u64(unsafe_shutdowns))?
        .build_section()?;

    let mut warnings = builder.add_section("smartlog_critical_warnings");
    for (bit, label) in CRITICAL_WARNING_BITS {
        warnings = warnings.field(label, u64::from(critical_warning & bit != 0))?;
    }
    warnings.build_section()?.build_info().map(|_| ())
}

fn error_metrics_section(ctx: &InfoContext) -> ValkeyResult<()> {
    ctx.builder()
        .add_section("error_metrics")
        .field(
            "nvme_read_errors",
            NVME_READ_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "nvme_write_errors",
            NVME_WRITE_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_read_errors",
            EFA_READ_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "efa_write_errors",
            EFA_WRITE_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "dram_pool_exhausted",
            DRAM_POOL_EXHAUSTED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "nvme_buffer_exhausted",
            NVME_BUFFER_EXHAUSTED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "nvme_capacity_exceeded",
            NVME_CAPACITY_EXCEEDED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "set_finalize_stale",
            SET_FINALIZE_STALE.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "set_value_failures",
            SET_VALUE_FAILURES.load(Ordering::Relaxed) as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

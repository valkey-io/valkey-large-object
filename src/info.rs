//! INFO largeobj — pool and error statistics exposed via `INFO largeobj`.
//!
//! Add new subsections by adding a `fn *_section(ctx) -> ValkeyResult<()>` and
//! calling it from `info_sections`. Each section is a discrete group of fields.

use std::sync::atomic::{AtomicU64, Ordering};
use valkey_module::{InfoContext, ValkeyResult};

use crate::smartlog::{media_read_only, snapshot_for_info};
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

/// Main INFO handler, registered in `valkey_module!` as `info: lo_info`.
pub fn lo_info(ctx: &InfoContext, _for_crash_report: bool) {
    if let Err(e) = info_sections(ctx) {
        valkey_module::logging::log_warning(format!("lo_info: failed to emit INFO: {e}"));
    }
}

fn info_sections(ctx: &InfoContext) -> ValkeyResult<()> {
    dram_pool_section(ctx)?;
    nvme_staging_section(ctx)?;
    smartlog_section(ctx)?;
    error_metrics_section(ctx)?;
    Ok(())
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

    ctx.builder()
        .add_section("largeobj_dram")
        .field("live_segments", live as i64)?
        .field("draining_segments", draining as i64)?
        .field("unused_segments", unused as i64)?
        .field("allocated_bytes", allocated as i64)?
        .field("capacity_bytes", capacity as i64)?
        .field("utilization_pct", util_pct)?
        .field("cached_objects", dram.object_count() as i64)?
        .field(
            "scaling_expand_total",
            dram.expand_count.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .field(
            "scaling_shrink_total",
            dram.shrink_count.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .field("maxmemory_bytes", crate::dram_maxmemory() as i64)?
        .field("segment_size_bytes", seg_size as i64)?
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
        .add_section("largeobj_nvme_staging")
        .field("live_segments", live as i64)?
        .field("unused_segments", unused as i64)?
        .field("staging_size_bytes", crate::nvme_staging_size() as i64)?
        .field("segment_size_bytes", crate::dram_segment_size() as i64)?
        .build_section()?
        .build_info()
        .map(|_| ())
}

/// NVMe SMART log health, Tiered mode only. The section appears once the
/// poller's first read lands; before that INFO simply omits it.
fn smartlog_section(ctx: &InfoContext) -> ValkeyResult<()> {
    if operating_mode() != OperatingMode::Tiered {
        return Ok(());
    }
    let Some(snap) = snapshot_for_info() else {
        return Ok(());
    };

    let mut section = ctx
        .builder()
        .add_section("smartlog")
        .field("snapshot_age_seconds", snap.age().as_secs())?;
    for d in &snap.devices {
        // /dev/nvme0 -> nvme0 field prefix
        let name = d.device.rsplit('/').next().unwrap_or(&d.device);
        section = match &d.health {
            Err(e) => section.field(
                &format!("{name}_read_error"),
                e.raw_os_error()
                    .map_or_else(|| e.to_string(), |errno| format!("errno {errno}")),
            )?,
            // Usage first, then warnings/errors.
            Ok(h) => section
                .field(
                    &format!("{name}_data_units_read"),
                    u64::try_from(h.data_units_read).unwrap_or(u64::MAX),
                )?
                .field(
                    &format!("{name}_data_units_written"),
                    u64::try_from(h.data_units_written).unwrap_or(u64::MAX),
                )?
                .field(
                    &format!("{name}_percentage_used"),
                    u64::from(h.percent_used),
                )?
                .field(
                    &format!("{name}_available_spare_pct"),
                    u64::from(h.avail_spare),
                )?
                .field(
                    &format!("{name}_temperature_kelvin"),
                    u64::from(h.temperature),
                )?
                .field(
                    &format!("{name}_critical_warning"),
                    u64::from(h.critical_warning),
                )?
                .field(
                    &format!("{name}_media_read_only"),
                    u64::from(media_read_only(h.critical_warning)),
                )?
                .field(
                    &format!("{name}_media_errors"),
                    u64::try_from(h.media_errors).unwrap_or(u64::MAX),
                )?
                .field(
                    &format!("{name}_unsafe_shutdowns"),
                    u64::try_from(h.unsafe_shutdowns).unwrap_or(u64::MAX),
                )?,
        };
    }
    section.build_section()?.build_info().map(|_| ())
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

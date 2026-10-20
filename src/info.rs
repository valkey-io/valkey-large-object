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

pub static DISK_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static DISK_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static RDMA_READ_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static RDMA_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static DRAM_POOL_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static DISK_STAGING_BUFFER_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static DISK_CAPACITY_EXCEEDED: AtomicU64 = AtomicU64::new(0);
pub static SET_FINALIZE_STALE: AtomicU64 = AtomicU64::new(0);
pub static SET_VALUE_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static RDMA_DISCARDED_TRANSFERS: AtomicU64 = AtomicU64::new(0);
pub static CRC_MISMATCH: AtomicU64 = AtomicU64::new(0);

// ─── Core Metrics ────────────────────────────────────────────────────────────

/// Live `LoValue` instances: +1 in `LoValue::new`, −1 in its `Drop`. Equals the
/// LargeObject keys in the keyspace, plus a SET's value briefly before commit.
pub static LARGE_OBJECT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Objects evicted to make room for a write, or by a Dram shrink: Dram objects or
/// Tiered files. Cached copies Tiered drops are demotions, not evictions.
pub static EVICTIONS: AtomicU64 = AtomicU64::new(0);

/// RDMA payload bytes of successful transfers. Reads pull client memory (the SET
/// path); writes push into it (the GET path).
pub static RDMA_READ_BYTES: AtomicU64 = AtomicU64::new(0);
pub static RDMA_WRITE_BYTES: AtomicU64 = AtomicU64::new(0);

/// `part` as a percentage of `whole`, to two decimals ("99.99").
fn pct(part: u64, whole: u64) -> String {
    // No budget, e.g. disk-maxmemory 0 (unlimited).
    if whole == 0 {
        return "0.00".to_string();
    }
    format!("{:.2}", part as f64 * 100.0 / whole as f64)
}

/// Main INFO handler, registered in `valkey_module!` as `info: lo_info`.
pub fn lo_info(ctx: &InfoContext, _for_crash_report: bool) {
    // Hide INFO while disabled, unless objects remain.
    if !crate::module_enabled() && LARGE_OBJECT_COUNT.load(Ordering::Relaxed) == 0 {
        return;
    }
    if let Err(e) = info_sections(ctx) {
        valkey_module::logging::log_warning(format!("lo_info: failed to emit INFO: {e}"));
    }
}

fn info_sections(ctx: &InfoContext) -> ValkeyResult<()> {
    core_metrics_section(ctx)?;
    dram_pool_section(ctx)?;
    disk_staging_section(ctx)?;
    disk_section(ctx)?;
    fd_section(ctx)?;
    smartlog_section(ctx)?;
    rdma_section(ctx)?;
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
            "pending_evictions",
            storage::reclaim::RECLAIM_LIST.len() as i64,
        )?
        .field("evictions", EVICTIONS.load(Ordering::Relaxed) as i64)?
        .field("disk_used_bytes", storage::nvme::nvme_disk_usage() as i64)?
        .field(
            "disk_orphaned_files",
            storage::nvme::ORPHANED_FILES.count.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "disk_orphaned_bytes",
            storage::nvme::ORPHANED_FILES.bytes.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "disk_unevictable_objects",
            storage::object_file::unevictable_count() as i64,
        )?
        .field(
            "inflight_requests",
            crate::engine::inflight_requests() as i64,
        )?
        .build_section()?
        .build_info()?;
    Ok(())
}

/// Tiered only: absent in Dram mode, where no FdPool exists. Object-file fd placement
/// first, then the fd cache.
fn fd_section(ctx: &InfoContext) -> ValkeyResult<()> {
    let Some(fds) = storage::FD_POOL.get() else {
        return Ok(());
    };
    ctx.builder()
        .add_section("fd")
        // Placement. open_object_fds counts cached fds and those held by running GETs
        // and SETs.
        .field(
            "open_object_fds",
            storage::fd_placement::open_object_fds() as i64,
        )?
        .field(
            "fd_placement_enabled",
            storage::fd_placement::placement_enabled() as i64,
        )?
        .field(
            "fd_placement_failures",
            storage::fd_placement::placement_failures() as i64,
        )?
        // Cache: the fds kept open between GETs.
        .field(
            "fd_cache_max_entries",
            storage::fd_placement::fd_cache_max_entries() as i64,
        )?
        .field("fd_cache_entries", fds.len() as i64)?
        .field(
            "fd_cache_evictions",
            fds.evictions.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

fn dram_pool_section(ctx: &InfoContext) -> ValkeyResult<()> {
    let dram = storage::DRAM_POOL
        .get()
        .expect("DRAM_POOL not initialized — lo_info called before module init");

    let (total, draining, unused) = dram.segment_counts();
    let seg_size = crate::dram_segment_size();
    let capacity = (total - draining) * seg_size;
    let allocated = dram.allocated_bytes();
    let util_pct = pct(allocated as u64, capacity as u64);

    let mut section = ctx
        .builder()
        .add_section("dram")
        .field("dram_segments", total as i64)?
        .field("dram_draining_segments", draining as i64)?
        .field("dram_unused_segments", unused as i64)?
        .field("dram_allocated_bytes", allocated as i64)?
        .field("dram_fragment_count", dram.fragment_count() as i64)?
        .field("dram_capacity_bytes", capacity as i64)?
        .field("dram_utilization_pct", util_pct)?
        .field("dram_objects", dram.object_count() as i64)?;
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
                "cache_promotions",
                cache.stats.promotions.load(Ordering::Relaxed) as i64,
            )?
            .field(
                "cache_admission_rejects",
                cache.admission.rejects.load(Ordering::Relaxed) as i64,
            )?
            .field(
                "cache_demotions",
                cache.stats.demotions.load(Ordering::Relaxed) as i64,
            )?;
    }
    section
        .field(
            "dram_scaling_expands",
            dram.expand_count.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .field(
            "dram_scaling_shrinks",
            dram.shrink_count.load(std::sync::atomic::Ordering::Relaxed) as i64,
        )?
        .field("dram_segment_size_bytes", seg_size as i64)?
        .field(
            "rdma_registered_segments",
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

fn disk_staging_section(ctx: &InfoContext) -> ValkeyResult<()> {
    let Some(nvme) = storage::NVME_POOL.get() else {
        return Ok(());
    };

    let (total, _draining, unused) = nvme.segment_counts();

    ctx.builder()
        .add_section("disk_staging")
        .field("disk_staging_segments", total as i64)?
        .field("disk_staging_unused_segments", unused as i64)?
        .field("disk_staging_fragment_count", nvme.fragment_count() as i64)?
        .field("disk_staging_size_bytes", crate::nvme_staging_size() as i64)?
        .field(
            "disk_staging_segment_size_bytes",
            crate::dram_segment_size() as i64,
        )?
        .field(
            "disk_staging_uring_registered_segments",
            nvme.io_uring_registered_count() as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

/// Object files on disk, Tiered mode only: how full the disk budget is.
fn disk_section(ctx: &InfoContext) -> ValkeyResult<()> {
    if operating_mode() != OperatingMode::Tiered {
        return Ok(());
    }
    let util_pct = pct(storage::nvme::nvme_disk_usage(), crate::nvme_maxmemory());

    ctx.builder()
        .add_section("disk")
        .field("disk_utilization_pct", util_pct)?
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

/// RDMA sessions and payload bytes, in both modes. Emitted as zeros where no fabric
/// is available, so consumers can depend on a fixed key set.
fn rdma_section(ctx: &InfoContext) -> ValkeyResult<()> {
    ctx.builder()
        .add_section("rdma")
        .field("rdma_sessions", crate::transport::session::count() as u64)?
        .field("rdma_read_bytes", RDMA_READ_BYTES.load(Ordering::Relaxed))?
        .field("rdma_write_bytes", RDMA_WRITE_BYTES.load(Ordering::Relaxed))?
        .build_section()?
        .build_info()
        .map(|_| ())
}

fn error_metrics_section(ctx: &InfoContext) -> ValkeyResult<()> {
    ctx.builder()
        .add_section("error_metrics")
        .field(
            "disk_read_errors",
            DISK_READ_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "disk_write_errors",
            DISK_WRITE_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "rdma_read_errors",
            RDMA_READ_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "rdma_write_errors",
            RDMA_WRITE_ERRORS.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "dram_pool_exhausted",
            DRAM_POOL_EXHAUSTED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "disk_staging_buffer_exhausted",
            DISK_STAGING_BUFFER_EXHAUSTED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "disk_capacity_exceeded",
            DISK_CAPACITY_EXCEEDED.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "set_finalize_stale",
            SET_FINALIZE_STALE.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "set_value_failures",
            SET_VALUE_FAILURES.load(Ordering::Relaxed) as i64,
        )?
        .field(
            "rdma_discarded_transfers",
            RDMA_DISCARDED_TRANSFERS.load(Ordering::Relaxed) as i64,
        )?
        .field("crc_mismatch", CRC_MISMATCH.load(Ordering::Relaxed) as i64)?
        .build_section()?
        .build_info()
        .map(|_| ())
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pct_has_two_decimals() {
        assert_eq!(pct(5, 0), "0.00");
        assert_eq!(pct(2, 3), "66.67");
        assert_eq!(pct(1, 8), "12.50");
        assert_eq!(pct(4096, 4096), "100.00");
    }
}

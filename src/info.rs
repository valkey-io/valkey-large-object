//! INFO largeobj — pool, I/O and error statistics exposed via `INFO largeobj`.
//!
//! Add new subsections by adding a `fn *_section(ctx) -> ValkeyResult<()>` and
//! calling it from `info_sections`. Each section is a discrete group of fields.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
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

// ─── I/O Metrics ─────────────────────────────────────────────────────────────

/// Count and total duration of one kind of I/O. `usec_total / count` is the mean.
#[derive(Default)]
pub struct IoStats {
    count: AtomicU64,
    usec: AtomicU64,
}

impl IoStats {
    pub const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            usec: AtomicU64::new(0),
        }
    }

    /// Record one I/O that started at `started` and has just completed.
    pub fn record(&self, started: Instant) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.usec
            .fetch_add(nearest_usec(started.elapsed()), Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn usec_total(&self) -> u64 {
        self.usec.load(Ordering::Relaxed)
    }
}

/// Rounded rather than truncated, so sub-microsecond I/Os don't bias the mean
/// low. Summing whole microseconds rather than nanoseconds keeps the total from
/// wrapping even with thousands of transfers in flight for years.
fn nearest_usec(elapsed: Duration) -> u64 {
    u64::try_from((elapsed.as_nanos() + 500) / 1_000).unwrap_or(u64::MAX)
}

/// One direction of EFA traffic: successful transfers and their payload bytes.
#[derive(Default)]
pub struct EfaStats {
    transfers: IoStats,
    bytes: AtomicU64,
}

impl EfaStats {
    pub const fn new() -> Self {
        Self {
            transfers: IoStats::new(),
            bytes: AtomicU64::new(0),
        }
    }

    /// Record one successful transfer of `len` bytes that started at `started`.
    pub fn record(&self, started: Instant, len: u64) {
        self.transfers.record(started);
        self.bytes.fetch_add(len, Ordering::Relaxed);
    }
}

/// io_uring reads and writes of NVMe object files on either ring, timed from
/// SQE push to CQE reap. Every reaped CQE counts, including failed and short I/Os.
pub static NVME_READS: IoStats = IoStats::new();
pub static NVME_WRITES: IoStats = IoStats::new();

/// Successful EFA transfers, timed from submission to completion, one per client
/// address of a chunk. Reads pull client memory (the SET path); writes push into
/// it (the GET path). The time includes any wait behind `fabric-max-in-flight`.
pub static EFA_READS: EfaStats = EfaStats::new();
pub static EFA_WRITES: EfaStats = EfaStats::new();

/// `part` as a percentage of `whole` to two decimals ("99.99"), or "0.00" when
/// `whole` is 0. The same `%.2f` Valkey uses for `expired_stale_perc` and
/// `current_fork_perc`, without the `%` some of its memory fields append.
fn pct(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "0.00".to_string();
    }
    format!("{:.2}", part as f64 * 100.0 / whole as f64)
}

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
    nvme_section(ctx)?;
    fd_pool_section(ctx)?;
    smartlog_section(ctx)?;
    efa_section(ctx)?;
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

    let (total, _draining, unused) = nvme.segment_counts();
    // Against the pool as built (whole segments), which can exceed
    // nvme-staging-size when that isn't a multiple of segment-size.
    let capacity = total * crate::dram_segment_size();
    let util_pct = pct(nvme.allocated_bytes() as u64, capacity as u64);

    ctx.builder()
        .add_section("nvme_staging")
        .field("nvme_segments", total as i64)?
        .field("nvme_unused_segments", unused as i64)?
        .field("nvme_fragment_count", nvme.fragment_count() as i64)?
        .field("nvme_staging_size_bytes", crate::nvme_staging_size() as i64)?
        .field("staging_utilization_pct", util_pct)?
        .field("nvme_segment_size_bytes", crate::dram_segment_size() as i64)?
        .field(
            "nvme_uring_registered_segments",
            nvme.io_uring_registered_count() as i64,
        )?
        .build_section()?
        .build_info()
        .map(|_| ())
}

/// NVMe object files, Tiered mode only: disk budget, object count and io_uring
/// I/O timing.
fn nvme_section(ctx: &InfoContext) -> ValkeyResult<()> {
    if operating_mode() != OperatingMode::Tiered {
        return Ok(());
    }
    let used = storage::nvme::nvme_disk_usage();
    // nvme-maxmemory 0 means unlimited: no budget to be a percentage of.
    let util_pct = pct(used, crate::nvme_maxmemory());

    ctx.builder()
        .add_section("nvme")
        .field("nvme_disk_used_bytes", used)?
        .field("nvme_disk_utilization_pct", util_pct)?
        .field("live_objects", crate::data_type::live_objects())?
        .field("nvme_reads_total", NVME_READS.count())?
        .field("nvme_read_usec_total", NVME_READS.usec_total())?
        .field("nvme_writes_total", NVME_WRITES.count())?
        .field("nvme_write_usec_total", NVME_WRITES.usec_total())?
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

/// EFA sessions and traffic, in both modes. Emitted as zeros where no fabric
/// is available, so consumers can depend on a fixed key set.
fn efa_section(ctx: &InfoContext) -> ValkeyResult<()> {
    ctx.builder()
        .add_section("efa")
        .field("efa_sessions", crate::transport::session::count() as u64)?
        .field(
            "efa_read_bytes_total",
            EFA_READS.bytes.load(Ordering::Relaxed),
        )?
        .field(
            "efa_write_bytes_total",
            EFA_WRITES.bytes.load(Ordering::Relaxed),
        )?
        .field("efa_reads_total", EFA_READS.transfers.count())?
        .field("efa_read_usec_total", EFA_READS.transfers.usec_total())?
        .field("efa_writes_total", EFA_WRITES.transfers.count())?
        .field("efa_write_usec_total", EFA_WRITES.transfers.usec_total())?
        .build_section()?
        .build_info()
        .map(|_| ())
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

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pct_has_two_decimals() {
        assert_eq!(pct(0, 0), "0.00");
        assert_eq!(pct(5, 0), "0.00");
        assert_eq!(pct(0, 4096), "0.00");
        assert_eq!(pct(1, 3), "33.33");
        assert_eq!(pct(2, 3), "66.67");
        assert_eq!(pct(1, 7), "14.29");
        assert_eq!(pct(1, 8), "12.50");
        assert_eq!(pct(9_999, 10_000), "99.99");
        assert_eq!(pct(4096, 4096), "100.00");
        assert_eq!(pct(1, 1_000_000), "0.00");
        assert_eq!(pct(u64::MAX, u64::MAX), "100.00");
    }

    #[test]
    fn test_io_stats_round_each_io_to_the_nearest_usec() {
        assert_eq!(nearest_usec(Duration::from_nanos(0)), 0);
        assert_eq!(nearest_usec(Duration::from_nanos(499)), 0);
        assert_eq!(nearest_usec(Duration::from_nanos(500)), 1);
        assert_eq!(nearest_usec(Duration::from_nanos(1_499)), 1);
        assert_eq!(nearest_usec(Duration::from_micros(250)), 250);
        assert_eq!(nearest_usec(Duration::MAX), u64::MAX);
        let stats = IoStats::new();
        assert_eq!((stats.count(), stats.usec_total()), (0, 0));
        stats.record(Instant::now());
        assert_eq!(stats.count(), 1);
    }

    #[test]
    fn test_efa_stats_record_count_and_bytes() {
        let efa = EfaStats::new();
        efa.record(Instant::now(), 4096);
        efa.record(Instant::now(), 1024);
        assert_eq!(efa.transfers.count(), 2);
        assert_eq!(efa.bytes.load(Ordering::Relaxed), 5120);
    }
}

//! Object-file fd placement — keep the module's fds out of the fd numbers Valkey
//! core serves clients on.
//!
//! Valkey core's event loop is an array indexed by fd number, sized
//! `maxclients + CONFIG_FDSET_INCR` (128). Those fd numbers are the core range:
//! core's own fds and client sockets live there, and a client socket numbered past
//! it is never served. Linux hands out the lowest free number, so object fds left
//! inside the core range (the fd cache, command fds) push client sockets past its
//! end. Cache misses and SET write files are opened through `open_object_file`,
//! which moves the fd into the module range: the fd numbers from the module start
//! (`maxclients + 128`) up to the fd limit. COPY opens its two files directly: it
//! runs on the main thread and closes them before returning to the event loop, so
//! no client connects while they are open. `raise_module_start` sets the module
//! start at load and on every `CONFIG SET maxclients`.
//!
//! When the fd limit ends inside the core range (Valkey's stock limit is
//! `maxclients + 32`), the module range is empty: no client can be stranded, and
//! placement is off.
//!
//! The fd cache never takes the numbers commands need. Its size is `max-cached-fds`
//! limited to what the module range holds after a command reserve of `maxclients`
//! numbers (`fd_cache_max_entries`). A shortfall is logged, never fatal: a command that finds
//! no free fd in the module range fails with an error, and the server keeps running.

use std::ffi::{c_int, c_void, CStr};
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use valkey_module::{raw, Context};

use crate::{fd_reserve, FdReserve};

/// Valkey's event loop holds `maxclients + CONFIG_FDSET_INCR` fds (server.h: 32 + 96).
const VALKEY_FDSET_INCR: u64 = 128;

/// The module start for the highest `maxclients` seen: the first fd number of the
/// module range, and the lowest number object files are moved to.
static MODULE_START: AtomicI32 = AtomicI32::new(0);

/// Whether placement is on: object fds are moved to the module range. Off when not
/// Tiered, while loading, and when the fd limit ends inside the core range.
static PLACEMENT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether `check_module_start_past_event_loop` has passed. The event loop is always
/// `maxclients + CONFIG_FDSET_INCR` wide, so one check at load covers every later module start.
static PROBED: AtomicBool = AtomicBool::new(false);

/// Object files currently open: the fd cache plus the fds of running GETs and SETs.
static OPEN_OBJECT_FDS: AtomicU64 = AtomicU64::new(0);

/// Object-file opens that found no free fd in the module range.
static PLACEMENT_FAILURES: AtomicU64 = AtomicU64::new(0);

/// How many object-file opens found no free fd in the module range.
pub fn placement_failures() -> u64 {
    PLACEMENT_FAILURES.load(Ordering::Relaxed)
}

/// Whether placement is on.
pub fn placement_enabled() -> bool {
    PLACEMENT_ENABLED.load(Ordering::Relaxed)
}

/// How many object files are open now: cached fds, plus the fds of running GETs
/// and SETs, including a GET's fd after the cache evicted it.
pub fn open_object_fds() -> u64 {
    OPEN_OBJECT_FDS.load(Ordering::Relaxed)
}

/// An open object file. It closes on drop and counts itself in `open_object_fds`
/// while open.
pub struct ObjectFd(OwnedFd);

impl AsRawFd for ObjectFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Drop for ObjectFd {
    fn drop(&mut self) {
        OPEN_OBJECT_FDS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Raise the module start to the end of the core range for the current `maxclients`,
/// and turn placement on or off. Tiered only, main thread only.
///
/// The fd limit covers the command reserve when `limit >= module start + maxclients`.
/// If it does not: `fd-reserve Lenient` turns placement off (and with it the cache),
/// logs, and returns Ok; `fd-reserve Strict` returns Err naming the fix. Other Errs:
/// the module start is not actually past the event loop (object fds would then take
/// fd numbers in the core range, and clients past them are never served), or the fd
/// limit cannot be raised.
pub fn raise_module_start(ctx: &Context) -> Result<(), String> {
    let maxclients: u64 = ctx
        .config_get("maxclients".to_string())
        .expect("CONFIG GET maxclients")
        .to_string()
        .parse()
        .expect("maxclients is numeric");
    let module_start = i32::try_from(maxclients + VALKEY_FDSET_INCR)
        .map_err(|_| format!("maxclients {maxclients} is too large for an fd number"))?;
    let before = nofile_limit().rlim_cur;
    raise_soft_fd_limit()?;
    let limit = nofile_limit().rlim_cur;
    if limit > before {
        ctx.log_notice(&format!(
            "largeobj: increased the open-file soft limit to the hard limit {limit} \
             (it was {before})"
        ));
    }
    // Only ever up: Valkey grows its event loop when maxclients grows, but never
    // shrinks it.
    let module_start = MODULE_START
        .fetch_max(module_start, Ordering::Relaxed)
        .max(module_start);
    let maxclients = module_start as u64 - VALKEY_FDSET_INCR;
    let needed = module_start as u64 + maxclients;
    if limit < needed {
        let shortfall = format!(
            "open-file limit {limit} is below {needed} (module start {module_start} + \
             maxclients {maxclients}), so the module range cannot hold the command reserve"
        );
        if fd_reserve() == FdReserve::Strict {
            PLACEMENT_ENABLED.store(false, Ordering::Relaxed);
            return Err(format!(
                "fd-reserve Strict: {shortfall}; set LimitNOFILE to at least {needed}, lower \
                 maxclients, or use fd-reserve Lenient"
            ));
        }
        PLACEMENT_ENABLED.store(false, Ordering::Relaxed);
        ctx.log_notice(&format!(
            "largeobj: {shortfall}; object-file fd placement is off (fd-reserve Lenient)"
        ));
        warn_if_cache_limited(crate::max_cached_fds() as u64);
        return Ok(());
    }
    PLACEMENT_ENABLED.store(true, Ordering::Relaxed);
    if !PROBED.load(Ordering::Relaxed) {
        check_module_start_past_event_loop(module_start)?;
        PROBED.store(true, Ordering::Relaxed);
    }
    ctx.log_notice(&format!(
        "largeobj: object-file fd placement is on: module range starts at fd {module_start}"
    ));
    if fd_reserve() == FdReserve::Strict {
        ctx.log_notice(&format!(
            "largeobj: fd-reserve Strict: a CONFIG SET maxclients above {} aborts the server \
             (the command reserve would no longer fit in open-file limit {limit})",
            limit.saturating_sub(VALKEY_FDSET_INCR) / 2
        ));
    }
    warn_if_cache_limited(crate::max_cached_fds() as u64);
    Ok(())
}

/// How many cached read fds the fd limit lets the module range hold once the command
/// reserve (one number per client) is set aside for command fds (a SET's write fd, or
/// a GET's fd evicted mid-read; both block their client, so a client holds at most
/// one). Zero when placement is off: a cached fd would then hold a client's fd number
/// for good.
fn cache_headroom() -> u64 {
    if !PLACEMENT_ENABLED.load(Ordering::Relaxed) {
        return 0;
    }
    cache_headroom_for(
        nofile_limit().rlim_cur,
        MODULE_START.load(Ordering::Relaxed),
    )
}

fn cache_headroom_for(limit: u64, module_start: i32) -> u64 {
    let module_start = module_start as u64;
    // The module_start tracks the highest maxclients seen; Valkey keeps clients above a
    // lowered maxclients connected, so that is the client bound.
    let maxclients = module_start.saturating_sub(VALKEY_FDSET_INCR);
    limit
        .saturating_sub(module_start)
        .saturating_sub(maxclients)
}

/// The fd cache max entries: the most fds the fd cache may hold. `max-cached-fds`,
/// limited to what the fd limit allows.
pub fn fd_cache_max_entries() -> usize {
    (crate::max_cached_fds() as u64).min(cache_headroom()) as usize
}

/// Log, when `max_cached_fds` fds cannot all be cached, how many can and what limit
/// would fit.
pub fn warn_if_cache_limited(max_cached_fds: u64) {
    let headroom = cache_headroom();
    if max_cached_fds == 0 || max_cached_fds <= headroom {
        return;
    }
    let limit = nofile_limit().rlim_cur;
    let module_start = MODULE_START.load(Ordering::Relaxed) as u64;
    let maxclients = module_start.saturating_sub(VALKEY_FDSET_INCR);
    let required = module_start + max_cached_fds + maxclients;
    let msg = if PLACEMENT_ENABLED.load(Ordering::Relaxed) {
        format!(
            "largeobj: open-file limit {limit} is too low for max-cached-fds {max_cached_fds} and \
             maxclients {maxclients}; the fd cache holds at most {headroom}. Raise the limit to at \
             least {required}"
        )
    } else {
        format!(
            "largeobj: open-file limit {limit} does not cover the command reserve, so the \
             fd cache is off (max-cached-fds {max_cached_fds}). Raise the limit to at least {required}"
        )
    };
    valkey_module::logging::log_warning(msg);
}

/// The process's `RLIMIT_NOFILE`.
fn nofile_limit() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit is a valid rlimit out-parameter.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    // getrlimit fails only on a bad pointer or resource (EFAULT, EINVAL).
    assert_eq!(
        rc,
        0,
        "getrlimit(RLIMIT_NOFILE): {}",
        std::io::Error::last_os_error()
    );
    limit
}

/// Raise the soft `RLIMIT_NOFILE` to the hard limit, so object fds can use every
/// fd number the hard limit allows.
fn raise_soft_fd_limit() -> Result<(), String> {
    let mut limit = nofile_limit();
    limit.rlim_cur = limit.rlim_max;
    // SAFETY: limit is a valid rlimit; raising soft up to hard needs no privilege.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(format!(
            "cannot raise the open-file soft limit to the hard limit {}: {}",
            limit.rlim_max,
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// One-shot check that `module_start` is past the end of Valkey's event loop, so the
/// copied `maxclients + CONFIG_FDSET_INCR` really marks the end of the Valkey core
/// range. `EventLoopAdd` answers ERANGE exactly when `fd >= setsize`. The probe is
/// an eventfd we own (epoll rejects regular files), placed at `module_start`, so no
/// client's event is touched; if it registers, it is removed at once. Main thread
/// only.
fn check_module_start_past_event_loop(module_start: i32) -> Result<(), String> {
    unsafe extern "C" fn noop(_fd: c_int, _data: *mut c_void, _mask: c_int) {}
    // Reads errno: call it right after the failing call, before the fds drop.
    let probe_err = |step: &str| {
        format!(
            "event-loop probe at fd {module_start}: {step}: {}",
            std::io::Error::last_os_error()
        )
    };
    // SAFETY: eventfd returns a new fd or -1.
    let low = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    if low < 0 {
        return Err(probe_err("eventfd"));
    }
    // SAFETY: low is a new fd we own.
    let low = unsafe { OwnedFd::from_raw_fd(low) };
    // SAFETY: low is valid; returns a new fd >= module_start or -1.
    let probe = unsafe { libc::fcntl(low.as_raw_fd(), libc::F_DUPFD_CLOEXEC, module_start) };
    if probe < 0 {
        return Err(probe_err("F_DUPFD_CLOEXEC"));
    }
    // SAFETY: probe is a new fd we own.
    let probe = unsafe { OwnedFd::from_raw_fd(probe) };
    let readable = raw::REDISMODULE_EVENTLOOP_READABLE as c_int;
    // SAFETY: the module API is initialized (OnLoad or a server event on the main
    // thread); probe is our own eventfd.
    let rc = unsafe {
        raw::RedisModule_EventLoopAdd.expect("EventLoopAdd API available")(
            probe.as_raw_fd(),
            readable,
            Some(noop),
            std::ptr::null_mut(),
        )
    };
    if rc == raw::REDISMODULE_OK as c_int {
        // SAFETY: probe was just registered by us, and is still open.
        unsafe {
            raw::RedisModule_EventLoopDel.expect("EventLoopDel API available")(
                probe.as_raw_fd(),
                readable,
            )
        };
        return Err(format!(
            "Valkey's event loop is larger than maxclients + {VALKEY_FDSET_INCR}: \
             fd {module_start} is inside it"
        ));
    }
    if std::io::Error::last_os_error().raw_os_error() != Some(libc::ERANGE) {
        return Err(probe_err("EventLoopAdd"));
    }
    Ok(())
}

/// Whether `fd` is below the module start, inside the core range. Always false when
/// placement is off.
pub fn below_module_start(fd: RawFd) -> bool {
    PLACEMENT_ENABLED.load(Ordering::Relaxed) && fd < MODULE_START.load(Ordering::Relaxed)
}

/// `open(2)` an object file, then move the fd in the module range set by
/// `raise_module_start`. When no fd is free in the module range it closes the low fd
/// and fails: keeping it would take an fd number in the core range. The
/// caller turns the failure into an error reply.
pub fn open_object_file(
    path: &CStr,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> std::io::Result<ObjectFd> {
    // SAFETY: path is NUL-terminated; open returns an fd or -1.
    let mut fd = unsafe { libc::open(path.as_ptr(), flags, mode as libc::c_uint) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if PLACEMENT_ENABLED.load(Ordering::Relaxed) {
        fd = move_above(fd, MODULE_START.load(Ordering::Relaxed))?;
    }
    OPEN_OBJECT_FDS.fetch_add(1, Ordering::Relaxed);
    // SAFETY: fd is a newly opened file descriptor owned exclusively by us.
    Ok(ObjectFd(unsafe { OwnedFd::from_raw_fd(fd) }))
}

/// Move `fd` to the lowest free number at or above `module_start` and close the original.
/// On failure closes `fd` too.
fn move_above(fd: RawFd, module_start: i32) -> std::io::Result<RawFd> {
    // SAFETY: fd is a valid fd we own; F_DUPFD_CLOEXEC returns a new fd >= module_start or -1.
    let high = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, module_start) };
    let moved = if high < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(high)
    };
    // SAFETY: fd is ours and no longer used; on success high refers to the same open file.
    unsafe { libc::close(fd) };
    if let Err(err) = &moved {
        note_failure(module_start, err);
    }
    moved
}

/// Count a failed move and log the first of each burst of 1024.
fn note_failure(module_start: i32, err: &std::io::Error) {
    let count = PLACEMENT_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if count % 1024 == 1 {
        valkey_module::logging::log_warning(format!(
            "largeobj: no free fd at or above {module_start} for an object file: {err}; \
             {count} failed so far"
        ));
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn open_dev_null() -> RawFd {
        let path = std::ffi::CString::new("/dev/null").unwrap();
        // SAFETY: path is NUL-terminated.
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        assert!(fd >= 0, "open /dev/null");
        fd
    }

    #[test]
    fn move_above_fails_without_panicking_when_no_fd_is_free() {
        // F_DUPFD rejects the module start at or above the limit, which is what a full range does.
        let failures = placement_failures();
        let err = move_above(open_dev_null(), i32::MAX).expect_err("no fd at i32::MAX");
        assert!(
            matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EMFILE)),
            "{err}"
        );
        assert_eq!(placement_failures(), failures + 1);
    }

    #[test]
    fn object_fd_counts_itself_while_open() {
        let path = std::ffi::CString::new("/dev/null").unwrap();
        let before = open_object_fds();
        let fd = open_object_file(&path, libc::O_RDONLY, 0).expect("open /dev/null");
        assert_eq!(open_object_fds(), before + 1);
        drop(fd);
        assert_eq!(open_object_fds(), before);
    }

    #[test]
    fn cache_headroom_reserves_maxclients_for_commands() {
        let (module_start, maxclients) = (10_128, 10_000);
        // No numbers in the module range, or fewer than one per client: no room.
        assert_eq!(cache_headroom_for(10_032, module_start), 0);
        assert_eq!(cache_headroom_for(10_240, module_start), 0);
        assert_eq!(
            cache_headroom_for(module_start as u64 + maxclients, module_start),
            0
        );
        // Each number past that is one cached fd.
        assert_eq!(
            cache_headroom_for(module_start as u64 + maxclients + 1, module_start),
            1
        );
        assert_eq!(cache_headroom_for(65_536, module_start), 45_408);
    }
}

//! Object-file fd placement — keep the module's long-lived fds out of the fd
//! numbers Valkey serves clients on.
//!
//! Valkey's event loop is an array indexed by fd number, sized
//! `maxclients + CONFIG_FDSET_INCR` (128). Those fd numbers are the Valkey core
//! range: core's own fds and client sockets live there, and a client socket
//! numbered past it is never served. Linux hands out the lowest free number, so
//! object fds left inside the range (the fd cache, in-flight SET files) push
//! client sockets past its end. Cache misses and SET write files are opened
//! through `open_object_file`, which moves the fd above a floor at the end of the
//! range. COPY opens its two files directly: it runs on the main thread and closes
//! them before returning to the event loop, so no client connects while they are
//! open. `raise_object_fd_floor` sets the floor at load and on every
//! `CONFIG SET maxclients`.
//!
//! When the open-file limit ends inside the Valkey core range (Valkey's stock
//! limit is `maxclients + 32`), no fd number past the range exists, so no client
//! can be stranded and fds are not moved.
//!
//! The fd cache never takes the slots commands need: it holds at most the fds that
//! fit above the floor after one slot per client (`effective_cap`). A shortfall is
//! logged, never fatal: a command that finds no free fd above the floor fails with
//! an error, and the server keeps running.

use std::ffi::{c_int, c_void, CStr};
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use valkey_module::{raw, Context};

/// Valkey's event loop holds `maxclients + CONFIG_FDSET_INCR` fds (server.h: 32 + 96).
const VALKEY_FDSET_INCR: u64 = 128;

/// End of the Valkey core range for the highest `maxclients` seen: the lowest fd
/// number object files are moved to.
static FD_FLOOR: AtomicI32 = AtomicI32::new(0);

/// Whether object fds are moved above `FD_FLOOR`. Off when not Tiered, while
/// loading, and when the open-file limit ends inside the Valkey core range.
static MOVE_ABOVE_FLOOR: AtomicBool = AtomicBool::new(false);

/// Whether `check_floor_past_event_loop` has passed. The event loop is always
/// `maxclients + CONFIG_FDSET_INCR` wide, so one check at load covers every later floor.
static PROBED: AtomicBool = AtomicBool::new(false);

/// Object-file opens that found no free fd at or above the floor.
pub static PLACEMENT_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Raise the floor that object-file fds are moved above to the end of the Valkey
/// core range for the current `maxclients`. Tiered only, main thread only. Turns
/// moving off when the open-file limit ends at or before the floor. Logs, and does
/// not fail, when the limit cannot hold every object fd above the floor. Errs only
/// when the floor is not actually past the event loop (object fds would then take fd
/// numbers in the Valkey core range, and clients past them are never served), or
/// when the limit cannot be raised.
pub fn raise_object_fd_floor(ctx: &Context) -> Result<(), String> {
    let maxclients: u64 = ctx
        .config_get("maxclients".to_string())
        .expect("CONFIG GET maxclients")
        .to_string()
        .parse()
        .expect("maxclients is numeric");
    let floor = i32::try_from(maxclients + VALKEY_FDSET_INCR)
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
    let floor = FD_FLOOR.fetch_max(floor, Ordering::Relaxed).max(floor);
    // Every fd number is below the limit, so none is past the Valkey core range.
    if limit <= floor as u64 {
        MOVE_ABOVE_FLOOR.store(false, Ordering::Relaxed);
        ctx.log_notice(&format!(
            "largeobj: open-file limit {limit} ends inside the Valkey core range \
             (fd {floor}); object-file fds are not moved"
        ));
        warn_if_cache_limited(crate::max_cached_fds() as u64);
        return Ok(());
    }
    MOVE_ABOVE_FLOOR.store(true, Ordering::Relaxed);
    if !PROBED.load(Ordering::Relaxed) {
        check_floor_past_event_loop(floor)?;
        PROBED.store(true, Ordering::Relaxed);
    }
    ctx.log_notice(&format!("largeobj: object-file fds placed at {floor}+"));
    let maxclients = floor as u64 - VALKEY_FDSET_INCR;
    if limit < floor as u64 + maxclients {
        ctx.log_warning(&format!(
            "largeobj: open-file limit {limit} leaves {} fds for object files above the \
             Valkey core range; commands fail with an error when more than that run at \
             once. Raise it to at least {}",
            limit - floor as u64,
            floor as u64 + maxclients
        ));
    }
    warn_if_cache_limited(crate::max_cached_fds() as u64);
    Ok(())
}

/// How many cached read fds fit above the floor once one slot per client is set
/// aside for commands (a SET's write fd, or a GET's fd evicted mid-read; both block
/// their client, so a client holds at most one). Zero when fds are not moved: a
/// cached fd would then hold a client's fd number for good.
fn cache_room() -> u64 {
    if !MOVE_ABOVE_FLOOR.load(Ordering::Relaxed) {
        return 0;
    }
    cache_room_for(nofile_limit().rlim_cur, FD_FLOOR.load(Ordering::Relaxed))
}

fn cache_room_for(limit: u64, floor: i32) -> u64 {
    let floor = floor as u64;
    // The floor tracks the highest maxclients seen; Valkey keeps clients above a
    // lowered maxclients connected, so that is the client bound.
    let maxclients = floor.saturating_sub(VALKEY_FDSET_INCR);
    limit.saturating_sub(floor).saturating_sub(maxclients)
}

/// The number of fds the fd cache may hold: `max-cached-fds`, limited to the room.
pub fn effective_cap() -> usize {
    (crate::max_cached_fds() as u64).min(cache_room()) as usize
}

/// Log, when `cap` fds cannot all be cached, how many can and what limit would fit.
pub fn warn_if_cache_limited(cap: u64) {
    let room = cache_room();
    if cap == 0 || cap <= room {
        return;
    }
    let limit = nofile_limit().rlim_cur;
    let floor = FD_FLOOR.load(Ordering::Relaxed) as u64;
    let maxclients = floor.saturating_sub(VALKEY_FDSET_INCR);
    let required = floor + cap + maxclients;
    let msg = if MOVE_ABOVE_FLOOR.load(Ordering::Relaxed) {
        format!(
            "largeobj: open-file limit {limit} is too low for max-cached-fds {cap} and \
             maxclients {maxclients}; the fd cache holds {room}. Raise the limit to at \
             least {required}"
        )
    } else {
        format!(
            "largeobj: open-file limit {limit} ends inside the Valkey core range, so the \
             fd cache is off (max-cached-fds {cap}). Raise the limit to at least {required}"
        )
    };
    valkey_module::logging::log_warning(msg);
}

/// Whether `cap` cached fds fit above the floor with one slot per client left for
/// commands. `CONFIG SET max-cached-fds` rejects a cap that does not. Always Ok
/// when fds are not moved: the cache is off then, and the cap is only logged.
pub fn check_fd_limit(cap: u64) -> Result<(), String> {
    if !MOVE_ABOVE_FLOOR.load(Ordering::Relaxed) {
        return Ok(());
    }
    let floor = FD_FLOOR.load(Ordering::Relaxed);
    let limit = nofile_limit().rlim_cur;
    if cap <= cache_room_for(limit, floor) {
        return Ok(());
    }
    let maxclients = floor as u64 - VALKEY_FDSET_INCR;
    Err(format!(
        "open-file limit {limit} is too low for max-cached-fds {cap} and \
         maxclients {maxclients}; raise it to at least {}",
        floor as u64 + cap + maxclients
    ))
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

/// One-shot check that `floor` is past the end of Valkey's event loop, so the
/// copied `maxclients + CONFIG_FDSET_INCR` really marks the end of the Valkey core
/// range. `EventLoopAdd` answers ERANGE exactly when `fd >= setsize`. The probe is
/// an eventfd we own (epoll rejects regular files), placed at `floor`, so no
/// client's event is touched; if it registers, it is removed at once. Main thread
/// only.
fn check_floor_past_event_loop(floor: i32) -> Result<(), String> {
    unsafe extern "C" fn noop(_fd: c_int, _data: *mut c_void, _mask: c_int) {}
    // Reads errno: call it right after the failing call, before the fds drop.
    let probe_err = |step: &str| {
        format!(
            "event-loop probe at fd {floor}: {step}: {}",
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
    // SAFETY: low is valid; returns a new fd >= floor or -1.
    let probe = unsafe { libc::fcntl(low.as_raw_fd(), libc::F_DUPFD_CLOEXEC, floor) };
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
             fd {floor} is inside it"
        ));
    }
    if std::io::Error::last_os_error().raw_os_error() != Some(libc::ERANGE) {
        return Err(probe_err("EventLoopAdd"));
    }
    Ok(())
}

/// Whether `fd` is below the floor, inside the Valkey core range. Always false when
/// fds are not moved.
pub fn below_floor(fd: RawFd) -> bool {
    MOVE_ABOVE_FLOOR.load(Ordering::Relaxed) && fd < FD_FLOOR.load(Ordering::Relaxed)
}

/// `open(2)` an object file, then move the fd above the floor set by
/// `raise_object_fd_floor`. Same contract as `open`: an fd, or -1 with errno set.
/// When no fd is free above the floor it closes the low fd and fails: keeping it
/// would take an fd number in the Valkey core range. The caller turns the failure
/// into an error reply.
pub fn open_object_file(path: &CStr, flags: libc::c_int, mode: libc::mode_t) -> RawFd {
    // SAFETY: path is NUL-terminated; open returns an fd or -1.
    let fd = unsafe { libc::open(path.as_ptr(), flags, mode as libc::c_uint) };
    if fd < 0 || !MOVE_ABOVE_FLOOR.load(Ordering::Relaxed) {
        return fd;
    }
    move_above(fd, FD_FLOOR.load(Ordering::Relaxed))
}

/// Move `fd` to the lowest free number at or above `floor` and close the original.
/// On failure closes `fd` and returns -1 with errno from the failed move.
fn move_above(fd: RawFd, floor: i32) -> RawFd {
    // SAFETY: fd is a valid fd we own; F_DUPFD_CLOEXEC returns a new fd >= floor or -1.
    let high = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, floor) };
    if high < 0 {
        let err = std::io::Error::last_os_error();
        // SAFETY: fd is ours and no longer used.
        unsafe { libc::close(fd) };
        note_failure(floor, &err);
        // SAFETY: __errno_location is this thread's errno; callers read it after us.
        unsafe { *libc::__errno_location() = err.raw_os_error().unwrap_or(libc::EMFILE) };
        return -1;
    }
    // SAFETY: fd is ours and no longer used; high refers to the same open file.
    unsafe { libc::close(fd) };
    high
}

/// Count a failed move and log the first of each burst of 1024.
fn note_failure(floor: i32, err: &std::io::Error) {
    let count = PLACEMENT_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if count % 1024 == 1 {
        valkey_module::logging::log_warning(format!(
            "largeobj: no free fd at or above {floor} for an object file: {err}; \
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
    fn move_above_places_the_fd_at_or_above_the_floor() {
        raise_soft_fd_limit().expect("raise the soft fd limit");
        let floor = 228;
        let fd = move_above(open_dev_null(), floor);
        assert!(fd >= floor, "fd {fd} is below the floor {floor}");
        // SAFETY: fd was just opened by this test.
        unsafe { libc::close(fd) };
    }

    #[test]
    fn move_above_fails_without_panicking_when_no_fd_is_free() {
        // F_DUPFD rejects a floor at or above the limit, which is what a full range does.
        let failures = PLACEMENT_FAILURES.load(Ordering::Relaxed);
        let fd = move_above(open_dev_null(), i32::MAX);
        let errno = std::io::Error::last_os_error().raw_os_error();
        assert_eq!(fd, -1);
        assert!(
            matches!(errno, Some(libc::EINVAL | libc::EMFILE)),
            "errno {errno:?}"
        );
        assert_eq!(PLACEMENT_FAILURES.load(Ordering::Relaxed), failures + 1);
    }

    #[test]
    fn cache_room_leaves_one_slot_per_client_for_commands() {
        let (floor, maxclients) = (10_128, 10_000);
        // No numbers above the floor, or fewer than one per client: no room.
        assert_eq!(cache_room_for(10_032, floor), 0);
        assert_eq!(cache_room_for(10_240, floor), 0);
        assert_eq!(cache_room_for(floor as u64 + maxclients, floor), 0);
        // Each number past that is one cached fd.
        assert_eq!(cache_room_for(floor as u64 + maxclients + 1, floor), 1);
        assert_eq!(cache_room_for(65_536, floor), 45_408);
    }
}

//! EFA libfabric RMA implementation.
//!
//! Provides EfaEndpoint (fabric + domain + AV + CQ + endpoint) and MemoryRegion
//! wrappers around the libfabric vtable-dispatch API.
//!
//! All libfabric "inline" functions (fi_domain, fi_endpoint, fi_mr_reg, fi_write,
//! fi_read, etc.) are dispatched through ops vtable pointers on fid objects.

use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::ffi;
use super::TransportError;

/// Timeout for CQ polling (seconds).
const CQ_TIMEOUT_SECS: u64 = 10;

/// Construct FI_VERSION from major/minor: (major << 16) | minor
fn fi_version(major: u32, minor: u32) -> u32 {
    (major << 16) | minor
}

// ─── Error Helpers ───────────────────────────────────────────────────────────

fn check_ret(ret: i32, op: &str) -> Result<(), String> {
    if ret != 0 {
        let msg = unsafe {
            let p = ffi::fi_strerror(-ret);
            if p.is_null() {
                format!("errno {}", -ret)
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().to_string()
            }
        };
        Err(format!("{} failed: {} (ret={})", op, msg, ret))
    } else {
        Ok(())
    }
}

fn check_ssize(ret: isize, op: &str) -> Result<(), String> {
    if ret < 0 {
        let msg = unsafe {
            let p = ffi::fi_strerror((-ret) as i32);
            if p.is_null() {
                format!("errno {}", -ret)
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().to_string()
            }
        };
        Err(format!("{} failed: {} (ret={})", op, msg, ret))
    } else {
        Ok(())
    }
}

// ─── MemoryRegion ────────────────────────────────────────────────────────────

/// Registered memory region. Wraps fid_mr.
/// The server registers its pool buffers for local access (FI_READ | FI_WRITE)
/// so they can be used as source/dest for fi_write/fi_read.
pub struct MemoryRegion {
    mr: *mut ffi::fid_mr,
}

// SAFETY: MR handles are thread-safe — libfabric documents fi_mr_desc() as
// safe to call concurrently when the domain uses FI_THREAD_SAFE threading model.
// The desc() pointer itself is immutable after registration (device-assigned).
unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

impl MemoryRegion {
    /// Remote key (for remote access — not used server-side, but needed if we
    /// ever expose server buffers to a remote peer).
    pub fn rkey(&self) -> u64 {
        unsafe { (*self.mr).key }
    }

    /// Local descriptor — passed to fi_write/fi_read as the `desc` parameter.
    pub fn desc(&self) -> *mut libc::c_void {
        unsafe { (*self.mr).mem_desc }
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        if !self.mr.is_null() {
            unsafe {
                let fid = &mut (*self.mr).fid;
                if let Some(close_fn) = (*fid.ops).close {
                    close_fn(fid as *mut ffi::fid);
                }
            }
        }
    }
}

// ─── CqHandle ────────────────────────────────────────────────────────────────

/// Wrapper around a raw CQ pointer that can be sent to a progress thread.
pub struct CqHandle {
    pub ptr: *mut ffi::fid_cq,
}

// SAFETY: libfabric CQ operations are thread-safe with FI_THREAD_SAFE domain.
unsafe impl Send for CqHandle {}

/// Drive the CQ progress engine in a polling loop. Required for EFA RDM endpoints
/// using FI_PROGRESS_MANUAL — the internal protocol only advances when polled.
pub fn run_cq_progress(handle: &CqHandle, stop: &Arc<AtomicBool>) {
    unsafe {
        let cq = handle.ptr;
        let cq_read_fn = match (*(*cq).ops).read {
            Some(f) => f,
            None => return,
        };
        let mut entry: ffi::fi_cq_entry = std::mem::zeroed();
        while !stop.load(Ordering::Relaxed) {
            let _ret = cq_read_fn(cq, &mut entry as *mut _ as *mut libc::c_void, 1);
            // Yield to avoid burning 100% CPU when idle.
            // The EFA RDM protocol only needs periodic polling, not tight spinning.
            std::thread::yield_now();
        }
    }
}

// ─── EfaEndpoint ─────────────────────────────────────────────────────────────

/// Full EFA endpoint: fabric + domain + AV + CQ + EP.
/// One per EFA device (currently we create one — multi-device LB is future work).
pub struct EfaEndpoint {
    info: *mut ffi::fi_info,
    fabric: *mut ffi::fid_fabric,
    domain: *mut ffi::fid_domain,
    av: *mut ffi::fid_av,
    cq: *mut ffi::fid_cq,
    ep: *mut ffi::fid_ep,
}

// SAFETY: EFA endpoint is used from the module's main thread and the transport
// thread pool. libfabric with FI_THREAD_SAFE supports this.
unsafe impl Send for EfaEndpoint {}
unsafe impl Sync for EfaEndpoint {}

impl EfaEndpoint {
    /// Create a new EFA endpoint with RMA capability.
    /// Returns Err if no EFA device is found (graceful fallback to TCP-only mode).
    pub fn new() -> Result<Self, TransportError> {
        unsafe {
            // fi_allocinfo() equivalent: fi_dupinfo(NULL)
            let hints: *mut ffi::fi_info = ffi::fi_dupinfo(ptr::null());
            if hints.is_null() {
                return Err(TransportError::DeviceNotFound);
            }

            (*hints).caps = (ffi::FI_MSG | ffi::FI_RMA) as u64;
            (*hints).ep_attr.as_mut().unwrap().type_ = ffi::fi_ep_type_FI_EP_RDM;

            // Use libc strdup so fi_freeinfo can safely free() the string
            let prov = b"efa\0";
            (*hints).fabric_attr.as_mut().unwrap().prov_name =
                libc::strdup(prov.as_ptr() as *const libc::c_char);

            // MR modes required by EFA
            (*hints).domain_attr.as_mut().unwrap().mr_mode = (ffi::FI_MR_LOCAL
                | ffi::FI_MR_VIRT_ADDR
                | ffi::FI_MR_ALLOCATED
                | ffi::FI_MR_PROV_KEY)
                as i32;

            let mut info: *mut ffi::fi_info = ptr::null_mut();
            let ret = ffi::fi_getinfo(
                fi_version(1, 18),
                ptr::null(),
                ptr::null(),
                0,
                hints,
                &mut info,
            );
            ffi::fi_freeinfo(hints);

            if ret != 0 || info.is_null() {
                return Err(TransportError::DeviceNotFound);
            }

            // fi_fabric
            let mut fabric: *mut ffi::fid_fabric = ptr::null_mut();
            let ret = ffi::fi_fabric((*info).fabric_attr, &mut fabric, ptr::null_mut());
            check_ret(ret, "fi_fabric").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_domain
            let mut domain: *mut ffi::fid_domain = ptr::null_mut();
            let domain_fn = (*(*fabric).ops)
                .domain
                .ok_or(TransportError::DeviceNotFound)?;
            let ret = domain_fn(fabric, info, &mut domain, ptr::null_mut());
            check_ret(ret, "fi_domain").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_av_open
            let mut av_attr: ffi::fi_av_attr = std::mem::zeroed();
            av_attr.type_ = ffi::fi_av_type_FI_AV_TABLE;
            av_attr.count = 256; // support up to 256 concurrent peers
            let mut av: *mut ffi::fid_av = ptr::null_mut();
            let av_open_fn = (*(*domain).ops)
                .av_open
                .ok_or(TransportError::DeviceNotFound)?;
            let ret = av_open_fn(domain, &mut av_attr, &mut av, ptr::null_mut());
            check_ret(ret, "fi_av_open").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_cq_open
            let mut cq_attr: ffi::fi_cq_attr = std::mem::zeroed();
            cq_attr.size = 1024;
            cq_attr.format = ffi::fi_cq_format_FI_CQ_FORMAT_CONTEXT;
            let mut cq: *mut ffi::fid_cq = ptr::null_mut();
            let cq_open_fn = (*(*domain).ops)
                .cq_open
                .ok_or(TransportError::DeviceNotFound)?;
            let ret = cq_open_fn(domain, &mut cq_attr, &mut cq, ptr::null_mut());
            check_ret(ret, "fi_cq_open").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_endpoint
            let mut ep: *mut ffi::fid_ep = ptr::null_mut();
            let endpoint_fn = (*(*domain).ops)
                .endpoint
                .ok_or(TransportError::DeviceNotFound)?;
            let ret = endpoint_fn(domain, info, &mut ep, ptr::null_mut());
            check_ret(ret, "fi_endpoint").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_ep_bind(av)
            let bind_fn = (*(*ep).fid.ops).bind.ok_or(TransportError::DeviceNotFound)?;
            let ret = bind_fn(
                &mut (*ep).fid as *mut ffi::fid,
                &mut (*av).fid as *mut ffi::fid,
                0,
            );
            check_ret(ret, "fi_ep_bind(av)").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_ep_bind(cq, FI_TRANSMIT | FI_RECV)
            let cq_flags = (ffi::FI_TRANSMIT | ffi::FI_RECV) as u64;
            let ret = bind_fn(
                &mut (*ep).fid as *mut ffi::fid,
                &mut (*cq).fid as *mut ffi::fid,
                cq_flags,
            );
            check_ret(ret, "fi_ep_bind(cq)").map_err(|_| TransportError::DeviceNotFound)?;

            // fi_enable
            let control_fn = (*(*ep).fid.ops)
                .control
                .ok_or(TransportError::DeviceNotFound)?;
            let ret = control_fn(
                &mut (*ep).fid as *mut ffi::fid,
                ffi::FI_ENABLE as i32,
                ptr::null_mut(),
            );
            check_ret(ret, "fi_enable").map_err(|_| TransportError::DeviceNotFound)?;

            Ok(EfaEndpoint {
                info,
                fabric,
                domain,
                av,
                cq,
                ep,
            })
        }
    }

    /// Get the local EFA address (32 bytes). Exchanged during LO.HELLO.
    pub fn get_local_addr(&self) -> Result<[u8; 32], TransportError> {
        unsafe {
            let getname_fn = (*(*self.ep).cm)
                .getname
                .ok_or(TransportError::DeviceNotFound)?;

            let mut addr_len: usize = 0;
            // First call to get size (returns -FI_ETOOSMALL, expected)
            let _ = getname_fn(
                &(*self.ep).fid as *const ffi::fid as *mut ffi::fid,
                ptr::null_mut(),
                &mut addr_len,
            );

            let mut addr = vec![0u8; addr_len];
            let ret = getname_fn(
                &(*self.ep).fid as *const ffi::fid as *mut ffi::fid,
                addr.as_mut_ptr() as *mut libc::c_void,
                &mut addr_len,
            );
            check_ret(ret, "fi_getname").map_err(|_| TransportError::DeviceNotFound)?;

            let mut result = [0u8; 32];
            let copy_len = addr_len.min(32);
            result[..copy_len].copy_from_slice(&addr[..copy_len]);
            Ok(result)
        }
    }

    /// Insert a peer's EFA address into the AV. Returns the fi_addr_t handle.
    pub fn insert_peer(&self, peer_addr: &[u8; 32]) -> Result<u64, TransportError> {
        unsafe {
            let insert_fn = (*(*self.av).ops)
                .insert
                .ok_or(TransportError::SessionCreateFailed)?;

            let mut fi_addr: u64 = 0;
            let ret = insert_fn(
                self.av,
                peer_addr.as_ptr() as *const libc::c_void,
                1,
                &mut fi_addr,
                0,
                ptr::null_mut(),
            );
            if ret != 1 {
                return Err(TransportError::SessionCreateFailed);
            }
            Ok(fi_addr)
        }
    }

    /// Register a buffer for local access (FI_READ | FI_WRITE).
    /// Used for server-side pool buffers that serve as source/dest for fi_write/fi_read.
    pub fn register_local_buffer(
        &self,
        buf_ptr: *mut u8,
        buf_len: usize,
    ) -> Result<MemoryRegion, TransportError> {
        unsafe {
            let reg_fn = (*(*self.domain).mr)
                .reg
                .ok_or(TransportError::RegistrationFailed)?;

            let mut mr: *mut ffi::fid_mr = ptr::null_mut();
            let access = (ffi::FI_READ | ffi::FI_WRITE) as u64;
            let ret = reg_fn(
                &mut (*self.domain).fid as *mut ffi::fid,
                buf_ptr as *const libc::c_void,
                buf_len,
                access,
                0, // offset
                0, // requested_key
                0, // flags
                &mut mr,
                ptr::null_mut(),
            );
            check_ret(ret, "fi_mr_reg(local)").map_err(|_| TransportError::RegistrationFailed)?;
            Ok(MemoryRegion { mr })
        }
    }

    /// Perform fi_write: push local buffer into remote memory.
    /// Blocks until CQ completion (synchronous for now).
    pub fn rma_write(
        &self,
        peer: u64,
        local_buf: *const u8,
        len: usize,
        local_desc: *mut libc::c_void,
        remote_addr: u64,
        rkey: u64,
    ) -> Result<(), TransportError> {
        unsafe {
            let write_fn = (*(*self.ep).rma)
                .write
                .ok_or(TransportError::WriteFailed { code: -1 })?;

            let mut ctx: u64 = 0;
            let ret = write_fn(
                self.ep,
                local_buf as *const libc::c_void,
                len,
                local_desc,
                peer,
                remote_addr,
                rkey,
                &mut ctx as *mut u64 as *mut libc::c_void,
            );
            check_ssize(ret, "fi_write")
                .map_err(|_| TransportError::WriteFailed { code: ret as i32 })?;
        }
        self.wait_cq()
            .map_err(|_| TransportError::WriteFailed { code: -2 })?;
        Ok(())
    }

    /// Perform fi_read: pull remote memory into local buffer.
    /// Blocks until CQ completion (synchronous for now).
    pub fn rma_read(
        &self,
        peer: u64,
        local_buf: *mut u8,
        len: usize,
        local_desc: *mut libc::c_void,
        remote_addr: u64,
        rkey: u64,
    ) -> Result<(), TransportError> {
        unsafe {
            let read_fn = (*(*self.ep).rma)
                .read
                .ok_or(TransportError::ReadFailed { code: -1 })?;

            let mut ctx: u64 = 0;
            let ret = read_fn(
                self.ep,
                local_buf as *mut libc::c_void,
                len,
                local_desc,
                peer,
                remote_addr,
                rkey,
                &mut ctx as *mut u64 as *mut libc::c_void,
            );
            check_ssize(ret, "fi_read")
                .map_err(|_| TransportError::ReadFailed { code: ret as i32 })?;
        }
        self.wait_cq()
            .map_err(|_| TransportError::ReadFailed { code: -2 })?;
        Ok(())
    }

    /// Get a CQ handle for running progress in a background thread.
    pub fn cq_handle(&self) -> CqHandle {
        CqHandle { ptr: self.cq }
    }

    /// Poll the completion queue until one entry completes or timeout.
    fn wait_cq(&self) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(CQ_TIMEOUT_SECS);
        unsafe {
            let cq_read_fn = (*(*self.cq).ops)
                .read
                .ok_or_else(|| "fi_ops_cq.read is NULL".to_string())?;

            let mut entry: ffi::fi_cq_entry = std::mem::zeroed();
            loop {
                let ret = cq_read_fn(self.cq, &mut entry as *mut _ as *mut libc::c_void, 1);
                if ret == 1 {
                    return Ok(());
                } else if ret == 0 || ret == -(ffi::FI_EAGAIN as isize) {
                    if Instant::now() > deadline {
                        // Check error queue before reporting timeout
                        if let Some(readerr) = (*(*self.cq).ops).readerr {
                            let mut err_entry: ffi::fi_cq_err_entry = std::mem::zeroed();
                            let eret = readerr(self.cq, &mut err_entry, 0);
                            if eret > 0 {
                                return Err(format!(
                                    "CQ error: err={}, prov_errno={}",
                                    err_entry.err, err_entry.prov_errno
                                ));
                            }
                        }
                        return Err("CQ poll timeout".to_string());
                    }
                    std::hint::spin_loop();
                } else if ret == -(ffi::FI_EAVAIL as isize) {
                    let cq_readerr_fn = (*(*self.cq).ops)
                        .readerr
                        .ok_or_else(|| "fi_ops_cq.readerr is NULL".to_string())?;
                    let mut err_entry: ffi::fi_cq_err_entry = std::mem::zeroed();
                    let eret = cq_readerr_fn(self.cq, &mut err_entry, 0);
                    if eret < 0 {
                        return Err(format!("fi_cq_readerr failed: {}", eret));
                    }
                    return Err(format!(
                        "CQ error: err={}, prov_errno={}",
                        err_entry.err, err_entry.prov_errno
                    ));
                } else {
                    return Err(format!("fi_cq_read unexpected: {}", ret));
                }
            }
        }
    }
}

impl Drop for EfaEndpoint {
    fn drop(&mut self) {
        unsafe {
            for fid_ptr in [
                &mut (*self.ep).fid as *mut ffi::fid,
                &mut (*self.cq).fid as *mut ffi::fid,
                &mut (*self.av).fid as *mut ffi::fid,
                &mut (*self.domain).fid as *mut ffi::fid,
                &mut (*self.fabric).fid as *mut ffi::fid,
            ] {
                if !fid_ptr.is_null() {
                    if let Some(close) = (*(*fid_ptr).ops).close {
                        close(fid_ptr);
                    }
                }
            }
            if !self.info.is_null() {
                ffi::fi_freeinfo(self.info);
            }
        }
    }
}

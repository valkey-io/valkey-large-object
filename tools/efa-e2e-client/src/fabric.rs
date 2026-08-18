//! Libfabric FFI wrappers for EFA RMA (one-sided RDMA).
//!
//! Provides EfaEndpoint (fabric + domain + AV + CQ + endpoint) and MemoryRegion.
//! All libfabric "inline" functions are dispatched through vtable pointers on fid objects
//! because bindgen cannot generate bindings for static inline C functions.

#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case, dead_code)]

use eyre::{eyre, Result};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod ffi {
    #![allow(
        non_upper_case_globals,
        non_camel_case_types,
        non_snake_case,
        dead_code,
        clippy::all
    )]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

/// Timeout for CQ polling (seconds).
const CQ_TIMEOUT_SECS: u64 = 10;

/// Construct FI_VERSION from major/minor: (major << 16) | minor
fn fi_version(major: u32, minor: u32) -> u32 {
    (major << 16) | minor
}

fn check_ret(ret: i32, op: &str) -> Result<()> {
    if ret != 0 {
        let msg = unsafe {
            let p = ffi::fi_strerror(-ret);
            if p.is_null() {
                format!("errno {}", -ret)
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().to_string()
            }
        };
        Err(eyre!("{} failed: {} (ret={})", op, msg, ret))
    } else {
        Ok(())
    }
}

fn check_ssize(ret: isize, op: &str) -> Result<()> {
    if ret < 0 {
        let msg = unsafe {
            let p = ffi::fi_strerror((-ret) as i32);
            if p.is_null() {
                format!("errno {}", -ret)
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().to_string()
            }
        };
        Err(eyre!("{} failed: {} (ret={})", op, msg, ret))
    } else {
        Ok(())
    }
}

// ─── MemoryRegion ────────────────────────────────────────────────────────────

/// Registered memory region. Wraps fid_mr.
pub struct MemoryRegion {
    mr: *mut ffi::fid_mr,
}

impl MemoryRegion {
    pub fn rkey(&self) -> u64 {
        unsafe { (*self.mr).key }
    }

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

/// CQ pointer wrapper for the progress thread.
pub struct CqHandle {
    pub ptr: *mut ffi::fid_cq,
}

// SAFETY: libfabric CQ operations are thread-safe with FI_THREAD_SAFE domain.
unsafe impl Send for CqHandle {}

/// Drive the CQ progress engine. Required for EFA RDM with FI_PROGRESS_MANUAL.
pub fn run_cq_progress(handle: CqHandle, stop: Arc<AtomicBool>) {
    unsafe {
        let cq = handle.ptr;
        let cq_read_fn = match (*(*cq).ops).read {
            Some(f) => f,
            None => return,
        };
        let mut entry: ffi::fi_cq_entry = std::mem::zeroed();
        while !stop.load(Ordering::Relaxed) {
            let _ret = cq_read_fn(cq, &mut entry as *mut _ as *mut libc::c_void, 1);
            std::hint::spin_loop();
        }
    }
}

// ─── EfaEndpoint ─────────────────────────────────────────────────────────────

/// Full EFA endpoint: fabric + domain + AV + CQ + EP.
pub struct EfaEndpoint {
    info: *mut ffi::fi_info,
    fabric: *mut ffi::fid_fabric,
    domain: *mut ffi::fid_domain,
    av: *mut ffi::fid_av,
    cq: *mut ffi::fid_cq,
    ep: *mut ffi::fid_ep,
}

impl EfaEndpoint {
    /// Create a new EFA endpoint with RMA capability.
    /// Requests API version 1.18 to enable automatic hardware RDMA write.
    pub fn new() -> Result<Self> {
        unsafe {
            let hints: *mut ffi::fi_info = ffi::fi_dupinfo(ptr::null());
            if hints.is_null() {
                return Err(eyre!("fi_dupinfo(NULL) returned null"));
            }

            (*hints).caps = (ffi::FI_MSG | ffi::FI_RMA) as u64;
            (*hints).ep_attr.as_mut().unwrap().type_ = ffi::fi_ep_type_FI_EP_RDM;
            (*hints).fabric_attr.as_mut().unwrap().prov_name =
                libc::strdup(b"efa\0".as_ptr() as *const libc::c_char);

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
            check_ret(ret, "fi_getinfo")?;

            if info.is_null() {
                return Err(eyre!("fi_getinfo returned no providers"));
            }

            // fi_fabric
            let mut fabric: *mut ffi::fid_fabric = ptr::null_mut();
            let ret = ffi::fi_fabric((*info).fabric_attr, &mut fabric, ptr::null_mut());
            check_ret(ret, "fi_fabric")?;

            // fi_domain
            let mut domain: *mut ffi::fid_domain = ptr::null_mut();
            let domain_fn = (*(*fabric).ops)
                .domain
                .ok_or_else(|| eyre!("fi_ops_fabric.domain is NULL"))?;
            let ret = domain_fn(fabric, info, &mut domain, ptr::null_mut());
            check_ret(ret, "fi_domain")?;

            // fi_av_open
            let mut av_attr: ffi::fi_av_attr = std::mem::zeroed();
            av_attr.type_ = ffi::fi_av_type_FI_AV_TABLE;
            av_attr.count = 16;
            let mut av: *mut ffi::fid_av = ptr::null_mut();
            let av_open_fn = (*(*domain).ops)
                .av_open
                .ok_or_else(|| eyre!("fi_ops_domain.av_open is NULL"))?;
            let ret = av_open_fn(domain, &mut av_attr, &mut av, ptr::null_mut());
            check_ret(ret, "fi_av_open")?;

            // fi_cq_open
            let mut cq_attr: ffi::fi_cq_attr = std::mem::zeroed();
            cq_attr.size = 128;
            cq_attr.format = ffi::fi_cq_format_FI_CQ_FORMAT_CONTEXT;
            let mut cq: *mut ffi::fid_cq = ptr::null_mut();
            let cq_open_fn = (*(*domain).ops)
                .cq_open
                .ok_or_else(|| eyre!("fi_ops_domain.cq_open is NULL"))?;
            let ret = cq_open_fn(domain, &mut cq_attr, &mut cq, ptr::null_mut());
            check_ret(ret, "fi_cq_open")?;

            // fi_endpoint
            let mut ep: *mut ffi::fid_ep = ptr::null_mut();
            let endpoint_fn = (*(*domain).ops)
                .endpoint
                .ok_or_else(|| eyre!("fi_ops_domain.endpoint is NULL"))?;
            let ret = endpoint_fn(domain, info, &mut ep, ptr::null_mut());
            check_ret(ret, "fi_endpoint")?;

            // fi_ep_bind(av)
            let bind_fn = (*(*ep).fid.ops)
                .bind
                .ok_or_else(|| eyre!("fi_ops.bind is NULL"))?;
            let ret = bind_fn(
                &mut (*ep).fid as *mut ffi::fid,
                &mut (*av).fid as *mut ffi::fid,
                0,
            );
            check_ret(ret, "fi_ep_bind(av)")?;

            // fi_ep_bind(cq, FI_TRANSMIT | FI_RECV)
            let cq_flags = (ffi::FI_TRANSMIT | ffi::FI_RECV) as u64;
            let ret = bind_fn(
                &mut (*ep).fid as *mut ffi::fid,
                &mut (*cq).fid as *mut ffi::fid,
                cq_flags,
            );
            check_ret(ret, "fi_ep_bind(cq)")?;

            // fi_enable
            let control_fn = (*(*ep).fid.ops)
                .control
                .ok_or_else(|| eyre!("fi_ops.control is NULL"))?;
            let ret = control_fn(
                &mut (*ep).fid as *mut ffi::fid,
                ffi::FI_ENABLE as i32,
                ptr::null_mut(),
            );
            check_ret(ret, "fi_enable")?;

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

    /// Get the local 32-byte EFA address.
    pub fn get_local_addr(&self) -> Result<Vec<u8>> {
        unsafe {
            let getname_fn = (*(*self.ep).cm)
                .getname
                .ok_or_else(|| eyre!("fi_ops_cm.getname is NULL"))?;

            let mut addr_len: usize = 0;
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
            check_ret(ret, "fi_getname")?;
            addr.truncate(addr_len);
            Ok(addr)
        }
    }

    /// Insert a peer's EFA address into the AV. Returns the fi_addr_t handle.
    pub fn insert_peer(&mut self, peer_addr: &[u8]) -> Result<u64> {
        unsafe {
            let insert_fn = (*(*self.av).ops)
                .insert
                .ok_or_else(|| eyre!("fi_ops_av.insert is NULL"))?;

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
                return Err(eyre!("fi_av_insert failed: returned {}", ret));
            }
            Ok(fi_addr)
        }
    }

    /// Register a buffer for remote access (FI_REMOTE_READ | FI_REMOTE_WRITE | FI_READ | FI_WRITE).
    pub fn register_remote(&self, buf: &mut [u8]) -> Result<MemoryRegion> {
        unsafe {
            let reg_fn = (*(*self.domain).mr)
                .reg
                .ok_or_else(|| eyre!("fi_ops_mr.reg is NULL"))?;

            let mut mr: *mut ffi::fid_mr = ptr::null_mut();
            let access =
                (ffi::FI_REMOTE_READ | ffi::FI_REMOTE_WRITE | ffi::FI_READ | ffi::FI_WRITE) as u64;
            let ret = reg_fn(
                &mut (*self.domain).fid as *mut ffi::fid,
                buf.as_mut_ptr() as *const libc::c_void,
                buf.len(),
                access,
                0,
                0,
                0,
                &mut mr,
                ptr::null_mut(),
            );
            check_ret(ret, "fi_mr_reg(remote)")?;
            Ok(MemoryRegion { mr })
        }
    }

    /// Get a CQ handle for the progress thread.
    pub fn cq_handle(&self) -> CqHandle {
        CqHandle { ptr: self.cq }
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

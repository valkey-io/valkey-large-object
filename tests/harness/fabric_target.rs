//! A raw libfabric passive peer for integration tests. Registers one buffer, exposes it, prints
//! the advertisement, and polls until bytes land. Copied/modified from vdma's
//! `examples/one_target.rs`.
//!
//! ```text
//! # prints: advertisement: <address-hex> <rkey> <remote-addr>
//! cargo run --features test-harness --bin fabric_target -- 127.0.0.1
//! # against a module started with fabric-provider Emulated:
//! LO.HELLO <address-hex>
//! LO.GET key <rkey> <remote-addr>            # writes the object into the target's buffer
//! ```
//!
//! `--read` prefills the buffer and serves it for `LO.SET key <len> <rkey> <remote-addr>` instead.
//! This holds the buffer open and lets the initiator do the verifying.
//!
//! `--efa` opens the `efa-direct` fabric instead of tcp loopback. There a target must hold the
//! initiator's address before it can be RMA'd against, so it takes one as its only positional.
//! `LO.HELLO` can't supply it, since HELLO needs the target's address first. The module logs each
//! service's address at load (`largeobj: fabric service N address <hex>`).

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::time::{Duration, Instant};

use dma_libfabric::sys::{
    fi_addr_t, fi_allocinfo, fi_av_attr, fi_av_insert, fi_av_open, fi_av_type_FI_AV_MAP, fi_close,
    fi_cq_attr, fi_cq_entry, fi_cq_format_FI_CQ_FORMAT_CONTEXT, fi_cq_open, fi_cq_read, fi_domain,
    fi_enable, fi_endpoint, fi_ep_bind, fi_ep_type_FI_EP_RDM, fi_fabric, fi_freeinfo, fi_getinfo,
    fi_getname, fi_info, fi_mr_key, fi_mr_reg, fi_version, fid_av, fid_cq, fid_domain, fid_ep,
    fid_fabric, fid_mr, FI_CONTEXT2, FI_MR_ALLOCATED, FI_MR_LOCAL, FI_MR_PROV_KEY, FI_MR_VIRT_ADDR,
    FI_MSG, FI_READ, FI_RECV, FI_REMOTE_READ, FI_REMOTE_WRITE, FI_RMA, FI_SOURCE, FI_TRANSMIT,
    FI_WRITE,
};
use dma_libfabric_protocol::{checksum, decode_hex, encode_hex};

const BUFFER_LEN: usize = 4096;

/// That which `one_transfer` writes and `read_from_peer` fetches.
const PATTERN: u8 = 0xab;

/// Everything opened, closed on drop in reverse construction order.
struct Target {
    memory_region: *mut fid_mr,
    endpoint: *mut fid_ep,
    completion_queue: *mut fid_cq,
    address_vector: *mut fid_av,
    domain: *mut fid_domain,
    fabric: *mut fid_fabric,
    info: *mut fi_info,
}

impl Drop for Target {
    fn drop(&mut self) {
        // SAFETY: each fid is closed once, in reverse order, and null when never opened.
        unsafe {
            for fid in [
                self.memory_region.cast::<c_void>(),
                self.endpoint.cast::<c_void>(),
                self.completion_queue.cast::<c_void>(),
                self.address_vector.cast::<c_void>(),
                self.domain.cast::<c_void>(),
                self.fabric.cast::<c_void>(),
            ] {
                if !fid.is_null() {
                    // Every fid_* starts with its `fid`, so the first field is the handle to close.
                    fi_close(fid.cast());
                }
            }
            if !self.info.is_null() {
                fi_freeinfo(self.info);
            }
        }
    }
}

fn check(code: i32, what: &str) -> Result<(), String> {
    if 0 == code {
        return Ok(());
    }
    Err(format!("{what} failed: {code}"))
}

fn main() -> Result<(), String> {
    let (flags, positional): (Vec<String>, Vec<String>) = std::env::args()
        .skip(1)
        .partition(|argument| argument.starts_with("--"));
    // Serve the buffer for a remote read rather than wait for a remote write.
    let serve_read = flags.iter().any(|flag| "--read" == flag);
    let use_efa = flags.iter().any(|flag| "--efa" == flag);
    let mut arguments = positional.into_iter();
    // An EFA endpoint binds to the device and advertises its own fabric address, so there is no
    // source to pin — the first positional is the initiator's address instead.
    let bind = if use_efa { None } else { arguments.next() };
    let initiator = arguments.next();

    let mut target = Target {
        memory_region: ptr::null_mut(),
        endpoint: ptr::null_mut(),
        completion_queue: ptr::null_mut(),
        address_vector: ptr::null_mut(),
        domain: ptr::null_mut(),
        fabric: ptr::null_mut(),
        info: ptr::null_mut(),
    };

    // Must match the initiator's hints or the two pick incompatible providers; mirrors
    // `endpoint::query_info`. On efa the provider name alone is ambiguous — it exposes both rxr
    // `efa` fabric and device-RDMA `efa-direct`.
    let provider = CString::new(if use_efa { "efa" } else { "tcp" })
        .map_err(|_| "provider name".to_string())?;
    let fabric_name = CString::new("efa-direct").map_err(|_| "fabric name".to_string())?;
    let node = match &bind {
        Some(bind) => Some(CString::new(bind.as_str()).map_err(|_| "bind address".to_string())?),
        None => None,
    };
    // SAFETY: hints are freed below; the CStrings outlive the fi_getinfo call and are detached from
    // the hints first, so fi_freeinfo doesn't free Rust-owned memory.
    unsafe {
        let hints = fi_allocinfo();
        if hints.is_null() {
            return Err("fi_allocinfo failed".into());
        }
        (*hints).caps =
            u64::from(FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE);
        (*(*hints).ep_attr).type_ = fi_ep_type_FI_EP_RDM;
        (*(*hints).domain_attr).mr_mode =
            (FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR) as i32;
        (*(*hints).fabric_attr).prov_name = provider.as_ptr().cast_mut();
        if use_efa {
            (*(*hints).fabric_attr).name = fabric_name.as_ptr().cast_mut();
            // efa-direct requires FI_CONTEXT2. This side posts no ops, but the mode has to match for
            // fi_getinfo to hand back the right fabric.
            (*hints).mode |= FI_CONTEXT2;
        }
        let (node_ptr, flags) = match &node {
            Some(node) => (node.as_ptr(), FI_SOURCE),
            None => (ptr::null(), 0),
        };
        let code = fi_getinfo(
            fi_version(),
            node_ptr,
            ptr::null(),
            flags,
            hints,
            &mut target.info,
        );
        (*(*hints).fabric_attr).prov_name = ptr::null_mut();
        (*(*hints).fabric_attr).name = ptr::null_mut();
        fi_freeinfo(hints);
        check(code, "fi_getinfo")?;
        if target.info.is_null() {
            return Err("no provider matched".into());
        }
    }

    // SAFETY: `target.info` is a live fi_info; each handle is stored in `target`, which closes them.
    let uses_virtual_addressing = unsafe {
        check(
            fi_fabric(
                (*target.info).fabric_attr,
                &mut target.fabric,
                ptr::null_mut(),
            ),
            "fi_fabric",
        )?;
        check(
            fi_domain(
                target.fabric,
                target.info,
                &mut target.domain,
                ptr::null_mut(),
            ),
            "fi_domain",
        )?;
        let mut av_attr: fi_av_attr = std::mem::zeroed();
        av_attr.type_ = fi_av_type_FI_AV_MAP;
        check(
            fi_av_open(
                target.domain,
                &mut av_attr,
                &mut target.address_vector,
                ptr::null_mut(),
            ),
            "fi_av_open",
        )?;
        let mut cq_attr: fi_cq_attr = std::mem::zeroed();
        cq_attr.format = fi_cq_format_FI_CQ_FORMAT_CONTEXT;
        check(
            fi_cq_open(
                target.domain,
                &mut cq_attr,
                &mut target.completion_queue,
                ptr::null_mut(),
            ),
            "fi_cq_open",
        )?;
        check(
            fi_endpoint(
                target.domain,
                target.info,
                &mut target.endpoint,
                ptr::null_mut(),
            ),
            "fi_endpoint",
        )?;
        check(
            fi_ep_bind(target.endpoint, &mut (*target.address_vector).fid, 0),
            "fi_ep_bind(av)",
        )?;
        check(
            fi_ep_bind(
                target.endpoint,
                &mut (*target.completion_queue).fid,
                u64::from(FI_TRANSMIT | FI_RECV),
            ),
            "fi_ep_bind(cq)",
        )?;
        check(fi_enable(target.endpoint), "fi_enable")?;
        (*(*target.info).domain_attr).mr_mode as u32 & FI_MR_VIRT_ADDR != 0
    };

    // Remote-accessible, unlike the initiator's local-only operands. Prefilled when the initiator is
    // the one fetching it.
    let buffer = vec![if serve_read { PATTERN } else { 0 }; BUFFER_LEN];
    let pointer = buffer.as_ptr().cast_mut();
    // SAFETY: `buffer` outlives the registration, which `target` closes before this returns.
    let remote_key = unsafe {
        check(
            fi_mr_reg(
                target.domain,
                pointer.cast::<c_void>(),
                BUFFER_LEN,
                u64::from(FI_REMOTE_READ | FI_REMOTE_WRITE | FI_READ | FI_WRITE),
                0,
                0,
                0,
                &mut target.memory_region,
                ptr::null_mut(),
            ),
            "fi_mr_reg",
        )?;
        fi_mr_key(target.memory_region)
    };
    // On FI_MR_VIRT_ADDR providers like efa the remote address is the buffer's virtual address; on
    // tcp it is an offset into the region, so 0.
    let remote_address = if uses_virtual_addressing {
        pointer as u64
    } else {
        0
    };

    // SAFETY: writes `length` bytes of address into `address`, after asking for the size.
    let address = unsafe {
        let mut length: usize = 0;
        fi_getname(&mut (*target.endpoint).fid, ptr::null_mut(), &mut length);
        let mut address = vec![0u8; length];
        check(
            fi_getname(
                &mut (*target.endpoint).fid,
                address.as_mut_ptr().cast::<c_void>(),
                &mut length,
            ),
            "fi_getname",
        )?;
        address.truncate(length);
        address
    };

    // efa-direct requires a target to hold the initiator's address before it can be RMA'd against.
    // tcp does not.
    if let Some(initiator) = &initiator {
        let bytes = decode_hex(initiator.as_bytes()).map_err(|error| error.to_string())?;
        let mut peer: fi_addr_t = 0;
        // SAFETY: one address of the provider's own format, from the initiator.
        let inserted = unsafe {
            fi_av_insert(
                target.address_vector,
                bytes.as_ptr().cast::<c_void>(),
                1,
                &mut peer,
                0,
                ptr::null_mut(),
            )
        };
        if 1 != inserted {
            return Err(format!("fi_av_insert inserted {inserted} of 1"));
        }
    }

    println!(
        "advertisement: {} {remote_key} {remote_address}",
        encode_hex(&address)
    );
    if serve_read {
        println!(
            "serving {BUFFER_LEN} bytes of {PATTERN:#04x} for a remote read, crc {:#010x} — the initiator verifies",
            checksum(&buffer)
        );
    } else {
        println!("waiting for a transfer into {BUFFER_LEN} bytes...");
    }

    // Under FI_PROGRESS_MANUAL nothing services the inbound RMA unless the queue is polled, and the
    // write raises no completion here — so poll for progress, watch the buffer for arrival.
    let deadline = Instant::now() + Duration::from_secs(300);
    while Instant::now() < deadline {
        // SAFETY: a one-entry read into an owned value; the queue is live until `target` drops.
        unsafe {
            let mut entry: fi_cq_entry = std::mem::zeroed();
            fi_cq_read(
                target.completion_queue,
                (&raw mut entry).cast::<c_void>(),
                1,
            );
        }
        // A read leaves nothing behind here, so there is no arrival to watch for: hold the buffer
        // open and keep polling until the window closes.
        if serve_read {
            std::thread::sleep(Duration::from_millis(1));
            continue;
        }
        // Volatile: the NIC writes these bytes outside the compiler's model.
        // SAFETY: in bounds of the live registered buffer.
        let landed =
            (0..BUFFER_LEN).all(|index| PATTERN == unsafe { pointer.add(index).read_volatile() });
        if landed {
            // The CRC the initiator also prints, so the pair cross-checks.
            println!(
                "received {BUFFER_LEN} bytes, crc {:#010x} — payload verified",
                checksum(&buffer)
            );
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if serve_read {
        return Ok(());
    }
    Err("timed out waiting for a transfer".into())
}

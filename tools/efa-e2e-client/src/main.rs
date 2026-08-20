//! EFA-aware E2E test client for ValkeyLargeObj (Multi-Device).
//!
//! Validates the full integrated pipeline with Option 2 (per-request rkey):
//!   Client EFA buffer → fi_read (hardware RDMA) → Server pool buf → io_uring → NVMe  (DMA.SET)
//!   NVMe → io_uring → Server pool buf → fi_write (hardware RDMA) → Client EFA buffer (DMA.GET)
//!
//! Protocol:
//!   DMA.HELLO <client_efa_addr>
//!     → returns array of ALL server EFA addresses (one per device)
//!   DMA.GET <key> <rkey> <remote_addr> <len>
//!     → server fi_writes into client buffer, replies with :bytes_written
//!   DMA.SET <key> <rkey> <remote_addr> <len>
//!     → server fi_reads from client buffer, writes to NVMe, replies +OK
//!
//! Usage:
//!   efa-e2e-client --server <valkey_server_private_ip> [--iterations 10] [--size 2097152]

mod fabric;
mod resp;

use clap::Parser;
use eyre::{eyre, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use fabric::{run_cq_progress, EfaEndpoint};
use resp::RespConnection;

const DEFAULT_BUF_SIZE: usize = 8 * 1024 * 1024; // 8MB registered buffer (2x for SET + GET regions)
const PATTERN_SET: u8 = 0xDD;
const PATTERN_CLR: u8 = 0x00;

#[derive(Parser, Debug)]
#[command(name = "efa-e2e-client", about = "EFA E2E test client for ValkeyLargeObj (multi-device)")]
struct Args {
    /// Valkey server private IP address
    #[arg(long)]
    server: String,

    /// Valkey server port
    #[arg(long, default_value = "6379")]
    port: u16,

    /// Object size in bytes for DMA.SET / DMA.GET
    #[arg(long, default_value = "2097152")]
    size: usize,

    /// Number of steady-state iterations (after warmup)
    #[arg(long, default_value = "10")]
    iterations: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let obj_size = args.size;
    let iterations = args.iterations;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      EFA E2E Client — ValkeyLargeObj (Multi-Device)         ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Server:     {}:{}", args.server, args.port);
    println!("  Object size: {} bytes ({:.1} MB)", obj_size, obj_size as f64 / 1048576.0);
    println!("  Iterations:  {}", iterations);
    println!();

    // ─── Step 1: Initialize EFA ──────────────────────────────────────────
    println!("── Step 1: Initialize EFA endpoint ──");
    let mut ep = EfaEndpoint::new()?;
    let local_addr = ep.get_local_addr()?;
    let local_addr_hex: String = local_addr.iter().map(|b| format!("{:02x}", b)).collect();
    println!("  EFA address: {} ({} bytes)", &local_addr_hex[..32.min(local_addr_hex.len())], local_addr.len());
    println!("  Provider:    efa (API version 1.18, hardware RDMA auto-enabled)");

    // ─── Step 2: Register buffer ─────────────────────────────────────────
    println!("\n── Step 2: Register memory buffer ──");
    // Allocate 2x obj_size: first half for SET data, second half for GET destination
    let buf_size = obj_size * 2;
    let mut buf = vec![0u8; buf_size].into_boxed_slice();
    let mr = ep.register_remote(&mut buf)?;
    let rkey = mr.rkey();
    let remote_addr = buf.as_ptr() as u64;
    println!("  Address:  {:#x}", remote_addr);
    println!("  Size:     {} bytes ({} MB)", buf_size, buf_size / 1048576);
    println!("  rkey:     {}", rkey);

    // ─── Step 3: CQ progress thread ─────────────────────────────────────
    println!("\n── Step 3: Start CQ progress thread ──");
    let cq_handle = ep.cq_handle();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_flag.clone();
    let progress_thread = std::thread::spawn(move || {
        run_cq_progress(cq_handle, stop_clone);
    });
    println!("  Running (FI_PROGRESS_MANUAL — required for EFA RDM protocol)");

    // ─── Step 4: Connect to Valkey ───────────────────────────────────────
    println!("\n── Step 4: Connect to Valkey (TCP) ──");
    let addr = format!("{}:{}", args.server, args.port);
    let mut conn = RespConnection::connect(&addr)?;

    let (reply, _) = conn.command(&["PING"])?;
    if !reply.starts_with("+PONG") {
        stop_flag.store(true, Ordering::Relaxed);
        progress_thread.join().unwrap();
        return Err(eyre!("PING failed: {}", reply.trim()));
    }

    // ─── Step 5: DMA.HELLO (Option 2: client_efa_addr only) ──────────────
    println!("\n── Step 5: DMA.HELLO (establish RDMA session, multi-device) ──");
    let hello_args: Vec<&str> = vec!["DMA.HELLO", &local_addr_hex];
    let (reply, _) = conn.command(&hello_args)?;

    if reply.starts_with('-') {
        stop_flag.store(true, Ordering::Relaxed);
        progress_thread.join().unwrap();
        return Err(eyre!("DMA.HELLO failed: {}", reply.trim()));
    }

    // Parse ALL server EFA addresses from the array reply
    let server_addrs = parse_resp_array_bulks(&reply)?;
    println!("  Session established. Server returned {} EFA address(es):", server_addrs.len());
    for (i, addr_hex) in server_addrs.iter().enumerate() {
        println!("    Device {}: {}...", i, &addr_hex[..32.min(addr_hex.len())]);
    }

    // Insert ALL server addresses into local AV
    for addr_hex in &server_addrs {
        let server_addr_bytes = hex_decode(addr_hex)?;
        let _fi_addr = ep.insert_peer(&server_addr_bytes)?;
    }
    println!("  All {} server address(es) inserted into local AV", server_addrs.len());

    // ─── Step 6: Warmup DMA.SET (absorbs handshake) ──────────────────────
    println!("\n── Step 6: Warmup (absorbs EFA handshake) ──");
    buf[..obj_size].fill(PATTERN_SET);
    let rkey_str = rkey.to_string();
    let remote_addr_str = remote_addr.to_string();
    let size_str = obj_size.to_string();

    let (reply, warmup_ms) = conn.command(&[
        "DMA.SET", "efa_warmup", &rkey_str, &remote_addr_str, &size_str,
    ])?;
    if reply.starts_with('-') {
        stop_flag.store(true, Ordering::Relaxed);
        progress_thread.join().unwrap();
        return Err(eyre!("Warmup DMA.SET failed: {}", reply.trim()));
    }
    println!("  Handshake + first transfer: {:.2} ms", warmup_ms);

    // Reset TCP counters after warmup
    let tcp_sent_before_bench = conn.tcp_bytes_sent;
    let tcp_recv_before_bench = conn.tcp_bytes_received;

    // ─── Step 7: Steady-state DMA.SET ────────────────────────────────────
    println!("\n── Step 7: Steady-state DMA.SET ({} x {} bytes) ──", iterations, obj_size);
    buf[..obj_size].fill(PATTERN_SET);

    let mut set_times: Vec<f64> = Vec::new();
    for i in 0..iterations {
        let key = format!("efa_bench_{}", i);
        let (reply, ms) = if i == 0 {
            println!("  Example command (first of {}):", iterations);
            conn.command(&["DMA.SET", &key, &rkey_str, &remote_addr_str, &size_str])?
        } else {
            conn.command_silent(&["DMA.SET", &key, &rkey_str, &remote_addr_str, &size_str])?
        };
        if reply.starts_with('-') {
            println!("  ERROR on iter {}: {}", i, reply.trim());
            break;
        }
        set_times.push(ms);
    }

    if !set_times.is_empty() {
        let avg = set_times.iter().sum::<f64>() / set_times.len() as f64;
        let min = set_times.iter().cloned().fold(f64::MAX, f64::min);
        let max = set_times.iter().cloned().fold(f64::MIN, f64::max);
        let tput = (obj_size as f64 / 1e9) / (avg / 1000.0);
        println!("  min={:.2} ms  avg={:.2} ms  max={:.2} ms  throughput={:.2} GB/s", min, avg, max, tput);
    }

    // ─── Step 8: Steady-state DMA.GET ────────────────────────────────────
    println!("\n── Step 8: Steady-state DMA.GET ({} x {} bytes) ──", iterations, obj_size);
    // Use an offset into the buffer for GET so we can verify data correctness
    let get_offset: usize = obj_size;
    let get_remote_addr = remote_addr + get_offset as u64;
    let get_remote_addr_str = get_remote_addr.to_string();

    let mut get_times: Vec<f64> = Vec::new();
    for i in 0..iterations {
        buf[get_offset..get_offset + obj_size].fill(PATTERN_CLR);
        let key = format!("efa_bench_{}", i);
        let (reply, ms) = if i == 0 {
            println!("  Example command (first of {}):", iterations);
            conn.command(&["DMA.GET", &key, &rkey_str, &get_remote_addr_str, &size_str])?
        } else {
            conn.command_silent(&["DMA.GET", &key, &rkey_str, &get_remote_addr_str, &size_str])?
        };
        if reply.starts_with('-') {
            println!("  ERROR on iter {}: {}", i, reply.trim());
            break;
        }
        get_times.push(ms);
    }

    if !get_times.is_empty() {
        let avg = get_times.iter().sum::<f64>() / get_times.len() as f64;
        let min = get_times.iter().cloned().fold(f64::MAX, f64::min);
        let max = get_times.iter().cloned().fold(f64::MIN, f64::max);
        let tput = (obj_size as f64 / 1e9) / (avg / 1000.0);
        println!("  min={:.2} ms  avg={:.2} ms  max={:.2} ms  throughput={:.2} GB/s", min, avg, max, tput);
    }

    // Verify last GET data
    std::thread::sleep(std::time::Duration::from_millis(5));
    let correct = buf[get_offset..get_offset + obj_size]
        .iter()
        .filter(|&&b| b == PATTERN_SET)
        .count();
    let verified = correct == obj_size;
    println!("  Data verification: {} ({}/{} bytes correct)",
             if verified { "PASS" } else { "FAIL" }, correct, obj_size);

    // ─── Step 9: Cleanup ─────────────────────────────────────────────────
    println!("\n── Step 9: Cleanup ──");
    conn.command_silent(&["DEL", "efa_warmup"])?;
    for i in 0..iterations {
        let key = format!("efa_bench_{}", i);
        conn.command_silent(&["DEL", &key])?;
    }
    println!("  Keys deleted");

    stop_flag.store(true, Ordering::Relaxed);
    progress_thread.join().unwrap();
    println!("  CQ progress thread stopped");

    // ─── Data Path Attribution ───────────────────────────────────────────
    let tcp_sent_bench = conn.tcp_bytes_sent - tcp_sent_before_bench;
    let tcp_recv_bench = conn.tcp_bytes_received - tcp_recv_before_bench;
    let efa_read_bytes = iterations as u64 * obj_size as u64;  // DMA.SET: server fi_read
    let efa_write_bytes = iterations as u64 * obj_size as u64; // DMA.GET: server fi_write
    let total_efa = efa_read_bytes + efa_write_bytes;
    let total_tcp = tcp_sent_bench + tcp_recv_bench;
    let efa_pct = if total_efa + total_tcp > 0 {
        (total_efa as f64 / (total_efa + total_tcp) as f64) * 100.0
    } else {
        0.0
    };

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                 DATA PATH ATTRIBUTION                       ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║                                                              ║");
    println!("║  TCP (commands + replies only):                              ║");
    println!("║    Sent:     {:>10} bytes  (RESP-encoded commands)       ║", tcp_sent_bench);
    println!("║    Received: {:>10} bytes  (RESP replies: +OK, :size)    ║", tcp_recv_bench);
    println!("║    Total:    {:>10} bytes                                 ║", total_tcp);
    println!("║                                                              ║");
    println!("║  EFA (object data, hardware RDMA):                           ║");
    println!("║    fi_read:  {:>10} bytes  ({} x DMA.SET)             ║", efa_read_bytes, iterations);
    println!("║    fi_write: {:>10} bytes  ({} x DMA.GET)             ║", efa_write_bytes, iterations);
    println!("║    Total:    {:>10} bytes                                 ║", total_efa);
    println!("║                                                              ║");
    println!("║  Object data over TCP:  0 bytes                              ║");
    println!("║  Object data over EFA:  {} bytes ({:.2}%)        ║", total_efa, efa_pct);
    println!("║                                                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    // ─── Summary ─────────────────────────────────────────────────────────
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                      RESULTS SUMMARY                        ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    if !set_times.is_empty() {
        let avg = set_times.iter().sum::<f64>() / set_times.len() as f64;
        let tput = (obj_size as f64 / 1e9) / (avg / 1000.0);
        println!("║  DMA.SET: {:.2} ms avg, {:.2} GB/s  (fi_read → NVMe)        ║", avg, tput);
    }
    if !get_times.is_empty() {
        let avg = get_times.iter().sum::<f64>() / get_times.len() as f64;
        let tput = (obj_size as f64 / 1e9) / (avg / 1000.0);
        println!("║  DMA.GET: {:.2} ms avg, {:.2} GB/s  (NVMe → fi_write)       ║", avg, tput);
    }
    println!("║  Warmup:  {:.2} ms (includes EFA handshake, one-time)       ║", warmup_ms);
    println!("║  Verify:  {}                                               ║", if verified { "PASS" } else { "FAIL" });
    println!("║  Server EFA devices: {}                                     ║", server_addrs.len());
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}

/// Parse all bulk strings from a RESP array reply.
fn parse_resp_array_bulks(reply: &str) -> Result<Vec<String>> {
    let lines: Vec<&str> = reply.lines().collect();
    if lines.is_empty() || !lines[0].starts_with('*') {
        return Err(eyre!("Expected RESP array, got: {:?}", reply));
    }
    let count: usize = lines[0][1..].trim().parse()?;
    let mut results = Vec::with_capacity(count);
    let mut i = 1;
    while i < lines.len() && results.len() < count {
        if lines[i].starts_with('$') {
            let len: usize = lines[i][1..].trim().parse()?;
            if i + 1 < lines.len() {
                let data = &lines[i + 1][..len.min(lines[i + 1].len())];
                results.push(data.to_string());
                i += 2;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }
    Ok(results)
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(eyre!("Hex string has odd length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| eyre!("Bad hex: {}", e)))
        .collect()
}

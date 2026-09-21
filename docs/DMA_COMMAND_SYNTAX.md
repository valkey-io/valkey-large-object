# DMA Command Design: Session Routing + Per-Request Memory Targeting

**Date:** 2026-08-18  **Status:** Option 2 implemented  **Author:** @KarthikSubbarao

---

## Summary

The ValkeyLargeObj module moves large objects (4KB–512MB) between GPU client memory and NVMe storage via EFA RDMA. This document specifies the DMA command syntax — how the GPU client and Valkey server establish an RDMA session and transfer data.

Three designs were evaluated. **Option 2 (per-request rkey with session routing) is implemented now.** The choice between Option 2 and Option 3 (fully stateless) remains open — both are viable for production. We implement Option 2 first. If we later decide Option 3 is better, the migration is subtractive (remove the HELLO command and session map) rather than additive in the ValkeyLargeObj Module and is minimal churn.

**Why start with Option 2:**
- There is a simple 1:N (client EFA device to server EFA device) mapping per client's HELLO session.
- The cost of fi_av_insert during registering the client EFA addr is during the HELLO operation and this keeps the GET and SET commands light weight.
- Migration to Option 3 is easy: delete the session map + HELLO handler, add lazy fi_av_insert.
- It creates a path forward to support Option 3 if needed with minimal churn.
- TBD — Having a session (client_efa_addr) concept per Valkey client can be beneficial for metrics, session limits, and disconnect cleanup.

**Why Option 3 might win later:**
- Zero state management — simpler module code. No Valkey client has a session associated with it.
- Valkey clients (once HELLO is used) are not pinned to one client EFA addr.

**Option 2 is chosen for the initial integration** for clarity and because it's the natural first step toward option 3, should it be found necessary.
- The libfabric connection management is still stateful, and without a client lifetime to bind to it, the connection address vector registry grows without bound. Ignoring state does not make it stateless.
- Swimlanes are easier to debug and understand, and client address errors are better communicated at HELLO during a handshake than at the first LO.GET down in some workflow.
- Opting into 3 in the future can be done by making the commands modal - HELLO without an address makes LO.GET require a target address.

---

## Table of Contents

- [1. Terminology](#1-terminology)
- [2. Option 2: Per-Request rkey with HELLO](#2-option-2-per-request-rkey-with-hello)
- [3. Option 3: Fully Stateless](#3-option-3-fully-stateless)
- [4. Comparison](#4-comparison)
- [5. Rejected: Option 1 (Per-Session Regions)](#5-rejected-option-1-per-session-regions)

---

## 1. Terminology

### Roles

- **Valkey server** = RDMA initiator. Pushes data to GPU client (`fi_write`) or pulls data from it (`fi_read`).
- **GPU client** = RDMA target. Passive during data transfer. Receives into or provides from its registered memory.
- Both sides call `fi_av_insert` to register the other's EFA address before any transfer.

### Terms

Each term builds on the previous.

| # | Term | Definition |
|---|------|-----------|
| 1 | EFA device | Physical network card using SRD (Scalable Reliable Datagram). An i8ge.48xlarge has 1–4. |
| 2 | Memory region (MR) | Chunk of memory (GPU VRAM or host RAM) registered with a local EFA device via `fi_mr_reg()`. Pins physical pages for DMA. Expensive (~ms), done once per region. |
| 3 | rkey | Token returned by `fi_mr_reg()`. Grants a remote machine permission to read/write that region. |
| 4 | remote_addr | Byte address within a registered memory region. Combined with rkey, tells the remote machine exactly where to read/write. |
| 5 | EFA address | 32-byte endpoint identity from `fi_getname()`. Exchanged so each side can target the other. |
| 6 | fi_av_insert | Registers a remote EFA address into the local address vector. Returns `dest_fi_addr` — the handle for targeting that peer. |
| 7 | fi_write | Server pushes local buffer into client's registered memory: `fi_write(ep, buf, len, desc, dest_fi_addr, remote_addr, rkey, ctx)`. |
| 8 | fi_read | Server pulls from client's registered memory into local buffer. Same parameters. |

---

## 2. Option 2: Per-Request rkey with HELLO

Client establishes a session once via `LO.HELLO` using the provided client EFA addr. Server registers the client EFA addr on all its N EFA devices. Subsequent GET/SET commands carry the client's rkey and remote_addr — the client chooses which memory region to use per request. A second `LO.HELLO` on the same connection is refused (`ERR DMA session already established`), because the efa-direct provider cannot hold a client's old and new endpoint at once when the new one reuses the old QPN.

**Threading model:** The session holds routing handles for all N server EFA devices. Any thread can use any device for a given operation — the server picks the least-loaded device (least-loaded). Threads do not own specific devices.

**Session:** Module-level bookkeeping only — not an EFA-level connection (EFA is connectionless datagrams). Stores one routing handle (`dest_fi_addr`) per server EFA device for targeting the client. No memory regions, no rkeys stored. Created at HELLO, freed on disconnect. The EFA layer itself has no concept of a session.

### LO.HELLO

```
LO.HELLO <client_efa_addr_hex>
```

**What happens:**

| Step | Who | Action |
|------|-----|--------|
| 1 | Client | `fi_getname()` → gets own EFA address |
| 2 | Client | Sends `LO.HELLO` with its EFA address |
| 3 | Server | `fi_av_insert(client_addr)` on ALL N EFA devices → N dest_fi_addr handles |
| 4 | Server | Returns array of ALL N server EFA addresses |
| 5 | Client | `fi_av_insert(server_addr)` for each returned address |

**Result:** Server can fi_write/fi_read to the client's single EFA address from any of its N devices (1 client EFA addr : N server EFA devices). Client accepts writes from any server device.

### LO.GET (DMA)

```
LO.GET <key> <rkey> <remote_addr> <len>
```

| Step | Who | Action |
|------|-----|--------|
| 1 | Client | Picks a registered memory region, sends command with its rkey + target address |
| 2 | Server | Reads object from NVMe/DRAM into a pool buffer |
| 3 | Server | Picks EFA device (picks least-loaded device) |
| 4 | Server | `fi_write(buf, len, dest_fi_addr, remote_addr, rkey)` → pushes to client memory |
| 5 | Server | Waits for CQ completion, replies with integer (bytes written) |
| 6 | Client | Data is already in GPU memory at remote_addr. Uses it directly. |

### LO.SET (DMA)

```
LO.SET <key> <rkey> <remote_addr> <len>
```

| Step | Who | Action |
|------|-----|--------|
| 1 | Client | Places data in a registered memory region, sends command with its rkey + source address |
| 2 | Server | Allocates a pool buffer |
| 3 | Server | Picks EFA device (least-loaded LB) |
| 4 | Server | `fi_read(buf, len, dest_fi_addr, remote_addr, rkey)` → pulls from client memory |
| 5 | Server | Waits for CQ completion, writes buffer to NVMe, stores key mapping |
| 6 | Server | Replies OK |


### Per-Client State

| What | Lifetime | Set when |
|------|----------|----------|
| dest_fi_addr handles (N per client) | Session | LO.HELLO |
| TCP connection | Session | Client connects |
| rkey | Per-request | Client chooses |
| remote_addr | Per-request | Client chooses |
| Server EFA device | Per-request | Server picks (LB) |

---

## 3. Option 3: Fully Stateless

No HELLO. No per-client session or state. Client passes its EFA address on every command.

**Threading model:** The server has N threads, each owning one server EFA device (its own endpoint and address vector). Unlike Option 2 where any thread can use any device, here each thread can only use its own device. Whichever thread handles the request determines which server EFA device is used — there is no explicit load balancing across devices.

On the first request from a new client to a given thread, that thread registers the client EFA address in its own address vector and caches the routing handle. Different threads register the same client independently.

**No session, no bookkeeping:** Unlike Option 2, the module does not explicitly track which clients are "connected" for DMA. Routing handles live in per-thread caches that grow silently. You cannot easily enumerate DMA clients, enforce a connection limit, or clean up on disconnect — cached entries persist until the process exits.

### LO.EFAINFO (Discovery)

```
LO.EFAINFO
```

Client sends this to get the server's EFA address. Server returns one address (first thread's endpoint). Client calls `fi_av_insert(server_addr)` and registers memory regions. No handshake — ready immediately.

### LO.GET (DMA)

```
LO.GET <key> <client_efa_addr> <remote_addr> <rkey> [len]
```

| Step | Who | Action |
|------|-----|--------|
| 1 | Client | Sends command with its OWN EFA address + rkey + target address |
| 2 | Server thread | Checks has this client_efa_addr been registered before? |
| 3 | Server thread | No → registers client_efa_addr, caches routing handle |
| 4 | Server | Reads object from NVMe/DRAM into buffer |
| 5 | Server | `fi_write(buf, len, dest_fi_addr, remote_addr, rkey)` from this thread's EFA device |
| 6 | Server | Waits for CQ completion, replies with integer |

### LO.SET (DMA)

```
LO.SET <key> <client_efa_addr> <remote_addr> <rkey> <len>
```

| Step | Who | Action |
|------|-----|--------|
| 1 | Client | Places data in registered memory, sends command with EFA addr + rkey + source address |
| 2 | Server thread | Registers client if first time (same as GET) |
| 3 | Server | `fi_read(buf, len, dest_fi_addr, remote_addr, rkey)` → pulls from client |
| 4 | Server | Writes to NVMe, stores key mapping, replies OK |

### Per-Client State

| What | Lifetime | Set when |
|------|----------|----------|
| dest_fi_addr (routing handle) | Cached after first request | Server registers client on first GET/SET |
| TCP connection | Session | Client connects |
| rkey | Per-request | Client sends |
| remote_addr | Per-request | Client sends |
| client_efa_addr | Per-request | Client sends |
| Server EFA device | Per-request | Determined by which thread handles the request |
---

## 4. Comparison

| Aspect | Option 2 (implemented) | Option 3 (deferred) |
|--------|:---:|:---:|
| Setup command | `LO.HELLO` (client addr) | `LO.EFAINFO` (discovery only) |
| Setup reply | ALL server EFA addrs | ONE server EFA addr |
| fi_av_insert timing | Once at HELLO, on ALL devices | Lazy, per thread, on first request from each client |
| GET/SET args (beyond key) | rkey, remote_addr, len | client_efa_addr, rkey, remote_addr, len |
| Per-client server state | N dest_fi_addr handles | Zero (lazy registration on first request) |
| Client memory locked? | No | No |
| Client EFA addr locked? | Yes (per session) | No (per request) |
| Server EFA device selection | Explicit (picks least-loaded device) | Implicit (whichever thread handles the request) |
| Disconnect cleanup | Free N handles | Nothing |
| Per-command overhead | ~26 bytes | ~90 bytes |
| Session metrics/limits | Yes | No (would need cache-based counting) |
| Multi-device LB | Explicit, optimal | Implicit, depends on thread distribution |
| Migration direction | → delete session map, add client_addr arg | ← add HELLO, remove client_addr arg |

---

## 5. Rejected: Option 1 (Per-Session Regions)

Client declares all memory regions at HELLO. Server stores them. Commands reference regions by index.

```
LO.HELLO <client_efa_addr> <num_regions> <rkey_0> <addr_0> <len_0> ...
LO.GET <key> <region_idx> <offset>
LO.SET <key> <len> <region_idx> <offset>
```

**Rejected because:**
- Client memory is locked at session creation — cannot use regions registered after HELLO.
- Client cannot recover from GPU memory reallocation without re-HELLO.
- Server is locked to one EFA device per session — no per-operation load balancing.

Both Option 2 and Option 3 eliminate these problems.

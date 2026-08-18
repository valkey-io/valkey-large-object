# DMA Command Syntax — Two Approaches

## Terms

Read in order — each term builds on the previous.

| # | Term | Definition |
|---|------|-----------|
| 1 | EFA device | A physical network card on an EC2 instance that can transfer data at high speed using a protocol called SRD (Scalable Reliable Datagram — a low-latency packet protocol developed by AWS). An i8ge.48xlarge instance may have 1-4 EFA devices. |
| 2 | Memory region (MR) | A chunk of memory (host RAM or GPU video memory) that you register with your local EFA device via `fi_mr_reg()`. Registration pins the physical pages so the EFA network card can read/write that memory directly (Direct Memory Access) without involving the CPU. Registration is expensive (~milliseconds), so you do it once and reuse. |
| 3 | rkey (remote key) | A token returned when you register a memory region. You give this token to a remote machine to grant it permission to read from or write into your registered memory. Without your rkey, no remote machine can touch your memory. |
| 4 | remote_addr | A specific address within a registered memory region. Combined with an rkey, it tells the remote machine: "write to THIS exact location — here's proof you're allowed." Example: you register 1GB of GPU memory starting at address 0x7f000000. rkey grants access to the whole 1GB. remote_addr=0x7f000400 means "write starting 1024 bytes in." |
| 5 | server EFA address | The 32-byte identity of one EFA device on the Valkey server (obtained via `fi_getname()`). The GPU client needs this to tell its own EFA hardware: "accept incoming data transfers from this specific server device." |
| 6 | client EFA address | The 32-byte identity of the GPU client's EFA device (obtained via `fi_getname()`). The Valkey server needs this to target fi_write/fi_read to the client. Without it, the server has no destination to send data to. |
| 7 | fi_av_insert | A libfabric call that registers a remote machine's EFA address into your local "address vector" (a table of known remote endpoints). You must do this before transfers can happen. Both sides call it: the GPU client calls `fi_av_insert(server_efa_address)` to accept incoming writes, and the Valkey server calls `fi_av_insert(client_efa_address)` to get a handle for targeting the client. |
| 8 | dest_fi_addr | A handle returned by `fi_av_insert()`. It's the server's local lookup ID for the client in its address vector. The server passes this as a parameter to fi_write/fi_read to tell libfabric which remote machine to send data to. |
| 9 | fi_write | A libfabric call: `fi_write(ep, local_buf, len, desc, dest_fi_addr, remote_addr, rkey, ctx)`. The Valkey server pushes data directly into the GPU client's registered memory. Parameters: dest_fi_addr (which client), remote_addr (where in their memory), rkey (proof of permission). The client's CPU is not involved — the client's EFA hardware handles it. |
| 10 | fi_read | The reverse of fi_write — the Valkey server pulls data FROM the GPU client's registered memory into its own local buffer. Same parameters. Used for DMA.SET (client has data, server needs it). |
| 11 | Session | Minimal per-connection state on the Valkey server: stores the dest_fi_addr handles for this client (one per server EFA device, all obtained via fi_av_insert at HELLO time). This is the ONLY per-client state the server needs — no memory regions, no rkeys. The server uses these handles to address fi_write/fi_read to the correct client. Could also be called "client routing state." |

## Roles

- **Valkey server** = initiator. It pushes data TO the GPU client (fi_write) or pulls data FROM the GPU client (fi_read).
- **GPU client** = target. It receives data into (or provides data from) its registered GPU/host memory. It is passive during the actual data transfer.
- Both sides must call `fi_av_insert` to register the other's EFA address before any transfer can happen.

---

## Option 1: Per-Session Regions

Client declares all memory regions at HELLO. Server stores them. Requests reference regions by index.

### DMA.HELLO

```
DMA.HELLO <client_efa_addr_hex> <num_regions> <rkey_0> <remote_addr_0> <len_0> [<rkey_1> <remote_addr_1> <len_1> ...]
```

**GPU client does before sending:**
1. Registers one or more memory regions with its local EFA device: `fi_mr_reg(base_pointer, total_length)` → gets one rkey per registered region. A region is a contiguous range of memory (could be 4KB or 1GB — size is the client's choice). The client can later use any address within a region for transfers by passing that region's rkey + the specific address.
2. Gets its own EFA address: `fi_getname()` → client_efa_addr

**GPU client sends:** its EFA address + all regions (rkey, base remote_addr, len per region).

**Valkey server does on receive:**
1. Picks ONE of its EFA devices for this session
2. `fi_av_insert(client_efa_addr)` on that one device — now the server can target the client from this device
3. Stores the regions array in the session
4. `fi_getname()` on that device → server_efa_addr

**Valkey server returns:** server_efa_addr (one address — the device it picked).

**GPU client does on reply:**
1. `fi_av_insert(server_efa_addr)` — now the client's NIC will accept incoming writes from the server

**Result:** Both sides have each other in their address vectors. Server has the client's memory regions stored. Ready for DMA.GET/DMA.SET.

---

### DMA.GET

```
DMA.GET <key> <region_idx> <remote_offset>
```

- `region_idx`: index into the client's regions array from HELLO (e.g., 0, 1, 2)
- `remote_offset`: byte offset within that region

**GPU client does:** sends the command and waits for reply.

**Valkey server does:**
1. Reads the value for `key` from NVMe/DRAM into a local buffer
2. Looks up `session.regions[region_idx]` → gets rkey, base_remote_addr
3. Computes destination: `dest = base_remote_addr + remote_offset`
4. `fi_write(local_buf, len, dest, rkey)` — pushes data into client's memory
5. Waits for fi_write completion (CQ poll)
6. Replies to client: integer (bytes written)

**GPU client does on reply:** data is already in its GPU memory at `regions[region_idx].addr + offset`. Uses it directly.

---

### DMA.SET

```
DMA.SET <key> <len> <region_idx> <remote_offset>
```

- `region_idx`: index into client's regions (where source data lives on the client)
- `remote_offset`: byte offset within that region

**GPU client does:** places data in `regions[region_idx].addr + offset`, sends the command, waits for reply.

**Valkey server does:**
1. Allocates a local buffer
2. Looks up `session.regions[region_idx]` → gets rkey, base_remote_addr
3. Computes source: `src = base_remote_addr + remote_offset`
4. `fi_read(local_buf, len, src, rkey)` — pulls data from client's memory into local buffer
5. Waits for fi_read completion (CQ poll)
6. Writes local buffer to NVMe, stores key → object mapping
7. Replies to client: OK

**GPU client does on reply:** nothing — data was already sent.

---

### Problems

- **Locked memory:** rkey per session prevents clients from using memory regions registered after session creation, switching between registrations on different local EFA devices, or recovering from GPU memory reallocation (old rkey invalidated). Client must re-HELLO to use any new memory registration.
- **Locked server EFA device:** The server does `fi_av_insert(client_addr)` on ONE device at HELLO time. All fi_write/fi_read calls for this session must go through that device because the client is only registered in that device's address vector. The server cannot use a different EFA device without re-inserting the client on another device (not part of this design). Result: no per-operation server-side load balancing.

---

## Option 2: Per-Request rkey (Recommended)

Nothing is locked at session creation. Client sends rkey + remote_addr per command. Server picks its EFA device per operation. Client is free to use any memory region, any local EFA device, any time.

### DMA.HELLO

```
DMA.HELLO <client_efa_addr>
```

**GPU client does before sending:**
1. Gets its own EFA address: `fi_getname()` → client_efa_addr

**GPU client sends:** its EFA address.

**Valkey server does on receive:**
1. `fi_av_insert(client_efa_addr)` on ALL of its EFA devices — registers the client on every device so any device can fi_write/fi_read to this client later
2. `fi_getname()` on each device → collects all server EFA addresses

**Valkey server returns:** array of ALL server EFA addresses.

**GPU client does on reply:**
1. `fi_av_insert(server_addr)` for EACH returned address — registers all server devices so the client's NIC will accept writes from any of them
2. (Separately, at any time) `fi_mr_reg(base_pointer, total_length)` on whatever GPU/host memory it wants → gets one rkey per registered region. Can register multiple regions of any size, on multiple local EFA devices.

**Result:** Both sides have each other in their address vectors — and the client is registered on ALL server devices (not just one). Ready for DMA.GET/DMA.SET on any device.

---

### DMA.GET

```
DMA.GET <key> <rkey> <remote_addr> <len>
```

- `key`: the Valkey key to read
- `rkey`: the client's memory region token for this specific request
- `remote_addr`: the exact address where the server should write the data
- `len`: number of bytes the client expects

**GPU client does:**
1. Picks which of its registered memory regions to receive data into
2. Sends the command with that region's rkey and the specific address within it
3. Waits for reply

**Valkey server does:**
1. Looks up `dest_fi_addr` for this TCP connection (stored since HELLO)
2. Reads the value for `key` from NVMe/DRAM into a local buffer
3. Picks which of its EFA devices to use (load balancing: best-of-two on in-flight count)
4. `fi_write(local_buf, len, desc, dest_fi_addr, remote_addr, rkey, ctx)` from the chosen device
5. Waits for fi_write completion (CQ poll)
6. Replies to client: integer (bytes written)

**GPU client does on reply:** data is already in its memory at `remote_addr`. Uses it directly.

---

### DMA.SET

```
DMA.SET <key> <rkey> <remote_addr> <len>
```

- `key`: the Valkey key to write
- `rkey`: the client's memory region token for this specific request
- `remote_addr`: the exact address where the source data lives on the client
- `len`: number of bytes to pull from the client

**GPU client does:**
1. Places data in one of its registered memory regions
2. Sends the command with that region's rkey and the specific address of the data
3. Waits for reply

**Valkey server does:**
1. Looks up `dest_fi_addr` for this TCP connection (stored since HELLO)
2. Allocates a local buffer
3. Picks which of its EFA devices to use (load balancing)
4. `fi_read(local_buf, len, desc, dest_fi_addr, remote_addr, rkey, ctx)` from the chosen device — pulls data from client
5. Waits for fi_read completion (CQ poll)
6. Writes local buffer to NVMe, stores key → object mapping
7. Replies to client: OK

**GPU client does on reply:** nothing — data was already sent.

---

### What Is Locked vs Dynamic

| Aspect | Per-session (fixed at HELLO) | Per-request (dynamic) |
|--------|:---:|:---:|
| dest_fi_addr (client routing) | ✓ (from fi_av_insert at HELLO) | |
| TCP connection | ✓ | |
| Client memory region / rkey | | ✓ |
| Client remote_addr | | ✓ |
| Client local EFA device | | ✓ (different rkeys from different EFA devices) |
| Server EFA device (outbound) | | ✓ (server decides per-op) |

Only the TCP connection and the client's routing handle (dest_fi_addr) are per-session. Everything else is dynamic per-operation.

### Multi-EFA Behavior

**Server (N devices):**
- All N addresses returned in DMA.HELLO reply
- Client is registered on all N devices (fi_av_insert at HELLO time)
- Server internally picks which device per-op (best-of-two on in-flight count)
- Client is unaware of which server device serves any given request

**Client (M devices):**
- Client can register memory on each of its M local EFA devices independently
- Client passes the rkey from whichever EFA device holds the target buffer
- Client can use one TCP connection and still target different local EFA devices per-request (different rkeys)
- OR client opens M connections for parallelism (Valkey is serial per-connection)

**N:1 (N server → 1 client device per connection):** Fully supported.
**1:M (1 server → M client devices via per-request rkey):** Supported — client just passes different rkeys.
**N:M:** Works naturally — server picks its device, client picks its rkey. No explicit coordination needed.

---

## Option 3: Fully Stateless (Kevin's POC)

No HELLO command. No per-client state at all. Client passes its EFA address on every single command alongside rkey + remote_addr. Server discovers client identity and does fi_av_insert lazily on first contact per worker thread.

This is the actual design implemented in Kevin McGehee's POC (`McGeheeBigObjectModule` on code.amazon.com).

### Discovery

```
BO.EFAINFO
```

**GPU client does:** sends the command to get the server's EFA address.

**Valkey server does:** returns its own EFA address (hex string). Currently returns ONE address (the first worker's endpoint).

**GPU client does on reply:**
1. `fi_av_insert(server_efa_addr)` — registers server so NIC accepts incoming writes
2. `fi_mr_reg()` on whatever memory it wants — gets rkey(s)

No handshake. No session setup. Client is ready to issue commands immediately.

---

### DMA.GET

```
BO.GET <key> <client_efa_addr> <remote_addr> <rkey> [len]
```

- `key`: the Valkey key to read
- `client_efa_addr`: the client's EFA device address (hex) — tells the server WHERE to send
- `remote_addr`: the exact address where the server should write the data
- `rkey`: the client's memory region token
- `len`: optional, defaults to stored object size

**GPU client does:**
1. Picks which of its registered memory regions to receive data into
2. Sends the command with its OWN EFA address + that region's rkey + specific address
3. Waits for reply

**Valkey server does (on worker thread):**
1. Checks per-worker peer cache: is this `client_efa_addr` already in this worker's AV?
   - Cache hit: use cached `dest_fi_addr`
   - Cache miss: `fi_av_insert(client_efa_addr)` → gets `dest_fi_addr`, caches it
2. Reads value from NVMe/DRAM into a local registered buffer
3. `fi_write(local_buf, len, desc, dest_fi_addr, remote_addr, rkey, ctx)` from this worker's EFA device
4. Waits for completion (CQ poll)
5. Replies to client: integer (bytes written)

**GPU client does on reply:** data is already in its memory. Uses it directly.

---

### DMA.SET

```
BO.SET <key> <client_efa_addr> <remote_addr> <rkey> <len>
```

- `key`: the Valkey key to write
- `client_efa_addr`: the client's EFA device address (hex)
- `remote_addr`: the exact address where the source data lives on the client
- `rkey`: the client's memory region token
- `len`: number of bytes to pull from the client

**GPU client does:**
1. Places data in one of its registered memory regions
2. Sends the command with its EFA address + that region's rkey + data address
3. Waits for reply

**Valkey server does (on worker thread):**
1. Peer cache lookup / fi_av_insert (same as GET)
2. `fi_read(local_buf, len, desc, dest_fi_addr, remote_addr, rkey, ctx)` — pulls data from client
3. Writes local buffer to NVMe, stores key → object mapping
4. Replies to client: OK

**GPU client does on reply:** nothing — data was already sent.

---

### What Is Locked vs Dynamic

| Aspect | Per-session | Per-request (dynamic) |
|--------|:---:|:---:|
| Client EFA address | | ✓ (in every command) |
| dest_fi_addr (client routing) | | ✓ (lazy, per-worker cache) |
| Client memory region / rkey | | ✓ |
| Client remote_addr | | ✓ |
| Client local EFA device | | ✓ |
| Server EFA device (outbound) | | ✓ (implicit: whichever worker dequeues the request) |
| TCP connection | ✓ | |

NOTHING is per-session except the TCP connection itself. All EFA state is per-request.

### Multi-EFA Behavior

**Server:** Each worker thread owns one EFA device. Load balancing is implicit — requests are dispatched to workers via a shared FIFO queue. No explicit per-op device selection.

**Client:** Can use different EFA addresses across requests on the same TCP connection. The server treats each EFA address independently (separate fi_av_insert per worker that encounters it).

---

## Pros and Cons

| | Option 1 (per-session regions) | Option 2 (per-request rkey, HELLO) | Option 3 (fully stateless, no HELLO) |
|---|---|---|---|
| **Simplicity** | Simple server — all state upfront | Moderate — HELLO for routing, per-request for data | Simplest server — zero setup, zero state |
| **Client flexibility** | None — locked at HELLO | Full for memory, fixed routing | Full for everything |
| **Per-request overhead** | Lowest — just index + offset | Moderate — parse rkey + addr | Highest — parse rkey + addr + efa_addr + cache lookup |
| **fi_av_insert cost** | Once at HELLO (fast) | Once at HELLO on N devices (fast) | Amortized via cache, but cold-start per worker (~1 fi_av_insert per worker per new client) |
| **Server state** | Large (stored regions per client) | Small (dest_fi_addr handles) | Zero (just a per-worker 1-entry cache) |
| **Disconnect cleanup** | Must free session state | Must free dest_fi_addr handles | Nothing to clean up |
| **Multi-EFA server** | No (locked to one device) | Yes (explicit load balancing) | Implicit (worker pool) |
| **Client reconnect cost** | Expensive (re-HELLO + re-register all regions) | Moderate (re-HELLO) | Free (just send next command) |
| **Sharding across workers** | Works (regions stored globally) | Works (dest_fi_addr stored globally) | Works (each shard carries efa_addr, each worker resolves independently) |
| **Extra bytes per command** | ~4 bytes (region_idx + offset) | ~26 bytes (rkey + remote_addr + len) | ~90 bytes (efa_addr hex + rkey + remote_addr + len) |
| **N:1 multi-EFA** | No | Yes (all devices registered at HELLO) | Partial (only the worker's device that dequeues — no cross-device load balancing within one request) |

### When to prefer each:

**Option 2 over Option 3:**
- You want explicit per-op server-side EFA load balancing (best-of-two across all N devices). Option 3's implicit worker-dispatch doesn't guarantee optimal device selection.
- You want fi_av_insert to happen ONCE upfront on ALL devices (predictable, no cold-start latency on first request to a worker).
- You want the server to know the client is "connected" for RDMA (useful for metrics, connection counting, graceful shutdown).

**Option 3 over Option 2:**
- Zero state management — nothing to allocate, nothing to clean up, no disconnect hooks needed.
- Client can change its EFA device mid-stream without any re-negotiation.
- Simpler module code — no HELLO command handler, no session map.
- Proven working (Kevin's POC ships data at full EFA line rate with this design).
- Sharding across workers is trivial — each shard request carries everything needed, workers are fully independent.

---

## Summary

| | Option 1 (per-session) | Option 2 (per-request rkey, HELLO) | Option 3 (fully stateless) |
|---|---|---|---|
| Setup command | DMA.HELLO (client addr + regions) | DMA.HELLO (client addr only) | BO.EFAINFO query (no handshake) |
| Setup reply | one server EFA addr | ALL server EFA addrs | one server EFA addr |
| Server fi_av_insert | on ONE device, at HELLO | on ALL devices, at HELLO | lazy per-worker, on first request |
| Client fi_av_insert | one server addr | ALL server addrs | one server addr |
| GET args | key, region_idx, offset | key, rkey, remote_addr, len | key, client_efa_addr, remote_addr, rkey, len |
| SET args | key, len, region_idx, offset | key, rkey, remote_addr, len | key, client_efa_addr, remote_addr, rkey, len |
| Per-client server state | regions + dest_fi_addr | dest_fi_addr handles (N) | zero (per-worker cache) |
| Client memory locked? | Yes | No | No |
| Client EFA device locked? | Yes | No | No |
| Server EFA device locked? | Yes | No — explicit LB | No — implicit (worker dispatch) |
| Disconnect cleanup | Free session | Free handles | Nothing |

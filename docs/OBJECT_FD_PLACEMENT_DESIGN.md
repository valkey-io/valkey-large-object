# Object-file fd placement: design

Scope: Tiered mode of the large-object module. Other modes open no long-lived object files and are not
affected.

This document describes the behavior being built. Section 9 lists the choices where a reviewer's opinion
is wanted.

## 1. Problem

Valkey serves a client only if the client's socket fd number is below `maxclients + 128`. That is the size
of Valkey's event loop. A socket numbered at or above it is accepted and counted, but never served: the
client hangs. Call such a client *stranded*.

The kernel gives every new fd the lowest free number. In Tiered mode the module keeps object files open:

- an fd cache, so repeated GETs on an object do not reopen its file;
- a write fd for each SET in progress;
- a read fd for each GET that misses the cache.

Left alone, these take numbers inside the range Valkey needs for clients. Clients connecting later get
numbers past the end and are stranded.

A client can only be stranded if the process is allowed fd numbers past that range. If the fd limit ends
inside the range, there is nowhere to be stranded to, but also nowhere for the module to move its fds.

## 2. Terms

| Term | Meaning |
|---|---|
| **fd limit** (`limit`) | The process's soft `RLIMIT_NOFILE`: the kernel hands out fd numbers `0 .. limit-1` only. The hard limit is just the ceiling the soft limit can be raised to. The module raises the soft limit to the hard limit at load, so afterwards the two are equal. |
| **core range** | fd numbers `0 .. maxclients + 127`, the numbers Valkey core's event loop covers: Valkey's own fds (32 reserved), up to `maxclients` client sockets, and 96 spare. |
| **module start** (`module_start`) | `maxclients + 128`, the first number after the core range. It uses the highest `maxclients` ever set, so it only rises. |
| **module range** | fd numbers `module_start .. limit-1`. The module's fds live here. It is empty when `limit <= module_start`. |
| **command fd** | An fd held while one command runs: a SET's write fd, or the read fd of a GET that missed the cache. Each client runs one command at a time, so at most `maxclients` command fds are open together. |
| **command reserve** | `maxclients` numbers in the module range kept free for command fds. |
| **placement** | Moving each object-file fd into the module range right after it is opened. *On* when the fd limit covers the command reserve (`limit >= module_start + maxclients`), *off* otherwise (`fd-reserve Lenient` only). |
| **fd cache** | Read fds kept open between GETs. An optimization, never required. |
| **`max-cached-fds`** | The config: how many fds the operator wants cached. |
| **fd cache max entries** | The most fds the fd cache holds: `min(max-cached-fds, max(0, limit - module_start - maxclients))`. The second part is what the fd limit allows after the command reserve. INFO `fd_cache_max_entries`. |
| **fd cache entries** | Fds in the fd cache now. INFO `fd_cache_entries`. |

## 3. Behavior in one paragraph

The module has one requirement of the fd limit: it covers the command reserve, `limit >= module_start +
maxclients`. When it does, placement is on: every object-file fd is moved into the module range, so it
never takes a number a client needs, and the cache gets whatever the module range holds beyond the
command reserve (diagram in 5.3). When it does not, the `fd-reserve` policy decides. `Lenient` (default)
loads with placement and the cache off, as stock Valkey would, and logs why; a `max-cached-fds` that does
not fit beside the reserve is clamped. `Strict` also needs the whole `max-cached-fds` to fit: it refuses to
load otherwise, rejects a `CONFIG SET max-cached-fds` that does not fit, and aborts the server if a later
`CONFIG SET maxclients` breaks the reserve or squeezes the cache. With placement on, a shortage of
numbers in the module range is never fatal: a command that finds no free fd fails with an error reply and
the server keeps running.

## 4. Where the fd limit comes from

The module does not choose the fd limit. Valkey does, before the module loads.

- At startup and on every `CONFIG SET maxclients`, Valkey checks that the limit is at least
  `maxclients + 32`. If not, it sets **both** the soft and the hard limit to exactly `maxclients + 32`.
  If the limit is already high enough, Valkey leaves it alone.
- So a low launch limit (systemd's default of 1024, or `LimitNOFILE=10032`) ends at `maxclients + 32`.
  That is below the module start (`maxclients + 128`): the module range is empty.
- A high launch limit (for example `LimitNOFILE=65536`) is left alone, so the module range is not empty.
- After Valkey has lowered the limit, a `CONFIG SET maxclients` to a larger value is rejected: Valkey would
  have to raise the hard limit, which needs root, and the Valkey process normally is not root. The sender
  of the command is irrelevant.
- Valkey also lowers the **hard** limit on a later `CONFIG SET maxclients` that needs a higher soft limit
  than the current one (65536 hard became 30032 in one run).

These were checked on a real server, not only read from the code (see section 10).

**Does Valkey change the limit after the module has loaded?** Only on `CONFIG SET maxclients`, and only
when the soft limit is below the new `maxclients + 32`. Once the module has raised the soft limit to the
hard limit, the two are equal, so that needs more than the hard limit:
- Valkey tries to set both limits to the new `maxclients + 32`. Raising the hard limit needs root, so for
  an ordinary user it fails, and Valkey rejects the `CONFIG SET`. The limit stays as it was.
- If Valkey does run as root, the call succeeds and the limit rises. The module then re-reads it
  (section 5.5).
- A `CONFIG SET maxclients` that fits inside the current soft limit leaves it alone.

So with an ordinary user the limit does not change after load. This is from reading `adjustOpenFilesLimit`
(`server.c`) and `updateMaxclients` (`config.c`). The experiments in section 10 cover the cases without
the module and the case with the module where the new value fits. No experiment ran a
`CONFIG SET maxclients` beyond the limit with the module loaded.

At `maxclients 10000`:

```
fd number:  0 ........ 10032 ........ 10128 ..................... limit
            |------------ core range ------------|---- module range ----|
            |- Valkey + up to 10000 clients -|- 96 spare -|
                                  ^                       ^
                  limit if Valkey sizes it                module start (maxclients + 128)
```

## 5. Rules

### 5.1 At load

1. Raise the soft limit to the hard limit. Log it when it changes (`it was N`). Reason: with the soft limit
   at the hard limit, Valkey has nothing left to raise later, so it does not lower the hard limit on a
   later `CONFIG SET maxclients`. It also lets the module use every number the hard limit allows.
2. Compute the module start from the current `maxclients`, and `needed = module_start + maxclients`
   (`Strict`: `module_start + maxclients + max-cached-fds`).
3. If `limit < needed`:
   - `fd-reserve Lenient`: **placement off.** The cache is off. Log a notice, and a warning naming the fd
     limit that would fit when `max-cached-fds` is non-zero. The module loads. (Under `Lenient` `needed` is
     the reserve alone, so a cache that does not fit is not a reason to turn placement off: see step 4.)
   - `fd-reserve Strict`: **refuse to load**, with an error that names the fix: "set LimitNOFILE to at
     least `needed`, lower maxclients or max-cached-fds, or use fd-reserve Lenient".
4. Otherwise: **placement on.** Verify once that the module start is really past Valkey's event loop (see
   5.7); if not, fail to load. Then, if `max-cached-fds` is larger than what the fd limit allows (`Lenient`
   only; `Strict` has just checked it fits), log a warning with the fd cache max entries and the fd limit
   that would fit `max-cached-fds`. Under `Strict`, also log the highest `maxclients` a later `CONFIG SET`
   may reach without aborting: `(limit - 128 - max-cached-fds) / 2`.

Loading never fails because the fd limit is too low, except under `fd-reserve Strict`.

### 5.2 Opening an object file

With placement on: `open()` the file, copy the fd to the lowest free number in the module range
(`F_DUPFD_CLOEXEC`), close the low one.

If no number in the module range is free, close the low fd, count the failure, and return the error
(`EMFILE`) to the caller. The caller turns it into an error reply: a SET reports an I/O error, a GET
reports an NVMe read error. The low fd is never kept, because keeping it would take a number in the core
range. This path never panics. With placement on, the module range has a number for every client's command,
so it is reached only if something else takes those numbers or the fd limit changes from outside the
server; it fails the command, not the server.

For a short moment, between `open()` and the close of the low fd, the file holds a number in the core
range. The 96 spare numbers are meant to absorb this; the number of opens that can overlap there is not
bounded (see section 9, open item 2).

COPY opens its two files directly and closes them before returning to the event loop. It runs on the main
thread, so no client connects while they are open, and they need no move.

With placement off, `open()` is used as is.

### 5.3 The fd cache and the command reserve

Everything the module holds open lives in the module range. Two things share it: the cache, and the
command fds (a SET's write fd, or the read fd of a GET that missed the cache).

```
  0 ............. X ................................. Y ................................. Z
  |  core range   |<-------------------------- module range ----------------------------->|
  |  (Valkey,     |<--- fd cache, at most P --->|<--- command reserve, >= maxclients free --->|
  |   clients)    |  (a full cache sits here)   |  (running SETs and GETs take numbers here)  |

  X = module start = maxclients + 128
  P = fd cache max entries = min(max-cached-fds, Z - X - maxclients)
  Y = X + P, never above Z - maxclients
  Z = fd limit = the soft open-file limit; the kernel hands out fd numbers 0 .. Z-1
```

The two do not get separate ranges. Both take the lowest free number in the module range, so a full cache
sits right at the module start and command fds take the numbers after it. What is guaranteed is a count:
the cache is limited so that at least `maxclients` numbers in the module range stay free for command fds,
however large the cache grows.

```
fd cache max entries = min(max-cached-fds, max(0, limit - module_start - maxclients))
```

Example: `maxclients 100`, module start 228, fd limit 1000. The module range has 772 numbers. 100 are the
command reserve, so the fd limit allows 672 cached fds. A full fd cache of 672 still leaves 100 numbers
for command fds. With `max-cached-fds 700` the fd cache max entries is 672.

Why `maxclients`: each client runs one command at a time, so at most `maxclients` commands hold an fd at
once. These are numbers in the module range, not client sockets: the core range (`0 .. module_start-1`)
holds the sockets.

Consequences:
- The command reserve is the requirement: `limit >= module_start + maxclients`. If it does not hold,
  placement is off (`Lenient`) or the module does not load (`Strict`), so there is no cache.
- The cache gets what is left above the reserve. An fd cache of P entries needs
  `limit >= module_start + maxclients + P`. `Lenient` clamps P to what fits; `Strict` also does not load
  if the whole `max-cached-fds` does not fit.

The reserve relies on a client holding at most one command fd at a time. A GET drops its fd before its
client is unblocked, and a SET's write fd closes before its client is unblocked, so a client's next command
cannot overlap its last one.

With placement off the fd cache max entries is 0: a cached fd would hold one of the numbers clients need for good.

`max-cached-fds 0` means no cache: open, read, close.

### 5.4 `CONFIG SET max-cached-fds`

`max-cached-fds` is a request under `Lenient`, not a promise: the fd cache max entries is the smaller of it
and what the fd limit allows. A larger value is accepted, as it is at load and after a `maxclients` raise.
The module logs a warning with the fd cache max entries and the fd limit that would fit; `CONFIG GET` still
shows the configured value, and `INFO` `fd_cache_max_entries` shows what applies. The fd cache is trimmed to
the new maximum at once, coldest fds first. An fd that a GET is still reading stays open until that GET
finishes. With placement off, any value is accepted (the fd cache is off) and a warning is logged.

Under `Strict` it is a promise: a value that does not fit (`limit < module_start + maxclients + value`) is
rejected with an error that names the fd limit that would fit, and the old value stays. The module can say
no here, which it cannot for `maxclients`.

### 5.5 `CONFIG SET maxclients`

Valkey applies the change before the module sees it; the module cannot veto it. The module then:

1. Recomputes the module start (it only rises) and re-evaluates the rules in 5.1.
2. Drops cached fds numbered below the new module start. Their next GET reopens them in the module range.
3. Trims the fd cache to the new fd cache max entries.

If the fd limit no longer covers the command reserve (`limit < module_start + maxclients`; legal, because
Valkey accepts any `maxclients` up to `limit - 32`):
- `fd-reserve Lenient`: placement turns off, the cache is emptied, and a notice is logged. The server keeps
  serving. Placement stays off until restart: the module start never falls, and an ordinary user's fd
  limit does not rise.
- `fd-reserve Strict`: the server aborts, with a fatal message that names the fix. It aborts both when the
  reserve no longer fits and when only `max-cached-fds` no longer does (`limit < module_start + maxclients +
  max-cached-fds`): Strict never runs clamped. Under `Strict` the highest safe `maxclients` is
  `(limit - 128 - max-cached-fds) / 2`, and the load log states it. `CONFIG REWRITE` after such
  a raise would make the next start refuse to load until the fd limit is raised.

### 5.6 The fd limit is read when used

The module reads the fd limit when it works out the fd cache max entries or opens a file. It does not watch for changes while
idle. Valkey itself changes the fd limit only on `CONFIG SET maxclients`, which 5.5 handles. An fd limit changed
from outside the server (for example `prlimit` on the running process) is not acted on; a command that
then finds no free fd fails as in 5.2.

### 5.7 Startup probe

The module start is a copy of Valkey's `maxclients + 128`. At load the module checks that the copy is
right: it places a probe eventfd at the module start and asks the event loop to watch it. Valkey answers `ERANGE`
exactly when the number is past the loop; any other answer fails the load. This runs once, because the
loop is always `maxclients + 128` wide, whatever `maxclients` later becomes.

## 6. Examples

`maxclients 10000`, module start 10128, default `max-cached-fds` 16384:

| fd limit | zone | `Lenient` | `Strict` |
|---|---|---|---|
| 10032 (Valkey sized it itself) | empty | loads, placement off, cache 0 | refuses to load |
| 10240 | partial (112 numbers, below `maxclients`) | loads, placement off, cache 0 | refuses to load |
| 25000 | full, cache does not fit | loads, placement on, cache clamped to 4872 | refuses to load (needs 36512) |
| 65536 | full | loads, placement on, cache 16384 | loads, cache 16384 |

Zones: *empty* is `limit <= module_start`, *partial* is `module_start < limit < module_start + maxclients`,
*full* is `limit >= module_start + maxclients` (20128 at the defaults). `Strict` needs the cache too:
`limit >= module_start + maxclients + max-cached-fds` (36512 at the defaults). Running Valkey with a higher fd limit
is an operator action: Valkey fixes the fd limit before the module loads.

## 7. Operator surface

| Item | Behavior |
|---|---|
| `max-cached-fds` | 0 to 1048576, default 16384, mutable. 0 disables the cache. Under `Lenient` a value that does not fit is clamped with a warning; under `Strict` it must fit: load refuses, `CONFIG SET` is rejected. |
| `fd-reserve` | `Lenient` (default) or `Strict`, immutable. What happens when the fd limit does not cover the command reserve (`Strict`: plus `max-cached-fds`). Behavior per event: decision 1. |
| `INFO largeobj_fd: open_object_fds` | Object files open now: cached fds, plus the fds of running GETs and SETs (a GET's fd stays counted after the cache evicts it). |
| `INFO largeobj_fd: fd_placement_enabled` | 1 when placement is on, 0 when the fd limit does not cover the command reserve (`Lenient` only; `Strict` never runs with it off). |
| `INFO largeobj_fd: fd_placement_failures` | Opens that found no free number in the module range. |
| `INFO largeobj_fd: fd_cache_max_entries` | The most fds the fd cache holds: `max-cached-fds`, or less when the fd limit allows less. |
| `INFO largeobj_fd: fd_cache_entries` | Fds in the cache now. |
| Log | One warning per 1024 placement failures, naming the module start and the error. One warning when `max-cached-fds` does not fit, with the fd limit that would. A notice when the soft limit is raised, and when placement is off. |

To raise the fd limit: set `LimitNOFILE` in the unit that starts Valkey (systemd drop-in), `--ulimit nofile=`
for Docker, or `ulimit -n` before `valkey-server`. Then restart.

## 8. Alternatives considered

- **Refusing to load for a low fd limit** is the `Strict` policy in decision 1, not repeated here.
- **Panic when no fd is free.** With placement on the condition needs something outside the reserve (other
  module fds, an fd limit changed from outside), so it is not an invariant the module can promise. A panic
  takes down the whole server, cache included. `Strict` aborts only for a breached reserve, which it can
  state exactly.
- **Evict cached fds when a SET cannot get a number.** Not reliable: an fd that a GET is still reading
  stays open after eviction, so its number is not freed at the moment it is needed.
- **A fixed command reserve of a few numbers.** Nothing bounds how many commands run at once, so a fixed
  number has no basis.

## 9. Decisions and open items for review

### Decisions

**1. What happens when the fd limit does not cover the command reserve?** A policy, `fd-reserve`.

The fd limit falls into three zones. `M` is `maxclients`.

- **Empty:** `limit <= module_start`. The module range has no numbers. Stock Valkey is here.
- **Partial:** `module_start < limit < module_start + M`. The module range has some numbers, but fewer
  than `M`.
- **Full:** `limit >= module_start + M`. The module range covers the command reserve.

| `fd-reserve` | Empty | Partial | Full, cache short | Full, cache fits | `CONFIG SET maxclients` breaks the reserve | `CONFIG SET maxclients` squeezes only the cache | `CONFIG SET max-cached-fds` too large |
|---|---|---|---|---|---|---|---|
| **Lenient** (default) | load, placement off, cache off | load, placement off, cache off | load, placement on, cache clamped | load | placement off, cache emptied, warning | cache clamped, warning | accepted, clamped, warning |
| **Strict** | refuse | refuse | refuse | load | abort the server | abort the server | rejected, error names the limit |

Both settings are bad in the partial zone, which is why placement is on only in the full zone. Placement on
makes commands fail: with an fd limit of 10129 and `M = 10000` the module range has one number, so the
second concurrent command fails. Placement off puts command fds back in the core range, while the kernel
can still hand out numbers past the event loop, so at near-saturation a client can land there and hang,
unserved. An overloaded server in the empty zone only gets a clean `EMFILE`, because no number past the
loop exists.

- **Lenient.** Valkey's habit: degrade, warn, keep serving. Stock Valkey loads Tiered mode out of the box.
  Cost: in the partial zone a client can hang silently when connected clients plus running commands exceed
  `M + 96`. That needs near-saturation, for example about 100 concurrent commands with the client count
  within 100 of `maxclients`. The state is visible in `INFO`: `fd_placement_enabled` 0, `fd_cache_max_entries` 0.
- **Strict.** Never degraded: it either runs with the full command reserve and the whole `max-cached-fds`, or
  does not run. "Strict" means fail-stop: the operator chose it, and `max-cached-fds` is an explicit setting
  that must be honored, so a cache that does not fit is refused at load, rejected on `CONFIG SET`, or aborts
  the server, never clamped. Cost: stock
  Valkey cannot load Tiered mode until `LimitNOFILE` is at least `2 x maxclients + 128 + max-cached-fds`
  (36512 at the defaults). A `CONFIG SET maxclients` above `(limit - 128 - max-cached-fds) / 2` aborts the
  server, which drops every client and every key, and any client allowed to run `CONFIG SET` can trigger it.
  If the raise was saved with `CONFIG REWRITE`, the next start refuses to load.

Rejected shapes of the same decision:
- **Placement on in the partial zone** (load, commands fail past the size of the module range). Closest to
  Valkey's habit of warning and carrying on, but the zone is nearly unusable at its low end, and failing
  commands are no better than the hang they avoid.
- **Refuse only when empty.** Stops stock Valkey from loading Tiered mode, and still runs the partial zone.
- **Refuse only when partial.** Stock Valkey loads and a well-sized fd limit loads, but a partial zone that
  is one step above stock is refused while stock itself is not. `Strict` and `Lenient` are the two
  consistent ends of that.

**2. Raise the soft limit to the hard limit at load.**

| | Behavior |
|---|---|
| **A. Raise** (chosen) | Logged. Stops Valkey lowering the hard limit on a later `CONFIG SET maxclients`. Goes further than Valkey, which raises only as far as it needs. |
| B. Leave the fd limit alone | Respects a soft limit the operator set on purpose. A later `maxclients` raise can make Valkey lower the hard limit. |

**3. How large is the command reserve?**

```
module start                                              fd limit
   |<-- cache, up to P -->|<-- command reserve, kept free -->|
```

The fd cache is limited so it never takes the command reserve: what the fd limit allows is `limit - module_start - reserve`. The
reserve is how many numbers stay free for the fds of running GETs and SETs. The question is how many.

| | Reserve | What it gives | Cost |
|---|---|---|---|
| **A. `maxclients`** (chosen) | `maxclients` numbers | Each client runs one command at a time, so every client can have a command running at once and none fails for lack of an fd. | An fd cache of `P` entries needs `limit >= module_start + maxclients + P`: `20128 + P` at `maxclients 10000`. |
| B. A configured limit on commands in flight, `N` | `N` numbers | An fd cache of `P` entries needs `limit >= module_start + N + P`, for example `10384 + P` with `N = 256`. Commands past `N` wait. | A new config, a wait mechanism, and a SET/mixed benchmark to choose `N`. |

The reserve is a precondition for placement: below it, `Lenient` runs with placement and the cache off, and
`Strict` does not run. With the reserve covered, commands cannot run out of module-range numbers, and the
fd cache is what pays for that guarantee.

How far `maxclients` is from reality depends on object size. In the cached-GET benchmark at 1 MB with 750
clients, about 717 commands were in flight at once, nearly every client, so A is not wasteful there. At
4 KB it was about 73.

### Open items

1. **Other module fds are not in the budget.** The fd cache max entries counts object-file fds only. The io_uring
   ring, EFA/libfabric and any sockets are not counted; what the module holds there has not been checked.
2. **The short-lived low fd in 5.2** is not bounded. If many opens overlap, they can exhaust the 96 spare
   numbers in the core range.
3. **One command per client** is a property of how the module blocks clients, not something it enforces.
   The fd cache max entries depends on it.
4. **A Valkey core change** would remove most of this: a way for a module to reserve fds against the
   `maxclients` budget, or core not lowering the hard limit. Out of scope here.

## 10. Verification

Experiments against a real `valkey-server`, `maxclients 10000` unless noted:

| Launch | Result |
|---|---|
| soft 1024, hard 65536, no module | After start: soft 10032, hard 10032 |
| same, then `CONFIG SET maxclients 20000` | Rejected; limits and `maxclients` unchanged |
| soft 20000, hard 65536, no module, then `CONFIG SET maxclients 30000` | soft 30032, hard 30032 |
| same launch with the module loaded | soft and hard 65536 before and after the same `CONFIG SET` |

The last row held in three repeats. One earlier run showed a different reading that could not be reproduced.

Tests:

- unit: a failed move returns an error, counts, and does not panic; the fd limit allowance for the fd cache leaves
  the command reserve; `max-cached-fds 0` opens without caching; trimming evicts down to the new size at once.
- integration:
  - a limit inside the core range, or in the partial zone, loads under `Lenient` with placement and the
    cache off;
  - a `max-cached-fds` larger than the fd limit allows is clamped at load, accepted and logged on
    `CONFIG SET`, `CONFIG GET` still shows the configured value, and `fd_cache_max_entries` shows the
    clamp;
  - `CONFIG SET maxclients` that breaks the reserve turns placement off and empties the cache under
    `Lenient`, and the server keeps serving;
  - `Strict` refuses to load below the reserve, refuses to load when only the cache does not fit, aborts on
    a `CONFIG SET maxclients` that breaks the reserve and on one that squeezes only the cache, and rejects a
    `CONFIG SET max-cached-fds` that does not fit (keeping the old value);
  - a command that finds no free fd in the module range fails with an error, is counted in
    `fd_placement_failures`, and the server survives and serves the next command.
- Mutation checks: removing the clamp fails 3 tests, making a failed move panic fails the
  failed-move test, disabling the `Strict` refusal fails both `Strict` load tests, leaving the cache out of the
  `Strict` check fails 3 `Strict` tests (load, `maxclients` squeeze, `CONFIG SET`), and removing the
  `max-cached-fds` validator fails the rejection test.

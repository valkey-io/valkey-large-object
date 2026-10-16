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
| **placement** | Moving each object-file fd into the module range right after it is opened. *On* when the module range is not empty, *off* otherwise. |
| **cache** | Read fds kept open between GETs. An optimization, never required. |
| **cache cap** (`cap`) | The `max-cached-fds` setting: how large the operator wants the cache. |
| **cache room** | `max(0, limit - module_start - maxclients)`: what the module range holds after the command reserve. |
| **cache size** | `min(cap, cache room)`: what the cache actually holds at most. |

## 3. Behavior in one paragraph

At load, the module turns placement on: every object-file fd is moved into the module range, so it never
takes a number a client needs. If the fd limit ends inside the core range the module range is empty;
placement is then off, fds stay where `open()` put them, and the cache is off. With placement on, the cache
is sized from what the module range holds after the command reserve (diagram in 5.3). A shortage is logged,
never fatal: a command that finds no free fd fails with an error reply and the server keeps running.

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
2. Compute the module start from the current `maxclients`.
3. If `limit <= module_start`: **placement off.** The cache is off. Log a notice, and a warning naming the
   fd limit that would fit when `max-cached-fds` is non-zero.
4. Otherwise: **placement on.** Verify once that the module start is really past Valkey's event loop (see
   5.7); if not, fail to load. Then:
   - if `limit < module_start + maxclients`, log a warning: commands fail with an error when more than
     `limit - module_start` run at once, and the fd limit that would cover the command reserve;
   - if the cap is larger than the cache room, log a warning with the cache size in use and the fd limit
     that would fit the cap.

Loading never fails because the fd limit is too low.

### 5.2 Opening an object file

With placement on: `open()` the file, copy the fd to the lowest free number in the module range
(`F_DUPFD_CLOEXEC`), close the low one.

If no number in the module range is free, close the low fd, count the failure, and return the error
(`EMFILE`) to the caller. The caller turns it into an error reply: a SET reports an I/O error, a GET
reports an NVMe read error. The low fd is never kept, because keeping it would take a number in the core
range. This path never panics: it is reachable in normal operation, when more commands run at once than
the module range has numbers, or after a `maxclients` raise.

For a short moment, between `open()` and the close of the low fd, the file holds a number in the core
range. The 96 spare numbers are meant to absorb this; the number of opens that can overlap there is not
bounded (see section 9, open item 2).

COPY opens its two files directly and closes them before returning to the event loop. It runs on the main
thread, so no client connects while they are open, and they need no move.

With placement off, `open()` is used as is.

### 5.3 Cache size

Everything the module holds open lives in the module range. Two things share it: the cache, and the
command fds (a SET's write fd, or the read fd of a GET that missed the cache).

```
  0 .............. X ............................ Y ............................ Z
  |  core range   |<---------------------- module range -------------------------------->|
  |  (Valkey,     |<------ cache, at most P ---->|<-- command reserve: >= maxclients free -->|
  |   clients)    |  (a full cache sits here:     |  (running SETs and GETs                   |
  |               |   lowest numbers above X)     |   take numbers from here)                 |

  X = module start = maxclients + 128
  P = cache size = min(max-cached-fds, Z - X - maxclients)
  Y = X + P, never above Z - maxclients
  Z = fd limit = the soft open-file limit; the kernel hands out fd numbers 0 .. Z-1
```

The two do not get separate ranges. Both take the lowest free number in the module range, so a full cache
sits right at the module start and command fds take the numbers after it. What is guaranteed is a count:
the cache is limited so that at least `maxclients` numbers in the module range stay free for command fds,
however large the cache grows.

```
cache room = max(0, limit - module_start - maxclients)
cache size = min(max-cached-fds, cache room)
```

Example: `maxclients 100`, module start 228, fd limit 1000. The module range has 772 numbers. 100 are the
command reserve, so the cache holds at most 672. A full cache of 672 still leaves 100 numbers for command
fds. With `max-cached-fds 700` the cache is cut to 672.

Why `maxclients`: each client runs one command at a time, so at most `maxclients` commands hold an fd at
once. These are numbers in the module range, not client sockets: the core range (`0 .. module_start-1`)
holds the sockets.

Consequences:
- A cache of size P needs `limit >= module_start + P + maxclients`.
- If `limit - module_start < maxclients`, the cache room is 0 and the cache is off. Commands can still
  fail when more than `limit - module_start` of them run at once.

The reserve relies on a client holding at most one command fd at a time. A GET drops its fd before its
client is unblocked, and a SET's write fd closes before its client is unblocked, so a client's next command
cannot overlap its last one.

With placement off the cache room is 0: a cached fd would hold one of the numbers clients need for good.

`max-cached-fds 0` means no cache: open, read, close.

### 5.4 `CONFIG SET max-cached-fds`

- With placement on and the new cap larger than the cache room: **reject**, with an error that names the
  fd limit to set (`module_start + cap + maxclients`).
- Otherwise accept. The cache is trimmed to the new cache size at once, coldest fds first. An fd that a
  GET is still reading stays open until that GET finishes.
- With placement off, any value is accepted (the cache is off) and a warning is logged.

### 5.5 `CONFIG SET maxclients`

Valkey applies the change before the module sees it; the module cannot veto it. The module then:

1. Recomputes the module start (it only rises) and re-evaluates the rules in 5.1.
2. Drops cached fds numbered below the new module start. Their next GET reopens them in the module range.
3. Trims the cache to the new cache size.

If the new module start is at or past the fd limit (legal: Valkey accepts any `maxclients` up to
`limit - 32`, and the module start is 96 higher), placement turns off, the cache is emptied, and a notice
is logged. The server keeps serving. Placement stays off until restart, because the module start never
falls.

### 5.6 The fd limit is read when used

The module reads the fd limit when it sizes the cache or opens a file. It does not watch for changes while
idle. Valkey itself changes the fd limit only on `CONFIG SET maxclients`, which 5.5 handles. An fd limit changed
from outside the server (for example `prlimit` on the running process) is not acted on; a command that
then finds no free fd fails as in 5.2.

### 5.7 Startup probe

The module start is a copy of Valkey's `maxclients + 128`. At load the module checks that the copy is
right: it places a probe eventfd at the module start and asks the event loop to watch it. Valkey answers `ERANGE`
exactly when the number is past the loop; any other answer fails the load. This runs once, because the
loop is always `maxclients + 128` wide, whatever `maxclients` later becomes.

## 6. Examples

`maxclients 10000`, module start 10128, default cap 16384:

| fd limit | placement | module range | cache room | cache size | commands |
|---|---|---|---|---|---|
| 10032 (Valkey sized it itself) | off | empty | 0 | 0 | fds share numbers with clients; at full load a command fails or Valkey refuses a client |
| 10240 | on | 112 | 0 | 0 | the 113th concurrent command fails with an error |
| 25000 | on | 14872 | 4872 | 4872 | every client has a command-reserve number |
| 65536 | on | 55408 | 45408 | 16384 | every client has a command-reserve number |

For Tiered mode to cover the full command reserve and have any cache, the fd limit must exceed
`module_start + maxclients` (20128 at the defaults). Running Valkey with a higher fd limit is an operator
action: Valkey fixes the fd limit before the module loads.

## 7. Operator surface

| Item | Behavior |
|---|---|
| `max-cached-fds` | 0 to 1048576, default 16384, mutable. 0 disables the cache. |
| `INFO largeobj_fd: open_object_fds` | Object files open now: cached fds, plus the fds of running GETs and SETs (a GET's fd stays counted after the cache evicts it). |
| `INFO largeobj_fd: fd_placement_enabled` | 1 when placement is on, 0 when the fd limit ends inside the core range. |
| `INFO largeobj_fd: fd_placement_failures` | Opens that found no free number in the module range. |
| `INFO largeobj_fd: fd_cache_size` | The cache size in use. Can be below `max-cached-fds`. |
| `INFO largeobj_fd: fd_cache_entries` | Fds in the cache now. |
| Log | One warning per 1024 placement failures, naming the module start and the error. One warning when the cap does not fit, with the fd limit that would. A notice when the soft limit is raised, and when placement is off. |

To raise the fd limit: set `LimitNOFILE` in the unit that starts Valkey (systemd drop-in), `--ulimit nofile=`
for Docker, or `ulimit -n` before `valkey-server`. Then restart.

## 8. Alternatives considered

- **Refuse to load unless the module range is not empty** (`limit > module_start`). One mode instead of
  two, no contention with clients, and stock Valkey cannot use Tiered mode until `LimitNOFILE` is raised.
  Not chosen: see decision 1.
- **Refuse to load unless the module range covers the command reserve** (`limit >= module_start +
  maxclients`). Commands never fail for lack of an fd. Needs `2 x maxclients + 128` before there is any
  cache, even with the cache off.
- **Panic when no fd is free.** The condition is reachable in normal operation (more concurrent commands
  than module-range numbers, a later `maxclients` raise), so it is not an invariant. A panic takes down the
  whole server, cache included.
- **Evict cached fds when a SET cannot get a number.** Not reliable: an fd that a GET is still reading
  stays open after eviction, so its number is not freed at the moment it is needed.
- **A fixed command reserve of a few numbers.** Nothing bounds how many commands run at once, so a fixed
  number has no basis.

## 9. Decisions and open items for review

### Decisions (the built behavior is marked)

**1. Should Tiered mode load when the fd limit does not cover the command reserve?**

| | Behavior | Cost |
|---|---|---|
| **A. Always load** (built) | Loads at any fd limit. With an empty module range, placement is off; with a module range smaller than `maxclients`, commands fail past its size. | With an empty module range, module fds and clients compete for numbers. The module cannot prevent this. |
| B. Load only if `limit > module_start` | Refuses when the module range is empty, loads otherwise. | Stock Valkey needs `LimitNOFILE` above 10128 first. |
| C. Load only if `limit >= module_start + maxclients` | Refuses unless the module range covers the command reserve. | Stock Valkey needs 20128 first. Commands never fail for lack of an fd. |

A is closest to how Valkey treats a low fd limit: it lowers what it offers (`maxclients`), warns, and exits
only when it cannot run at all.

**2. `CONFIG SET max-cached-fds` larger than the cache room.**

| | Behavior |
|---|---|
| **A. Reject** (built) | The command fails with an error naming the fd limit to set. Matches how Valkey rejects a `maxclients` it cannot honor. |
| B. Clamp and warn | The command succeeds, the cache is smaller than asked, `CONFIG GET` shows the larger value. |

At load the same condition is clamped with a warning, since a failed load for a performance setting would
conflict with decision 1A. The two places therefore differ on purpose.

**3. Raise the soft limit to the hard limit at load.**

| | Behavior |
|---|---|
| **A. Raise** (built) | Logged. Stops Valkey lowering the hard limit on a later `CONFIG SET maxclients`. Goes further than Valkey, which raises only as far as it needs. |
| B. Leave the fd limit alone | Respects a soft limit the operator set on purpose. A later `maxclients` raise can make Valkey lower the hard limit. |

**4. Size of the command reserve.**

| | Behavior |
|---|---|
| **A. `maxclients`** (built) | Never short of numbers when `limit >= module_start + maxclients`. Large: 20128 at the defaults before there is any cache. |
| B. A configured limit on commands in flight | Smaller reserve (`module_start + inflight + cap`). Commands past the limit wait. Needs a new config, a wait mechanism and a SET/mixed benchmark to choose the number. |

Large objects at saturation keep nearly every client inside a command (from the cached-GET benchmark:
about 717 of 750 clients at 1 MB), so A is not pessimistic there.

### Open items

1. **Other module fds are not in the budget.** The cache room counts object-file fds only. The io_uring
   ring, EFA/libfabric and any sockets are not counted; what the module holds there has not been checked.
2. **The short-lived low fd in 5.2** is not bounded. If many opens overlap, they can exhaust the 96 spare
   numbers in the core range.
3. **One command per client** is a property of how the module blocks clients, not something it enforces.
   The cache room depends on it.
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

- unit: a failed move returns an error, counts, and does not panic; the cache room leaves the command
  reserve; `max-cached-fds 0` opens without caching; trimming
  evicts down to the cap at once.
- integration: a limit inside the core range loads with the cache off; a cap larger than the room loads
  with the clamped cache size; `CONFIG SET maxclients` with no room empties the cache and the server keeps
  serving; a command that finds no free fd in the module range fails with an error, is counted in
  `fd_placement_failures`, and the server survives and serves the next command.
- Two mutation checks: removing the cache-room clamp fails 3 tests, and making a failed move panic fails
  the failed-move test.

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

A client can only be stranded if the process is allowed fd numbers past that range. If the open-file limit
ends inside the range, there is nowhere to be stranded to, but also nowhere for the module to move its fds.

## 2. Terms

| Term | Meaning |
|---|---|
| **limit** | The process's soft `RLIMIT_NOFILE`. The kernel never hands out an fd number at or above it. |
| **core range** | fd numbers `0 .. maxclients + 127`: Valkey's own fds (32 reserved), up to `maxclients` client sockets, and 96 spare. |
| **floor** | `maxclients + 128`, the first number after the core range. It uses the highest `maxclients` ever set, so it only rises. |
| **operation fd** | An fd held while one command runs: a SET's write fd, or the read fd of a GET that missed the cache. Each client runs one command at a time, so at most `maxclients` operation fds are open together. |
| **operation slot** | An fd number at or above the floor. There are `limit - floor` of them. |
| **cache** | Read fds kept open between GETs. An optimization, never required. |
| **cap** | The `max-cached-fds` setting: how large the operator wants the cache. |
| **cache room** | `max(0, limit - floor - maxclients)`: the operation slots left after reserving one per client for commands. |
| **cache size** | `min(cap, cache room)`: what the cache actually holds at most. |

## 3. Behavior in one paragraph

At load, the module moves every object-file fd to a number at or above the floor, so it never takes a
number a client needs. If the limit ends inside the core range there are no such numbers; the module then
leaves fds where `open()` put them and turns the cache off. The cache is sized from what the limit leaves
after one slot per client. A shortage is logged, never fatal: a command that finds no free fd fails with an
error reply and the server keeps running.

## 4. Where the limit comes from

The module does not choose the limit. Valkey does, before the module loads.

- At startup and on every `CONFIG SET maxclients`, Valkey checks that the limit is at least
  `maxclients + 32`. If not, it sets **both** the soft and the hard limit to exactly `maxclients + 32`.
  If the limit is already high enough, Valkey leaves it alone.
- So a low launch limit (systemd's default of 1024, or `LimitNOFILE=10032`) ends at `maxclients + 32`.
  That is below the floor (`maxclients + 128`): no number past the core range exists.
- A high launch limit (for example `LimitNOFILE=65536`) is left alone, so numbers past the core range exist.
- After Valkey has lowered the limit, a `CONFIG SET maxclients` to a larger value is rejected: Valkey would
  have to raise the hard limit, which needs root, and the Valkey process normally is not root. The sender
  of the command is irrelevant.
- Valkey also lowers the **hard** limit on a later `CONFIG SET maxclients` that needs a higher soft limit
  than the current one (65536 hard became 30032 in one run).

These were checked on a real server, not only read from the code (see section 10).

At `maxclients 10000`:

```
fd number:  0 ........ 10032 ........ 10128 ..................... limit
            |- Valkey + up to 10000 clients -|- 96 spare -|- operation slots -|
                                  ^                       ^
                  limit if Valkey sizes it                floor (maxclients + 128)
```

## 5. Rules

### 5.1 At load

1. Raise the soft limit to the hard limit. Log it when it changes (`it was N`). Reason: with the soft limit
   at the hard limit, Valkey has nothing left to raise later, so it does not lower the hard limit on a
   later `CONFIG SET maxclients`. It also lets the module use every number the hard limit allows.
2. Compute the floor from the current `maxclients`.
3. If `limit <= floor`: **do not move fds.** The cache is off. Log a notice, and a warning naming the
   limit that would fit when `max-cached-fds` is non-zero.
4. Otherwise: **move fds.** Verify once that the floor is really past Valkey's event loop (see 5.7); if
   not, fail to load. Then:
   - if `limit < floor + maxclients`, log a warning: commands fail with an error when more than
     `limit - floor` run at once, and the limit that would give every client a slot;
   - if the cap is larger than the cache room, log a warning with the cache size in use and the limit
     that would fit the cap.

Loading never fails because the limit is too low.

### 5.2 Opening an object file

When fds are moved: `open()` the file, copy the fd to the lowest free number at or above the floor
(`F_DUPFD_CLOEXEC`), close the low one.

If no number at or above the floor is free, close the low fd, count the failure, and return the error
(`EMFILE`) to the caller. The caller turns it into an error reply: a SET reports an I/O error, a GET
reports an NVMe read error. The low fd is never kept, because keeping it would take a number in the core
range. This path never panics: it is reachable in normal operation, when more commands run at once than
there are slots, or after a `maxclients` raise.

For a short moment, between `open()` and the close of the low fd, the file holds a number in the core
range. The 96 spare numbers are meant to absorb this; the number of opens that can overlap there is not
bounded (see section 9, open item 2).

COPY opens its two files directly and closes them before returning to the event loop. It runs on the main
thread, so no client connects while they are open, and they need no move.

When fds are not moved, `open()` is used as is.

### 5.3 Cache size

```
cache room = max(0, limit - floor - maxclients)
cache size = min(max-cached-fds, cache room)
```

The `- maxclients` term is a separate set of numbers from the `floor` term. `floor` counts the core range
(`0 .. floor-1`); `maxclients` counts operation slots above it, one per client that may be running a
command. The cache gets only what is left after those are set aside. Without that, a full cache could use
every number above the floor and the next SET could not open its write fd.

The bound relies on a client holding at most one operation fd at a time. A GET drops its fd before its
client is unblocked, and a SET's write fd closes before its client is unblocked, so a client's next command
cannot overlap its last one.

When fds are not moved the cache room is 0: a cached fd would hold one of the numbers clients need for good.

`max-cached-fds 0` means no cache: open, read, close.

### 5.4 `CONFIG SET max-cached-fds`

- When fds are moved and the new cap is larger than the cache room: **reject**, with an error that names
  the limit to set (`floor + cap + maxclients`).
- Otherwise accept. The cache is trimmed to the new cache size at once, coldest fds first. An fd that a
  GET is still reading stays open until that GET finishes.
- When fds are not moved, any value is accepted (the cache is off) and a warning is logged.

### 5.5 `CONFIG SET maxclients`

Valkey applies the change before the module sees it; the module cannot veto it. The module then:

1. Recomputes the floor (it only rises) and re-evaluates the rules in 5.1.
2. Drops cached fds numbered below the new floor. Their next GET reopens them above it.
3. Trims the cache to the new cache size.

If the new floor is at or past the limit (legal: Valkey accepts any `maxclients` up to `limit - 32`, and
the floor is 96 higher), fds stop being moved, the cache is emptied, and a notice is logged. The server
keeps serving. fds stay unmoved until restart, because the floor never falls.

### 5.6 The limit is read when used

The module reads the limit when it sizes the cache or opens a file. It does not watch for changes while
idle. Valkey itself changes the limit only on `CONFIG SET maxclients`, which 5.5 handles. A limit changed
from outside the server (for example `prlimit` on the running process) is not acted on; a command that
then finds no free fd fails as in 5.2.

### 5.7 Startup probe

The floor is a copy of Valkey's `maxclients + 128`. At load the module checks that the copy is right:
it places a probe eventfd at the floor and asks the event loop to watch it. Valkey answers `ERANGE`
exactly when the number is past the loop; any other answer fails the load. This runs once, because the
loop is always `maxclients + 128` wide, whatever `maxclients` later becomes.

## 6. Examples

`maxclients 10000`, floor 10128, default cap 16384:

| limit | fds moved | operation slots | cache room | cache size | commands |
|---|---|---|---|---|---|
| 10032 (Valkey sized it itself) | no | none | 0 | 0 | fds share numbers with clients; at full load a command fails or Valkey refuses a client |
| 10240 | yes | 112 | 0 | 0 | the 113th concurrent command fails with an error |
| 25000 | yes | 14872 | 4872 | 4872 | a slot for every client |
| 65536 | yes | 55408 | 45408 | 16384 | a slot for every client |

For Tiered mode to have the full guarantee and any cache, the limit must exceed `floor + maxclients`
(20128 at the defaults). Running Valkey with a higher limit is an operator action: Valkey fixes the limit
before the module loads.

## 7. Operator surface

| Item | Behavior |
|---|---|
| `max-cached-fds` | 0 to 1048576, default 16384, mutable. 0 disables the cache. |
| `INFO largeobj_fd: open_object_fds` | Object files open now: cached fds, plus the fds of running GETs and SETs (a GET's fd stays counted after the cache evicts it). |
| `INFO largeobj_fd: fd_placement_failures` | Opens that found no free number above the floor. |
| `INFO largeobj_fd: fd_cache_capacity` | The cache size in use. Can be below `max-cached-fds`. |
| `INFO largeobj_fd: fd_cache_entries` | Fds in the cache now. |
| Log | One warning per 1024 placement failures, naming the floor and the error. One warning when the cap does not fit, with the limit that would. A notice when the soft limit is raised, and when fds are not moved. |

To raise the limit: set `LimitNOFILE` in the unit that starts Valkey (systemd drop-in), `--ulimit nofile=`
for Docker, or `ulimit -n` before `valkey-server`. Then restart.

## 8. Alternatives considered

- **Refuse to load unless the limit leaves operation slots** (`limit > floor`). One mode instead of two, no
  contention with clients, and stock Valkey cannot use Tiered mode until `LimitNOFILE` is raised. Not
  chosen: see decision 1.
- **Refuse to load unless every client has a slot** (`limit >= floor + maxclients`). Commands never fail
  for lack of an fd. Needs `2 x maxclients + 128` before there is any cache, even with the cache off.
- **Panic when no fd is free.** The condition is reachable in normal operation (more concurrent
  commands than slots, a later `maxclients` raise), so it is not an invariant. A panic takes down the whole server, cache included.
- **Evict cached fds when a SET cannot get a slot.** Not reliable: an fd that a GET is still reading stays
  open after eviction, so its number is not freed at the moment it is needed.
- **A fixed reserve of a few slots for commands.** Nothing bounds how many commands run at once, so a
  fixed number has no basis.

## 9. Decisions and open items for review

### Decisions (the built behavior is marked)

**1. Should Tiered mode load when the limit does not leave a slot for every client?**

| | Behavior | Cost |
|---|---|---|
| **A. Always load** (built) | Loads at any limit. With no slots above the floor, fds are not moved; with fewer slots than clients, commands fail past the slots. | Without slots above the floor, module fds and clients compete for numbers. The module cannot prevent this. |
| B. Load only if `limit > floor` | Refuses when no slot exists, loads otherwise. | Stock Valkey needs `LimitNOFILE` above 10128 first. |
| C. Load only if `limit >= floor + maxclients` | Refuses unless every client has a slot. | Stock Valkey needs 20128 first. Commands never fail for lack of an fd. |

A is closest to how Valkey treats a low limit: it lowers what it offers (`maxclients`), warns, and exits
only when it cannot run at all.

**2. `CONFIG SET max-cached-fds` larger than the cache room.**

| | Behavior |
|---|---|
| **A. Reject** (built) | The command fails with an error naming the limit to set. Matches how Valkey rejects a `maxclients` it cannot honor. |
| B. Clamp and warn | The command succeeds, the cache is smaller than asked, `CONFIG GET` shows the larger value. |

At load the same condition is clamped with a warning, since a failed load for a performance setting would
conflict with decision 1A. The two places therefore differ on purpose.

**3. Raise the soft limit to the hard limit at load.**

| | Behavior |
|---|---|
| **A. Raise** (built) | Logged. Stops Valkey lowering the hard limit on a later `CONFIG SET maxclients`. Goes further than Valkey, which raises only as far as it needs. |
| B. Leave the limit alone | Respects a soft limit the operator set on purpose. A later `maxclients` raise can make Valkey lower the hard limit. |

**4. Size of the reserve for commands.**

| | Behavior |
|---|---|
| **A. `maxclients`** (built) | Never short of slots when `limit >= floor + maxclients`. Large: 20128 at the defaults before there is any cache. |
| B. A configured limit on commands in flight | Smaller reserve (`floor + inflight + cap`). Commands past the limit wait. Needs a new config, a wait mechanism and a SET/mixed benchmark to choose the number. |

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

- unit: a moved fd lands at or above the floor; a failed move returns an error, counts, and does not
  panic; the cache room leaves one slot per client; `max-cached-fds 0` opens without caching; trimming
  evicts down to the cap at once.
- integration: a limit inside the core range loads with the cache off; a cap larger than the room loads
  with the clamped cache size; `CONFIG SET maxclients` with no room empties the cache and the server keeps
  serving; a command that finds no free fd above the floor fails with an error, is counted in
  `fd_placement_failures`, and the server survives and serves the next command.
- Two mutation checks: removing the cache-room clamp fails 3 tests, and making a failed move panic fails
  the failed-move test.

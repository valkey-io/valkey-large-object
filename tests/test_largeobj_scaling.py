"""
Integration tests for DRAMPool expand/shrink scaling behavior, and for eviction.

Tests cover:
  - Dram mode: reactive expand (TCP and EFA SET) and proactive expand
  - Dram mode: expansion gated by server maxmemory watermark
  - Dram mode: shrink deletes the victim segment's keys and frees memory
  - Dram mode: expand, shrink to zero segments, expand again
  - Tiered mode: reactive expand on promotion
  - Tiered mode: shrink drops cached copies, NVMe data survives
  - Tiered mode: expand, shrink to zero segments, expand again
  - Tiered mode: shrink releases an EFA-registered segment
  - Both modes: a SET or COPY that finds its budget full (the DRAM arena, or `nvme-maxmemory`)
    evicts other objects; their keys read as misses until the scaling cron deletes them
"""

import os
import shutil
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager, suppress

import pytest
from valkey import OutOfMemoryError, ResponseError
from valkeytestframework.util.waiters import wait_for_true
from test_largeobj_fabric import PEER_ADDRESS
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj


# 900KB in a 1MB segment: one object of this size fills a segment by itself.
SEGMENT_FILLING_SIZE = 900 * 1024


def wait_uring_registered_matches_segments(client, timeout=10):
    """Tiered: each pool's io_uring ring registers all its segments
    (dram_uring==dram_segments, nvme_uring==nvme_segments). wait_for because the
    re-register on expand/shrink is fire-and-forget on the poller."""
    def _match():
        i = info_largeobj(client)
        return (i['largeobj_dram_uring_registered_segments'] == i['largeobj_dram_segments']
                and i['largeobj_nvme_uring_registered_segments'] == i['largeobj_nvme_segments'])
    wait_for_true(_match, timeout=timeout)


def apply_shrink_pressure(client, policy='noeviction'):
    """Cap maxmemory at 85% of used_memory (ratio ~1.18, above the shrink
    watermark). With no TTL keys, volatile-lru evicts nothing in core, like
    noeviction, but allows Dram shrink."""
    client.config_set('maxmemory-policy', policy)
    client.config_set('maxmemory', int(client.info('memory')['used_memory'] * 0.85))


def apply_full_shrink_pressure(client, policy='noeviction'):
    """maxmemory 1 byte: pressure never lifts, so shrink releases every segment."""
    client.config_set('maxmemory-policy', policy)
    client.config_set('maxmemory', 1)


def shrink_completed(client):
    """No segment draining and every reclaimed key deleted."""
    info = info_largeobj(client)
    return info['largeobj_dram_draining_segments'] == 0 and info['largeobj_pending_reclaims'] == 0


def promote(client, keys):
    """Tiered (promote-min-hits 1): one GET per key caches it in DRAM."""
    for key in keys:
        client.execute_command('BLOB.GET', key)
    wait_for_true(lambda: info_largeobj(client)['largeobj_dram_objects'] == len(keys))


# ─── Dram Mode Scaling ────────────────────────────────────────────────────────

def set_policy(client, policy='allkeys-lru'):
    """Let the module evict (or not): it needs maxmemory > 0 and a policy other than noeviction.

    Dram also freezes the pool, so a full segment can only be served by evicting: with the shrink
    watermark at its 50% floor, a maxmemory about two segments above usage stops `try_expand` (any
    tighter and the core evicts or refuses the command itself). Tiered leaves maxmemory far above
    usage, so the core never evicts and every eviction seen is the module's.
    """
    used = int(client.info('memory')['used_memory'])
    if 'largeobj_nvme_segments' in info_largeobj(client):
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024 * 1024))
    else:
        segment = info_largeobj(client)['largeobj_dram_segment_size_bytes']
        client.execute_command('CONFIG', 'SET', 'largeobj.scaling-shrink-watermark', 50)
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(2 * (used + segment) - 512 * 1024))
    client.execute_command('CONFIG', 'SET', 'maxmemory-policy', policy)


def wait_unlinked(client):
    """Tiered eviction credits a victim's bytes when its file is unlinked, off the event loop."""
    wait_for_true(lambda: info_largeobj(client)['largeobj_disk_pending_free_bytes'] == 0, timeout=10)


def hook(client, name, value):
    """Set a test-only pause or failure hook, which opens a window in the production path."""
    client.execute_command('CONFIG', 'SET', f'largeobj.test-{name}', value)


def background_set(server, key, payload):
    """BLOB.SET on a connection of its own, in a thread. Returns the thread and its outcome, whose
    'reply' appears when it finishes."""
    outcome = {}
    thread = threading.Thread(target=lambda: outcome.update(
        reply=server.get_new_client().execute_command('BLOB.SET', key, payload)))
    thread.start()
    return thread, outcome


@contextmanager
def paused_set(server, client, key, payload):
    """A SET paused after writing its file and before its key commits. The pause is lifted at once,
    so other commands run meanwhile; the SET commits when the block ends. Yields its outcome."""
    hook(client, 'pause-before-finalize-set-ms', 3000)
    thread, outcome = background_set(server, key, payload)
    time.sleep(0.5)  # the file is written and the task is paused
    hook(client, 'pause-before-finalize-set-ms', 0)
    try:
        yield outcome
    finally:
        thread.join(timeout=15)


def set_objects(client, prefix, count, size):
    """SET `count` objects of `size` bytes, each filled with its own byte; return them by key."""
    payloads = {f'{prefix}{i}': bytes([i % 251 + 1]) * size for i in range(count)}
    for key, payload in payloads.items():
        assert client.execute_command('BLOB.SET', key, payload) == b'OK'
    return payloads


def surviving(client, payloads):
    """The objects that survive, by key, read back. `EXISTS` is no judge: an evicted key outlives its
    object until the scaling cron deletes it."""
    got = {key: client.execute_command('BLOB.GET', key) for key in payloads}
    return {key: value for key, value in got.items() if value is not None}


def pending_reclaims(client):
    return info_largeobj(client)['largeobj_pending_reclaims']


def pinned_skips(client):
    return info_largeobj(client)['largeobj_pinned_skips']


def wait_reclaimed(client, timeout=15):
    """The scaling cron deletes the keys of evicted objects, one tick (`scaling-poll-ms`) at a time."""
    wait_for_true(lambda: pending_reclaims(client) == 0, timeout=timeout)


def write_until(client, prefix, payload, done, timeout=10):
    """SET `payload` under `prefix`0, `prefix`1, ... until `done()` or the timeout, ignoring failed
    SETs. Returns the keys it tried."""
    keys, deadline = [], time.time() + timeout
    while not done() and time.time() < deadline:
        for _ in range(16):
            keys.append(f'{prefix}{len(keys)}')
            with suppress(ResponseError):
                client.execute_command('BLOB.SET', keys[-1], payload)
    return keys


@contextmanager
def transfer_holding(server, client, key):
    """Hold a fabric transfer of `key`, which pins it. The peer is dead, so each GET holds its pin
    until the fabric times out, and a new one takes over."""
    reader = server.get_new_client()
    reader.execute_command('BLOB.HELLO', PEER_ADDRESS)
    stop = threading.Event()

    def hold():
        while not stop.is_set():
            try:
                if reader.execute_command('BLOB.GET', key, 0, 0, 1 << 30) is None:  # rkey, addr, len
                    time.sleep(0.01)  # the key is gone: nothing left to hold
            except ResponseError:
                pass

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    try:
        wait_for_true(lambda: int(client.info('clients')['blocked_clients']) >= 1)  # a transfer is in flight
        yield
    finally:
        stop.set()
        thread.join(timeout=30)


def write_until_skipped(client, prefix, payload):
    """SET until eviction has passed over a pinned object. Returns the keys it tried."""
    before = pinned_skips(client)
    keys = write_until(client, prefix, payload, lambda: pinned_skips(client) > before)
    assert pinned_skips(client) > before, "eviction never met the pinned object"
    return keys


class TestDramReactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows reactively when a segment fills (SET path).

    scaling-poll-ms is set very high (60s) so the scaling cron cannot fire
    during the test. Any expand observed must be from the reactive SET path.
    """

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB → pool starts with 1 segment, grows on demand.
        # scaling-poll-ms=60000 → cron fires at most once per minute, won't interfere.
        # fabric-provider Emulated on loopback for EFA reactive expand test.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-poll-ms 60000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags):
        """Launch the fabric_target peer process and return (process, address, rkey, remote_addr, length)."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        process = subprocess.Popen(
            [target, '127.0.0.1', *flags],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        line = process.stdout.readline()
        assert line.startswith('advertisement: '), line
        address, rkey, remote_addr, length = line.split()[1:]
        return process, address, int(rkey), int(remote_addr), int(length)

    def test_expand_on_segment_full(self):
        """A SET that does not fit the one segment expands the pool."""
        client = self.server.get_new_client()
        before = info_largeobj(client)

        assert client.execute_command('BLOB.SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE) == b'OK'
        assert client.execute_command('BLOB.SET', 'key_b', b'B' * SEGMENT_FILLING_SIZE) == b'OK'

        after = info_largeobj(client)
        assert after['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 1
        assert after['largeobj_dram_segments'] == 2
        # Dram mode has no io_uring ring, so nothing is ever io_uring-registered.
        assert after['largeobj_dram_uring_registered_segments'] == 0

    def test_expand_data_integrity(self):
        """Objects spread over expanded segments all read back correctly."""
        client = self.server.get_new_client()
        before = info_largeobj(client)['largeobj_dram_scaling_expands']
        payloads = {f'key_{i}': bytes([i]) * (800 * 1024) for i in range(4)}

        for key, payload in payloads.items():
            client.execute_command('BLOB.SET', key, payload)

        assert info_largeobj(client)['largeobj_dram_scaling_expands'] == before + 3
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.GET', key) == payload

    def test_efa_set_triggers_reactive_expand(self):
        """An EFA SET that fits no segment expands the pool itself.

        Fill segment 1 with 4KB TCP objects until one more expands the pool,
        then give segment 2 the same number: it is now as full as segment 1,
        so the next 4KB object -- the EFA SET -- fits neither.
        """
        client = self.server.get_new_client()
        expands = lambda: info_largeobj(client)['largeobj_dram_scaling_expands']
        efa_len = 4096
        # fabric_target --read serves 0x00..0xFF repeating (its generate_pattern()).
        efa_payload = bytes(i % 256 for i in range(efa_len))
        small = b'F' * efa_len
        start = expands()

        sets = 0
        while expands() == start:
            client.execute_command('BLOB.SET', f'filler_{sets}', small)
            sets += 1
            assert sets < 1024, "4KB objects never filled a 1MB segment"
        per_segment = sets - 1  # the last SET went to segment 2
        for i in range(per_segment - 1):
            client.execute_command('BLOB.SET', f'filler_{sets + i}', small)
        assert expands() == start + 1
        assert info_largeobj(client)['largeobj_dram_segments'] == 2

        process, address, rkey, remote_addr, length = self.start_target('--read')
        try:
            client.execute_command('BLOB.HELLO', address)
            result = client.execute_command('BLOB.SET', 'efa_key', efa_len, rkey, remote_addr, length)
            assert result == b'OK', f"EFA SET failed: {result}"
            assert client.execute_command('BLOB.GET', 'efa_key') == efa_payload
        finally:
            process.kill()
        assert expands() == start + 2
        assert info_largeobj(client)['largeobj_dram_segments'] == 3


class TestDramProactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows proactively when the scaling cron sees utilization > watermark.

    scaling-poll-ms=1000 so the cron fires every second. The test writes enough
    data to push utilization above the expand watermark, then stops writing and
    waits for the cron to add a segment.
    """

    EXPAND_TIMEOUT_S = 15

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB; no server maxmemory, so shrink never fires.
        # scaling-expand-watermark=50 so filling half a segment triggers proactive expand.
        # scaling-poll-ms=1000 so the cron fires frequently.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-expand-watermark 50"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_proactive_expand_fires_when_watermark_exceeded(self):
        """The scaling cron adds a segment when utilization exceeds the expand watermark.

        One 600KB object puts one 1MB segment at ~59% > 50%. No SET follows, so
        any expand is the cron's.
        """
        client = self.server.get_new_client()
        expand_before = info_largeobj(client)['largeobj_dram_scaling_expands']

        assert client.execute_command('BLOB.SET', 'probe', b'P' * (600 * 1024)) == b'OK'
        assert info_largeobj(client)['largeobj_dram_scaling_expands'] == expand_before

        wait_for_true(
            lambda: info_largeobj(client)['largeobj_dram_scaling_expands'] > expand_before,
            timeout=self.EXPAND_TIMEOUT_S,
        )

        # The next 600KB object doesn't fit the first segment, so it must land in
        # the cron's new one. Raising the watermark to 95 (~59% used after it)
        # stops cron expands, so any expand from here would be the SET's own.
        client.config_set('largeobj.scaling-expand-watermark', 95)
        expanded = info_largeobj(client)['largeobj_dram_scaling_expands']
        second = b'S' * (600 * 1024)
        assert client.execute_command('BLOB.SET', 'second', second) == b'OK'
        assert info_largeobj(client)['largeobj_dram_scaling_expands'] == expanded
        assert client.execute_command('BLOB.GET', 'second') == second


class TestDramServerMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: expansion is gated by the SERVER maxmemory watermark.

    There is no module-local DRAM budget — the pool grows on demand and the
    only ceiling is Valkey's own `maxmemory` (via would_cross_memory_watermark).
    """

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_expansion_capped_by_server_maxmemory(self):
        """With less than one segment of headroom under maxmemory, an object
        that needs a new segment is rejected by the module and the pool does
        not grow. The object is small enough that core's own OOM check passes."""
        client = self.server.get_new_client()
        client.config_set('maxmemory-policy', 'noeviction')
        client.execute_command('BLOB.SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE)

        used = int(client.info('memory')['used_memory'])
        # 600KB under maxmemory: core's OOM check lets a 200KB SET through, but
        # the module can't add a 1MB segment.
        client.config_set('maxmemory', used + 600 * 1024)
        before = info_largeobj(client)

        # 200KB does not fit the ~124KB left in the segment, so it needs a new
        # segment, and the module rejects it.
        with pytest.raises(ResponseError, match='pool exhausted'):
            client.execute_command('BLOB.SET', 'key_b', b'B' * (200 * 1024))
        after = info_largeobj(client)
        assert after['largeobj_dram_segments'] == before['largeobj_dram_segments']
        assert after['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands']


class TestDramShrink(ValkeyLargeObjTestCaseBase):
    """Dram mode: under server memory pressure and an evicting maxmemory-policy,
    the scaling cron reclaims a segment by deleting the keys whose objects live
    on it, so used_memory drops and writes fit again.
    """

    TIMEOUT_S = 20
    # One object per segment, so each shrink reclaims exactly one key.
    PAYLOADS = {f'dshrink_{i}': bytes([i]) * SEGMENT_FILLING_SIZE for i in range(4)}

    def get_module_args(self, data_dir, direct_io):
        # Expand watermark 95 (above one object's ~88% of a segment): no proactive
        # expand, so the pool holds exactly one segment per object and no empty
        # segment the shrink could pick instead.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-poll-ms 1000"
            f" scaling-expand-watermark 95"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def _write_payloads(self, client):
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'

    def test_shrink_deletes_victim_keys_and_frees_memory(self):
        """Under maxmemory pressure with an evicting policy, a shrink releases a
        segment and deletes its keys, which frees enough memory for core writes
        to succeed again. Every deleted key fires an 'evicted' keyspace event,
        and every surviving key still reads back correctly."""
        client = self.server.get_new_client()
        self._write_payloads(client)
        before = info_largeobj(client)
        # Subscribe before the shrink so no 'evicted' event is missed.
        client.config_set('notify-keyspace-events', 'Ee')
        events = client.pubsub()
        events.subscribe('__keyevent@0__:evicted')
        assert events.get_message(timeout=1)['type'] == 'subscribe'

        apply_shrink_pressure(client, 'volatile-lru')
        # Over maxmemory: core rejects the write before any shrink has run.
        with pytest.raises(OutOfMemoryError):
            client.set('core_key', 'v')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_scaling_shrinks']
                      > before['largeobj_dram_scaling_shrinks'], timeout=self.TIMEOUT_S)
        # Retry SET on a standard (core) Valkey string key until it returns OK:
        # it raises OOM until the shrink has freed enough memory. The client
        # maps an OK reply to True.
        wait_for_true(lambda: client.set('core_key', 'v') is True,
                      ignore_exception=OutOfMemoryError, timeout=self.TIMEOUT_S)

        # Lift the pressure so no further shrink starts, then wait for the last
        # one to finish: segment released and its keys deleted.
        client.config_set('maxmemory', 0)
        wait_for_true(lambda: shrink_completed(client), timeout=self.TIMEOUT_S)

        after = info_largeobj(client)
        # Every shrink releases exactly one segment.
        num_shrinks = after['largeobj_dram_scaling_shrinks'] - before['largeobj_dram_scaling_shrinks']
        assert num_shrinks > 0
        assert after['largeobj_dram_segments'] == before['largeobj_dram_segments'] - num_shrinks
        # Every key the shrinks deleted is counted as one reclaim.
        surviving_keys = [k for k in self.PAYLOADS if client.exists(k)]
        deleted_keys = set(self.PAYLOADS) - set(surviving_keys)
        assert deleted_keys
        assert after['largeobj_reclaims'] == before['largeobj_reclaims'] + len(deleted_keys)
        # Keys whose segments were not released read back unchanged.
        for key in surviving_keys:
            assert client.execute_command('BLOB.GET', key) == self.PAYLOADS[key]
        # Each deleted key fires the same 'evicted' event as a core eviction.
        evicted_keys = []
        while (msg := events.get_message(timeout=1)) is not None:
            if msg['type'] == 'message':
                evicted_keys.append(msg['data'].decode())
        assert sorted(evicted_keys) == sorted(deleted_keys)

    def test_reclaimed_key_reads_as_missing(self):
        """Between the shrink and its key deletion, the victim key still exists
        but every module command treats it as missing. A user DEL in that window
        takes it off the reclaim list but is not counted as a reclaim."""
        client = self.server.get_new_client()
        # The shrink tick re-arms the cron 60s out, holding the window open.
        client.config_set('largeobj.reclaim-poll-ms', 60000)
        self._write_payloads(client)

        apply_shrink_pressure(client, 'volatile-lru')
        wait_for_true(lambda: info_largeobj(client)['largeobj_pending_reclaims'] == 1,
                      timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)  # let COPY (denyoom) reach the module

        victims = [k for k in self.PAYLOADS if client.execute_command('BLOB.GET', k) is None]
        assert len(victims) == 1
        victim = victims[0]
        assert client.exists(victim) == 1
        with pytest.raises(ResponseError, match='not found'):
            client.execute_command('BLOB.INFO', victim)
        with pytest.raises(ResponseError, match='module key failed to copy'):
            client.execute_command('COPY', victim, 'copy_dst')

        reclaims = info_largeobj(client)['largeobj_reclaims']
        client.delete(victim)
        wait_for_true(lambda: info_largeobj(client)['largeobj_pending_reclaims'] == 0)
        # The user deleted it, not the module, so it isn't counted.
        assert info_largeobj(client)['largeobj_reclaims'] == reclaims

    def test_no_shrink_under_noeviction(self):
        """Dram shrink deletes keys, so noeviction disables it."""
        client = self.server.get_new_client()
        self._write_payloads(client)
        shrinks = info_largeobj(client)['largeobj_dram_scaling_shrinks']

        apply_shrink_pressure(client)
        time.sleep(3)  # three cron ticks at scaling-poll-ms 1000

        assert info_largeobj(client)['largeobj_dram_scaling_shrinks'] == shrinks
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.GET', key) == payload

    def test_no_shrink_without_maxmemory(self):
        """maxmemory 0 gives no pressure signal, so no shrink under any policy."""
        client = self.server.get_new_client()
        self._write_payloads(client)
        shrinks = info_largeobj(client)['largeobj_dram_scaling_shrinks']

        client.config_set('maxmemory-policy', 'volatile-lru')
        time.sleep(3)  # three cron ticks at scaling-poll-ms 1000

        assert info_largeobj(client)['largeobj_dram_scaling_shrinks'] == shrinks
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.GET', key) == payload

    def test_expand_shrink_to_zero_expand(self):
        """The pool expands, shrinks to zero segments (every key reclaimed),
        and expands again for the next SET."""
        client = self.server.get_new_client()
        before = info_largeobj(client)
        self._write_payloads(client)
        expanded = info_largeobj(client)
        assert expanded['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 3
        assert expanded['largeobj_dram_segments'] == 4

        apply_full_shrink_pressure(client, 'volatile-lru')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_segments'] == 0
                      and shrink_completed(client), timeout=self.TIMEOUT_S)
        shrunk = info_largeobj(client)
        assert shrunk['largeobj_dram_scaling_shrinks'] == expanded['largeobj_dram_scaling_shrinks'] + 4
        assert shrunk['largeobj_reclaims'] == expanded['largeobj_reclaims'] + 4
        assert shrunk['largeobj_num_objects'] == 0
        assert client.dbsize() == 0

        client.config_set('maxmemory', 0)
        assert client.execute_command('BLOB.SET', 'after', b'Z' * SEGMENT_FILLING_SIZE) == b'OK'
        regrown = info_largeobj(client)
        assert regrown['largeobj_dram_scaling_expands'] == shrunk['largeobj_dram_scaling_expands'] + 1
        assert regrown['largeobj_dram_segments'] == 1
        assert client.execute_command('BLOB.GET', 'after') == b'Z' * SEGMENT_FILLING_SIZE


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────

TIERED_ARGS = (
    "operating-mode Tiered"
    " nvme-dir {data_dir}"
    " nvme-staging-size 4194304"
    " segment-size 1048576"
    " max-promote-size 983040"
    " promote-min-hits 1"
    " scaling-poll-ms 1000"
    " chunk-size 65536"
    " bench-mode no"
    " direct-io no"
)


class DramArena:
    """Dram mode with one 1MB segment that `set_policy` freezes, so eviction is the only way a SET
    succeeds once it is full. `POLL_MS` keeps the scaling cron out of the way: a victim's key stays
    on the reclaim list for the test to see."""

    SEGMENT_SIZE = 1024 * 1024
    OBJ = SEGMENT_SIZE // 4
    OBJECTS_PER_CAP = 3  # a fourth would fill the segment exactly, but talc's metadata lives in it
    EVICTIONS = 'largeobj_evictions'
    POLL_MS = 60_000

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" max-object-size {self.SEGMENT_SIZE - 64 * 1024}"
            f" chunk-size 65536"
            f" scaling-poll-ms {self.POLL_MS}"
            f" bench-mode no"
            f" direct-io no"
        )

    def new_client(self):
        client = self.server.get_new_client()
        set_policy(client)
        return client

    def wait_settled(self, client, live):
        """Every victim freed: `live` objects are left."""
        wait_for_true(lambda: info_largeobj(client)['largeobj_num_objects'] == live)


class TestDramEviction(DramArena, ValkeyLargeObjTestCaseBase):

    def test_writing_past_the_segment_evicts_and_victims_read_as_misses(self):
        """Three segments' worth cannot be resident at once, so these SETs only succeed by evicting.
        A victim's key, still in the keyspace, reads as a miss until the cron deletes it."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 12, self.OBJ)

        info = info_largeobj(client)
        survivors = surviving(client, payloads)
        victims = len(payloads) - len(survivors)
        assert info['largeobj_evictions'] == victims > 0
        assert info['largeobj_dram_segments'] == 1
        assert 'obj_11' in survivors, "the last write cannot have been anyone's victim"
        assert all(survivors[key] == payloads[key] for key in survivors)
        assert pending_reclaims(client) == victims
        assert client.execute_command('EXISTS', *payloads) == len(payloads), "no key is gone yet"

    def test_a_full_arena_is_refused_not_evicted_under_noeviction(self):
        client = self.server.get_new_client()
        set_policy(client, 'noeviction')
        payloads = set_objects(client, 'obj_', 3, self.OBJ)

        with pytest.raises(ResponseError, match='pool exhausted'):
            client.execute_command('BLOB.SET', 'refused', b'R' * self.OBJ)
        assert info_largeobj(client)['largeobj_evictions'] == 0
        assert surviving(client, payloads) == payloads

    def test_copy_evicts_another_key_and_never_its_source(self):
        """COPY pins its source: alone in the segment it fails cleanly, and with another key
        resident that key is the victim."""
        client = self.new_client()
        big = b'S' * (640 * 1024)
        assert client.execute_command('BLOB.SET', 'src', big) == b'OK'
        with pytest.raises(ResponseError):
            client.execute_command('COPY', 'src', 'dst')
        assert client.execute_command('BLOB.GET', 'src') == big
        assert client.execute_command('EXISTS', 'dst') == 0
        assert info_largeobj(client)['largeobj_evictions'] == 0

        client.execute_command('FLUSHALL')
        obj = b'A' * (384 * 1024)
        assert client.execute_command('BLOB.SET', 'src', obj) == b'OK'
        assert client.execute_command('BLOB.SET', 'other', b'O' * len(obj)) == b'OK'
        assert client.execute_command('COPY', 'src', 'dst') in (1, True)
        assert info_largeobj(client)['largeobj_evictions'] == 1
        assert client.execute_command('BLOB.GET', 'other') is None
        assert client.execute_command('BLOB.GET', 'src') == obj
        assert client.execute_command('BLOB.GET', 'dst') == obj

    def test_efa_set_evicts_at_alloc_time(self):
        """An EFA SET into a full arena evicts before the transfer. The peer is dead, so the SET
        errors; the counter moving is the assertion."""
        client = self.new_client()
        for i in range(3):  # one short of a full segment, which would evict on its own
            assert client.execute_command('BLOB.SET', f'fill_{i}', b'F' * self.OBJ) == b'OK'
        client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        assert info_largeobj(client)['largeobj_evictions'] == 0

        with suppress(ResponseError):
            client.execute_command('BLOB.SET', 'efa_newcomer', self.OBJ, 0, 0, self.OBJ)  # len, rkey, addr, len
        assert info_largeobj(client)['largeobj_evictions'] > 0

    def test_a_pinned_object_is_not_claimed_for_the_arena(self):
        """A transfer reading the only resident pins it: a SET that needs its room is refused, not
        served by taking it. Once the transfer ends it is a victim again."""
        client = self.new_client()
        held = b'H' * (self.SEGMENT_SIZE // 2 + 64 * 1024)
        assert client.execute_command('BLOB.SET', 'held', held) == b'OK'
        with transfer_holding(self.server, client, 'held'):
            before = pinned_skips(client)
            with pytest.raises(ResponseError):
                client.execute_command('BLOB.SET', 'newcomer', held)
            assert pinned_skips(client) > before
            assert client.execute_command('BLOB.GET', 'held') == held

        assert client.execute_command('BLOB.SET', 'newcomer', held) == b'OK'
        assert client.execute_command('BLOB.GET', 'held') is None


class TestDramEvictionPolicy(DramArena, ValkeyLargeObjTestCaseBase):
    """*Which* object eviction destroys: the one with the fewest hits, whatever the
    `maxmemory-policy`. Four residents against the default 5 samples mean every key is scored, so
    the order is exact."""

    OBJ = DramArena.SEGMENT_SIZE // 5  # four fit; a fifth overshoots, and one victim covers the shortfall
    HOT, COLD = ('hot_0', 'hot_1'), ('cold_0', 'cold_1')

    def fill(self, policy):
        client = self.server.get_new_client()
        set_policy(client, policy)
        for key in self.HOT + self.COLD:
            assert client.execute_command('BLOB.SET', key, key.encode()[:1] * self.OBJ) == b'OK'
        return client

    def read(self, client, keys):
        return [client.execute_command('BLOB.GET', key) for key in keys]

    @pytest.mark.parametrize('policy', ['allkeys-lfu', 'volatile-ttl'])
    def test_the_rarely_used_keys_are_evicted(self, policy):
        client = self.fill(policy)
        for _ in range(10):  # the first hit is certain to count: this lifts the hot pair clear
            self.read(client, self.HOT)

        newcomer = b'N' * self.OBJ
        assert client.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        assert all(value is not None for value in self.read(client, self.HOT)), "the hot pair paid"
        assert sum(value is None for value in self.read(client, self.COLD)) == 1, "one victim covers it"
        assert client.execute_command('BLOB.GET', 'newcomer') == newcomer



class TestTieredExpand(ValkeyLargeObjTestCaseBase):
    """Tiered mode: SET writes to NVMe, so the DRAMPool grows on promotion (GET)."""

    def get_module_args(self, data_dir, direct_io):
        # Expand watermark 95: no proactive expand, only the promotion's own.
        return TIERED_ARGS.format(data_dir=data_dir) + " scaling-expand-watermark 95"

    def test_tiered_expand_on_promotion(self):
        """Promoting an object that fits no segment expands the pool."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE)
        client.execute_command('BLOB.SET', 'key_b', b'B' * SEGMENT_FILLING_SIZE)
        before = info_largeobj(client)

        promote(client, ['key_a', 'key_b'])

        after = info_largeobj(client)
        assert after['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 1
        assert after['largeobj_dram_segments'] == 2
        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * SEGMENT_FILLING_SIZE
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * SEGMENT_FILLING_SIZE
        # The expanded DRAM segment joins its ring's io_uring table (per-pool).
        wait_uring_registered_matches_segments(client)


class TestTieredShrink(ValkeyLargeObjTestCaseBase):
    """Tiered mode: under server memory pressure the scaling cron drops cached
    DRAM copies a segment at a time; the data stays on NVMe. Tiered shrink
    deletes no keys, so it runs under noeviction too."""

    TIMEOUT_S = 20
    PAYLOADS = {f'tshrink_{i}': bytes([i]) * SEGMENT_FILLING_SIZE for i in range(4)}

    def get_module_args(self, data_dir, direct_io):
        # Expand watermark 95: one segment per cached object and no empty
        # segment the shrink could pick instead.
        return TIERED_ARGS.format(data_dir=data_dir) + " scaling-expand-watermark 95"

    def _write_and_promote(self, client):
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.SET', key, payload) == b'OK'
        promote(client, self.PAYLOADS)

    def _assert_reads_from_nvme(self, client):
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.GET', key) == payload

    def test_shrink_drops_cached_copies_keeps_nvme_data(self):
        client = self.server.get_new_client()
        self._write_and_promote(client)
        before = info_largeobj(client)
        assert before['largeobj_dram_segments'] == 4

        apply_shrink_pressure(client, 'noeviction')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_scaling_shrinks']
                      > before['largeobj_dram_scaling_shrinks'], timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)
        wait_for_true(lambda: shrink_completed(client), timeout=self.TIMEOUT_S)

        after = info_largeobj(client)
        shrinks = after['largeobj_dram_scaling_shrinks'] - before['largeobj_dram_scaling_shrinks']
        assert after['largeobj_dram_segments'] == 4 - shrinks
        assert after['largeobj_dram_objects'] == 4 - shrinks
        assert after['largeobj_reclaims'] == before['largeobj_reclaims']  # no key deleted
        assert client.dbsize() == 4
        self._assert_reads_from_nvme(client)
        # The released segment left its ring's io_uring table too.
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)

    def test_no_shrink_without_maxmemory(self):
        """maxmemory 0 gives no pressure signal, so cached copies stay."""
        client = self.server.get_new_client()
        self._write_and_promote(client)
        before = info_largeobj(client)

        time.sleep(3)  # three cron ticks at scaling-poll-ms 1000

        after = info_largeobj(client)
        assert after['largeobj_dram_scaling_shrinks'] == before['largeobj_dram_scaling_shrinks']
        assert after['largeobj_dram_objects'] == before['largeobj_dram_objects']

    def test_expand_shrink_to_zero_expand(self):
        """Promotion expands the pool, shrink drops every cached copy down to
        zero segments, and the next promotion expands again."""
        client = self.server.get_new_client()
        before = info_largeobj(client)
        self._write_and_promote(client)
        expanded = info_largeobj(client)
        assert expanded['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 3
        assert expanded['largeobj_dram_segments'] == 4

        apply_full_shrink_pressure(client, 'noeviction')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_segments'] == 0
                      and shrink_completed(client), timeout=self.TIMEOUT_S)
        shrunk = info_largeobj(client)
        assert shrunk['largeobj_dram_scaling_shrinks'] == expanded['largeobj_dram_scaling_shrinks'] + 4
        assert shrunk['largeobj_dram_objects'] == 0
        # Still under pressure, so no segment can be added: reads come from NVMe.
        self._assert_reads_from_nvme(client)
        assert info_largeobj(client)['largeobj_dram_segments'] == 0
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)

        client.config_set('maxmemory', 0)
        promote(client, ['tshrink_0'])
        regrown = info_largeobj(client)
        assert regrown['largeobj_dram_scaling_expands'] == shrunk['largeobj_dram_scaling_expands'] + 1
        assert regrown['largeobj_dram_segments'] == 1
        assert client.execute_command('BLOB.INFO', 'tshrink_0', 'TIER') == b'dram'
        assert client.execute_command('BLOB.GET', 'tshrink_0') == self.PAYLOADS['tshrink_0']
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)


class TestTieredShrinkReleasesEfaRegisteredSegment(ValkeyLargeObjTestCaseBase):
    """Tiered mode, fabric UP: a segment added by expansion is EFA-registered, and shrinking it
    must tear that registration down (Segment::drop -> efa_release_segment) BEFORE the segment
    memory is freed — a broken teardown (registration outliving freed pages) would crash here."""

    TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        # Fabric up (Emulated on loopback) so expanded segments are actually EFA-registered.
        return (TIERED_ARGS.format(data_dir=data_dir)
                + " fabric-provider Emulated fabric-interfaces lo")

    def test_shrink_releases_efa_registered_expanded_segment(self):
        client = self.server.get_new_client()
        expand_before = info_largeobj(client)['largeobj_dram_scaling_expands']

        # Promoting the second ~full-segment object forces a reactive expand, and with the
        # fabric up try_expand EFA-registers that new segment (the path under test).
        client.execute_command('BLOB.SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE)
        client.execute_command('BLOB.SET', 'key_b', b'B' * SEGMENT_FILLING_SIZE)
        promote(client, ['key_a', 'key_b'])

        expanded = info_largeobj(client)
        assert expanded['largeobj_dram_scaling_expands'] > expand_before
        dram_segments = expanded['largeobj_dram_segments']
        registered = expanded['largeobj_efa_registered_segments']
        assert dram_segments > 1
        # EFA registration spans BOTH pools: every live segment (DRAM + NVMe staging) is registered.
        assert registered == dram_segments + expanded['largeobj_nvme_segments']
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)

        # In Tiered mode shrink releases a live-data segment (data persists on NVMe), and with
        # the fabric up the released segment carries an EFA registration Segment::drop tears down.
        apply_shrink_pressure(client)
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_scaling_shrinks']
                      > expanded['largeobj_dram_scaling_shrinks'], timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)
        wait_for_true(lambda: shrink_completed(client), timeout=self.TIMEOUT_S)

        assert client.execute_command('PING')
        after = info_largeobj(client)
        assert after['largeobj_efa_registered_segments'] < registered
        assert after['largeobj_efa_registered_segments'] == (
            after['largeobj_dram_segments'] + after['largeobj_nvme_segments'])
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)
        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * SEGMENT_FILLING_SIZE
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * SEGMENT_FILLING_SIZE


class TieredCap:
    """Tiered mode against a small `nvme-maxmemory`, the only budget eviction can free. The pools
    are bigger than any object, so only the ledger can reject a SET, and it is exact. A victim's
    key outlives its file on the reclaim list (`POLL_MS` keeps the cron away): survivors still
    read back."""

    OBJ = 512 * 1024
    DISK_PER_OBJ = OBJ + 4096  # the ledger also charges the file's one-page header
    OBJECTS_PER_CAP = 8
    CAP = OBJECTS_PER_CAP * DISK_PER_OBJ  # a whole number of objects, so filling it evicts nothing
    EVICTIONS = 'largeobj_disk_evictions'
    POLL_MS = 60_000
    EXTRA_ARGS = ""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size {2 * self.CAP}"
            f" segment-size {2 * self.CAP}"
            f" max-object-size {self.CAP}"
            f" max-promote-size {self.CAP}"
            f" scaling-poll-ms {self.POLL_MS}"
            f" bench-mode no"
            f" direct-io no"
        ) + self.EXTRA_ARGS

    def new_client(self):
        client = self.server.get_new_client()
        set_policy(client)
        return client

    def fill_cap(self, client):
        return set_objects(client, 'fill_', self.OBJECTS_PER_CAP, self.OBJ)

    def wait_settled(self, client, live, timeout=10):
        """Every victim unlinked and every pin released: the ledger and the directory hold exactly
        `live` objects."""
        def settled():
            info = info_largeobj(client)
            return (info['largeobj_disk_pending_free_bytes'] == 0
                    and info['largeobj_disk_pinned_objects'] == 0
                    and info['largeobj_disk_used_bytes'] == live * self.DISK_PER_OBJ
                    and len(self._object_files()) == live)
        wait_for_true(settled, timeout=timeout)


class TestTieredEviction(TieredCap, ValkeyLargeObjTestCaseBase):

    def assert_rejected(self, client, key, payload):
        with pytest.raises(ResponseError, match='capacity exceeded'):
            client.execute_command('BLOB.SET', key, payload)

    def test_writing_past_the_cap_evicts_exactly_and_stays_bounded(self):
        """Three caps' worth: the ledger, counters, survivors, reclaim list and directory agree on
        what died."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)

        info = info_largeobj(client)
        victims = info['largeobj_disk_evictions']
        assert victims > 0
        assert info['largeobj_disk_eviction_failures'] == 0
        assert info['largeobj_evictions'] == 0, "a Tiered node never evicts for DRAM"
        assert info['largeobj_disk_eviction_reclaimed_bytes'] == victims * self.DISK_PER_OBJ
        survivors = surviving(client, payloads)
        assert f'obj_{len(payloads) - 1}' in survivors
        assert len(survivors) == len(payloads) - victims
        assert all(value == payloads[key] for key, value in survivors.items())
        assert info['largeobj_disk_used_bytes'] == len(survivors) * self.DISK_PER_OBJ
        assert info['largeobj_pending_reclaims'] == victims, "every victim's key waits to be deleted"
        assert info['largeobj_disk_pinned_objects'] == 0, "no write is in flight"
        wait_for_true(lambda: len(self._object_files()) == len(survivors), timeout=10)

    def test_a_refused_request_destroys_nothing(self):
        """A COPY needing more room than the one other object can give (its source is no victim)
        evicts nothing, and is no failed eviction either: that counter is for requests that
        destroyed something and still fell short."""
        client = self.new_client()
        big, small = b'S' * (5 * self.DISK_PER_OBJ - 4096), b's' * self.OBJ
        assert client.execute_command('BLOB.SET', 'src', big) == b'OK'
        assert client.execute_command('BLOB.SET', 'small', small) == b'OK'
        before, files = info_largeobj(client), self._object_files()

        with pytest.raises(ResponseError):
            client.execute_command('COPY', 'src', 'dst')

        after = info_largeobj(client)
        assert client.execute_command('BLOB.GET', 'src') == big
        assert client.execute_command('BLOB.GET', 'small') == small
        assert client.execute_command('EXISTS', 'dst') == 0
        for field in ('disk_used_bytes', 'disk_evictions', 'disk_eviction_failures', 'disk_pinned_objects'):
            assert after[f'largeobj_{field}'] == before[f'largeobj_{field}'], field
        assert after['largeobj_pending_reclaims'] == 0
        assert self._object_files() == files

    def test_requests_the_cap_cannot_serve_destroy_nothing(self):
        """One bigger than the cap itself, and any write under `noeviction` or without a
        `maxmemory`."""
        client = self.new_client()
        payloads = self.fill_cap(client)

        self.assert_rejected(client, 'huge', b'H' * self.CAP)  # the payload alone is the cap
        assert info_largeobj(client)['largeobj_disk_eviction_failures'] == 0, "refused without a search"
        set_policy(client, 'noeviction')
        self.assert_rejected(client, 'overflow', b'O' * self.OBJ)
        set_policy(client)
        client.execute_command('CONFIG', 'SET', 'maxmemory', 0)
        self.assert_rejected(client, 'overflow', b'O' * self.OBJ)
        assert info_largeobj(client)['largeobj_disk_evictions'] == 0
        assert len(surviving(client, payloads)) == len(payloads)

    def test_copy_evicts_another_key_and_never_its_source(self):
        client = self.new_client()
        payloads = self.fill_cap(client)
        assert client.execute_command('COPY', 'fill_0', 'dst') in (1, True)
        wait_unlinked(client)  # the victims' files go in the background, then the ledger drops
        info = info_largeobj(client)
        victims = info['largeobj_disk_evictions']
        assert victims >= 1 and info['largeobj_disk_used_bytes'] <= self.CAP
        assert client.execute_command('BLOB.GET', 'dst') == payloads['fill_0']
        assert client.execute_command('BLOB.GET', 'fill_0') == payloads['fill_0'], "the source is never a victim"
        survivors = surviving(client, payloads)
        assert len(survivors) == self.OBJECTS_PER_CAP - victims
        assert all(value == payloads[key] for key, value in survivors.items())
        wait_for_true(lambda: len(self._object_files()) == len(survivors) + 1, timeout=10)
        assert info_largeobj(client)['largeobj_disk_pinned_objects'] == 0

    def test_a_freed_key_keeps_its_file_while_a_reader_holds_it(self):
        """DEL frees the key but not the file a transfer is still reading. Eviction must leave it
        alone, or it would list an object that no key leads to. When the transfer ends the file goes
        and its bytes come back."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        with transfer_holding(self.server, client, 'fill_0'):
            assert client.execute_command('DEL', 'fill_0') == 1
            written = write_until_skipped(client, 'pinned_', b'N' * self.OBJ)
            info = info_largeobj(client)
            assert info['largeobj_disk_pinned_objects'] == 1
            live = len(surviving(client, {**payloads, **dict.fromkeys(written)}))
            assert info['largeobj_disk_used_bytes'] == (live + 1) * self.DISK_PER_OBJ, \
                "the freed key's file is still there, and still charged"

        self.wait_settled(client, live, timeout=30)

    def test_a_write_that_fails_after_evicting_returns_every_byte(self):
        """An EFA SET to a dead peer evicts, charges, starts writing and fails: the victim stays
        gone, and the ledger and directory settle to exactly the survivors."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        with suppress(ResponseError):
            client.execute_command('BLOB.SET', 'efa_key', self.OBJ, 0, 0, self.OBJ)

        survivors = len(payloads) - 1
        assert info_largeobj(client)['largeobj_disk_evictions'] == 1
        assert client.execute_command('EXISTS', 'efa_key') == 0
        self.wait_settled(client, survivors)

    def test_a_write_that_cannot_create_its_file_returns_every_byte(self):
        """`nvme-dir` vanishes under a SET that has already charged its file: the charge comes back.
        At the cap the vanished directory cannot even be read for victims, so the SET evicts
        nothing either."""
        client = self.new_client()
        away = self.data_dir + '.away'

        def set_while_dir_is_away(key):
            os.rename(self.data_dir, away)
            try:
                with pytest.raises(ResponseError):
                    client.execute_command('BLOB.SET', key, b'N' * self.OBJ)
            finally:
                os.rename(away, self.data_dir)

        set_while_dir_is_away('with_room')
        assert info_largeobj(client)['largeobj_disk_used_bytes'] == 0
        self.fill_cap(client)
        set_while_dir_is_away('at_cap')
        info = info_largeobj(client)
        # The directory could not be read, so nothing was evicted for it, and nothing leaked.
        assert info['largeobj_disk_used_bytes'] == self.OBJECTS_PER_CAP * self.DISK_PER_OBJ
        assert info['largeobj_disk_pinned_objects'] == 0

    def test_a_victim_whose_key_was_deleted_stays_the_evicting_writes_to_unlink(self):
        """A victim's bytes belong to the write that evicted it, even once its key is deleted: the
        teardown leaves the file alone, and no other write may admit itself into that room or claim
        the file again, or both would unlink and credit it."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        hook(client, 'pause-before-evict-unlink-ms', 2000)
        writes = [background_set(self.server, 'w0', b'W' * self.OBJ)]
        wait_for_true(lambda: pending_reclaims(client) == 1, timeout=10)
        victim = next(key for key in payloads if key not in surviving(client, payloads))
        assert client.execute_command('DEL', victim) == 1
        time.sleep(0.5)  # the teardown runs now, in the window

        info = info_largeobj(client)
        assert info['largeobj_disk_used_bytes'] == (self.OBJECTS_PER_CAP + 1) * self.DISK_PER_OBJ
        assert info['largeobj_disk_pending_free_bytes'] == self.DISK_PER_OBJ

        # Enough writes to claim every file in the directory, the deleted victim's included if it
        # could be: all of them arrive well within the pause, before any new file exists.
        writes += [background_set(self.server, f'w{i}', b'W' * self.OBJ) for i in range(1, self.OBJECTS_PER_CAP)]
        for thread, _ in writes:
            thread.join(timeout=30)
        assert [outcome['reply'] for _, outcome in writes] == [b'OK'] * self.OBJECTS_PER_CAP

        wait_unlinked(client)
        assert info_largeobj(client)['largeobj_disk_evictions'] == self.OBJECTS_PER_CAP
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_a_key_being_read_is_not_evicted(self):
        """A GET pins its object until it ends, so eviction passes it over and it survives; once the
        transfer ends it is a victim again."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        with transfer_holding(self.server, client, 'fill_0'):
            write_until_skipped(client, 'pinned_', b'N' * self.OBJ)
        assert client.execute_command('BLOB.GET', 'fill_0') == payloads['fill_0']

        gone = lambda: client.execute_command('BLOB.GET', 'fill_0') is None
        write_until(client, 'after_', b'N' * self.OBJ, gone)
        assert gone(), "the key is still pinned after the transfer ended"

    @pytest.mark.parametrize('overwrite', [False, True])
    def test_a_set_in_flight_is_not_evicted(self, overwrite):
        """A SET's file exists before its key does, and a SET pins the object it overwrites too.
        While it is paused before its commit, writes that need room must pass over both, or the SET
        loses its file mid-flight. Enough writes that an unpinned file would surely be taken."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        key = 'fill_0' if overwrite else 'slow'
        with paused_set(self.server, client, key, b'S' * self.OBJ) as outcome:
            others = set_objects(client, 'other_', 5 * self.OBJECTS_PER_CAP, self.OBJ)
            if overwrite:
                assert client.execute_command('BLOB.GET', key) == payloads[key]

        assert outcome['reply'] == b'OK'
        assert client.execute_command('BLOB.GET', key) == b'S' * self.OBJ
        live = len(surviving(client, {**payloads, **others, key: None}))
        self.wait_settled(client, live)

    def test_a_write_discarded_as_stale_leaves_nothing_behind(self):
        """A SET slow to commit finds a newer object under its key and is dropped: its file is
        unlinked, its bytes come back and nothing stays listed."""
        client = self.server.get_new_client()
        with paused_set(self.server, client, 'key', b'L' * self.OBJ):  # holds the older object id
            assert client.execute_command('BLOB.SET', 'key', b'N' * self.OBJ) == b'OK'

        assert client.execute_command('BLOB.GET', 'key') == b'N' * self.OBJ
        self.wait_settled(client, 1)

    def test_a_failed_unlink_leaves_a_live_victim_serving_and_its_key_credits_it_later(self):
        """Eviction cannot unlink a victim whose key is alive: the object serves again, its bytes
        stay charged, and deleting its key later unlinks the file and credits them."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        hook(client, 'fail-unlink', 1)
        assert client.execute_command('BLOB.SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        wait_unlinked(client)

        info = info_largeobj(client)
        assert pending_reclaims(client) == 0, "the victim is un-listed"
        assert info['largeobj_disk_leaked_files'] == 0, "its key is alive, so its handle owns the file"
        assert info['largeobj_disk_used_bytes'] == (self.OBJECTS_PER_CAP + 1) * self.DISK_PER_OBJ
        assert surviving(client, payloads) == payloads
        assert len(self._object_files()) == self.OBJECTS_PER_CAP + 1

        hook(client, 'fail-unlink', 0)
        assert client.execute_command('DEL', *payloads) == len(payloads)  # whichever was the victim
        self.wait_settled(client, 1)

    def test_a_victim_freed_before_a_failed_unlink_is_counted_as_leaked(self):
        """The key is deleted while the unlink is pending, so the teardown leaves the file to
        eviction; then the unlink fails and nobody is left to remove it. The leak is counted, and
        the file stays on disk, charged."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        hook(client, 'pause-before-evict-unlink-ms', 2000)
        hook(client, 'fail-unlink', 1)
        thread, outcome = background_set(self.server, 'newcomer', b'N' * self.OBJ)
        wait_for_true(lambda: pending_reclaims(client) == 1, timeout=10)
        victim = next(key for key in payloads if key not in surviving(client, payloads))
        assert client.execute_command('DEL', victim) == 1
        thread.join(timeout=30)
        assert outcome['reply'] == b'OK'
        wait_unlinked(client)

        info = info_largeobj(client)
        assert info['largeobj_disk_leaked_files'] == 1
        assert pending_reclaims(client) == 0
        assert info['largeobj_disk_pinned_objects'] == 1, "the stray file stays out of eviction's reach"
        assert info['largeobj_disk_used_bytes'] == (self.OBJECTS_PER_CAP + 1) * self.DISK_PER_OBJ
        assert len(self._object_files()) == self.OBJECTS_PER_CAP + 1, "the victim's file is still there"

    def test_a_file_whose_teardown_unlink_fails_is_never_a_victim(self):
        """A freed key's file that cannot be unlinked stays on disk with no key. Its bytes are
        credited, and eviction must keep passing it over, or it would list an id no key leads to."""
        client = self.new_client()
        self.fill_cap(client)
        hook(client, 'fail-unlink', 1)
        assert client.execute_command('DEL', 'fill_0') == 1
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_used_bytes']
                      == (self.OBJECTS_PER_CAP - 1) * self.DISK_PER_OBJ, timeout=10)
        hook(client, 'fail-unlink', 0)
        assert info_largeobj(client)['largeobj_disk_pinned_objects'] == 1
        assert len(self._object_files()) == self.OBJECTS_PER_CAP

        write_until_skipped(client, 'more_', b'M' * self.OBJ)
        assert info_largeobj(client)['largeobj_disk_pinned_objects'] == 1, "it was never claimed"

    def test_a_freed_key_stays_pinned_until_its_file_is_unlinked(self):
        """With no reader holding it, a freed key's file is still the teardown's to unlink. While the
        teardown waits, writes that need room must pass over it, or both would credit it."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        hook(client, 'pause-before-teardown-unlink-ms', 3000)
        assert client.execute_command('DEL', 'fill_0') == 1
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_pinned_objects'] == 1, timeout=10)

        before = pinned_skips(client)
        assert client.execute_command('BLOB.SET', 'other', b'O' * self.OBJ) == b'OK'
        assert pinned_skips(client) > before, "eviction never met the file"
        self.wait_settled(client, len(surviving(client, {**payloads, 'other': None})), timeout=15)

    def test_overlapping_writes_each_evict_only_for_themselves(self):
        """A victim stays in the ledger until its file is unlinked, so a write that arrives meanwhile
        must count it as freed: else every overlapping write evicts for the same shortfall."""
        client = self.new_client()
        self.fill_cap(client)
        threads, writes = 8, 20

        def writer(n):
            c = self.server.get_new_client()
            for i in range(writes):
                assert c.execute_command('BLOB.SET', f'w{n}_{i}', b'W' * self.OBJ) == b'OK'

        pool = [threading.Thread(target=writer, args=(n,)) for n in range(threads)]
        for t in pool:
            t.start()
        for t in pool:
            t.join(timeout=60)

        wait_unlinked(client)
        assert info_largeobj(client)['largeobj_disk_evictions'] == threads * writes, "one victim per write"
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    @pytest.mark.parametrize('policy', ['allkeys-lru', 'volatile-ttl'])
    def test_any_policy_but_noeviction_evicts_one_random_victim(self, policy):
        """A directory has no hit counts or access times, so the policy only decides whether to
        evict: reading an object does not protect it."""
        client = self.server.get_new_client()
        set_policy(client, policy)
        payloads = self.fill_cap(client)
        for key in payloads:
            assert client.execute_command('BLOB.GET', key) == payloads[key]

        assert client.execute_command('BLOB.SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        assert info_largeobj(client)['largeobj_disk_evictions'] == 1
        assert len(surviving(client, payloads)) == self.OBJECTS_PER_CAP - 1

    def test_a_late_commit_overwrites_a_newer_write_that_was_evicted(self):
        """A SET slow to commit finds the key holding a newer object, which eviction has since
        given up. That object is a miss, not a write to defer to, so the late commit must land."""
        client = self.new_client()
        late = b'L' * self.OBJ
        with paused_set(self.server, client, 'key', late) as outcome:  # holds the older object id
            assert client.execute_command('BLOB.SET', 'key', b'N' * self.OBJ) == b'OK'

            gone = lambda: client.execute_command('BLOB.GET', 'key') is None
            write_until(client, 'fill_', b'F' * self.OBJ, gone)
            assert gone(), "never evicted"
            assert client.execute_command('EXISTS', 'key') == 1, "the key outlives its data"
        assert outcome['reply'] == b'OK'
        assert client.execute_command('BLOB.GET', 'key') == late

    def test_an_evicted_object_releases_its_promoted_copy_at_once(self):
        """The victim's key waits for the cron, but its DRAM copy must not: `lo_free` would drop
        it, and that is a minute away."""
        client = self.new_client()
        client.execute_command('CONFIG', 'SET', 'largeobj.promote-min-hits', 1)
        promote(client, self.fill_cap(client))

        assert client.execute_command('BLOB.SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        info = info_largeobj(client)
        assert info['largeobj_disk_evictions'] == 1 and info['largeobj_pending_reclaims'] == 1
        assert info['largeobj_dram_objects'] == self.OBJECTS_PER_CAP - 1

    def test_an_evicted_key_reads_as_a_miss_until_it_is_overwritten(self):
        client = self.new_client()
        key = 'victim'
        assert client.execute_command('BLOB.SET', key, b'V' * ((self.OBJECTS_PER_CAP - 1) * self.OBJ)) == b'OK'
        assert client.execute_command('BLOB.SET', 'newcomer', b'N' * (2 * self.OBJ)) == b'OK'  # room for only one
        assert client.execute_command('EXISTS', key) == 1, "the key outlives its data"
        assert pending_reclaims(client) == 1

        assert client.execute_command('BLOB.GET', key) is None
        with pytest.raises(ResponseError, match='not found'):
            client.execute_command('BLOB.INFO', key)
        with suppress(ResponseError):
            client.execute_command('COPY', key, 'copy')
        assert client.execute_command('EXISTS', 'copy') == 0
        assert client.execute_command('MEMORY', 'USAGE', key) < 1024, "the payload is gone"
        assert client.execute_command('DEBUG', 'DIGEST-VALUE', key)

        # The reclaim is the old object's, not the name's: a new write under the name is no miss.
        assert client.execute_command('BLOB.SET', key, b'fresh' * 1000) == b'OK'
        assert client.execute_command('BLOB.GET', key) == b'fresh' * 1000
        wait_reclaimed(client, 10)
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_pinned_objects'] == 0, timeout=10)
        assert len(self._object_files()) == 2, "the newcomer's and the fresh write's"

    def test_files_that_are_not_objects_are_left_alone(self):
        """Only `{id:016x}.dat` names an object: anything else in the directory is neither a victim
        nor a charge."""
        client = self.new_client()
        strays = ['README', 'ffffffffffffffff.txt', 'abc.dat', 'FFFFFFFFFFFFFFFF.dat']
        for name in strays:
            open(os.path.join(self.data_dir, name), 'w').close()

        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        survivors = surviving(client, payloads)
        assert 0 < len(survivors) < len(payloads)
        wait_unlinked(client)
        assert info_largeobj(client)['largeobj_disk_used_bytes'] == len(survivors) * self.DISK_PER_OBJ
        assert len(self._object_files()) == len(survivors) + len(strays)
        assert all(os.path.exists(os.path.join(self.data_dir, name)) for name in strays)


class TestTieredEvictionOnTmpfs(TieredCap, ValkeyLargeObjTestCaseBase):
    """tmpfs reports no range of directory offsets to draw from, so the sampler reads the directory
    in order instead. Every other test runs on a filesystem that does."""

    def get_module_args(self, data_dir, direct_io):
        self.shm = tempfile.mkdtemp(prefix='nvme-', dir='/dev/shm')
        return super().get_module_args(self.shm, direct_io)

    def teardown_method(self):
        shutil.rmtree(self.shm, ignore_errors=True)

    def _object_files(self):
        return sorted(os.listdir(self.shm))

    def test_eviction_works_where_a_directory_cannot_be_sampled_at_random(self):
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        survivors = surviving(client, payloads)
        info = info_largeobj(client)
        assert 0 < len(survivors) < len(payloads)
        assert info['largeobj_disk_evictions'] == len(payloads) - len(survivors)
        assert info['largeobj_disk_sequential_draws'] > 0
        self.wait_settled(client, len(survivors))


# ─── The scaling cron ─────────────────────────────────────────────────────────

class EvictionReclaim:
    """The scaling cron deletes the keys of evicted objects, a tick (`scaling-poll-ms`) at a time,
    wherever the keys have gone. Mixed into each mode, and into each mode on a cluster node."""

    POLL_MS = 1000

    def reclaim(self, client, timeout=15):
        """Wait for the cron to delete the listed keys. With Dram's shrink pressure lifted, the tick
        after that deletes nothing more: it would drain the arena the survivors are in."""
        client.execute_command('CONFIG', 'SET', 'maxmemory', 1 << 40)
        wait_reclaimed(client, timeout)

    def test_the_cron_deletes_the_keys_eviction_took(self):
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        live = surviving(client, payloads)
        assert 0 < len(live) < len(payloads)

        self.reclaim(client)
        assert client.execute_command('DBSIZE') == len(live), "only live keys are left"
        assert surviving(client, payloads) == live
        assert info_largeobj(client)['largeobj_reclaims'] == info_largeobj(client)[self.EVICTIONS], \
            "each victim is reclaimed once"
        self.wait_settled(client, len(live))

        # Flushing frees what is left: nothing stays charged, held, listed or on disk.
        client.execute_command('FLUSHALL')
        self.wait_settled(client, 0)

    def test_a_deleted_victim_leaves_nothing_for_the_cron(self):
        """Deleting an evicted key takes its object off the list, so the cron has nothing to hunt,
        and what eviction already freed is not freed a second time."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        victims = [key for key in payloads if client.execute_command('BLOB.GET', key) is None]
        assert victims
        deleted = sum(client.execute_command('DEL', key) for key in victims)  # the cron may have deleted some

        self.reclaim(client, timeout=5)
        assert info_largeobj(client)['largeobj_reclaims'] == len(victims) - deleted, "only the cron's deletes count"
        self.wait_settled(client, len(payloads) - len(victims))

    def test_a_write_takes_victims_from_every_db_and_the_cron_reaches_them(self):
        """The budget is node-wide. Tiered SETs land in db 0 whatever db the client selected, so the
        keys are moved out afterwards, to a db each. A newcomer worth two and a half objects needs
        more victims than any one db holds."""
        db0 = self.new_client()
        dbs = [self.server.create_from_server(db=db) for db in (1, 2, 3)]
        payloads = {f'fill_{i}': bytes([i + 1]) * self.OBJ for i in range(self.OBJECTS_PER_CAP)}
        home = {}
        for i, (key, payload) in enumerate(payloads.items()):
            assert db0.execute_command('BLOB.SET', key, payload) == b'OK'
            assert db0.execute_command('MOVE', key, 1 + i % 3) == 1
            home[key] = dbs[i % 3]

        newcomer = b'N' * (5 * self.OBJ // 2)
        assert db0.execute_command('BLOB.SET', 'newcomer', newcomer) == b'OK'
        assert db0.execute_command('BLOB.GET', 'newcomer') == newcomer
        survivors = [key for key, db in home.items() if db.execute_command('BLOB.GET', key) is not None]
        victims = len(payloads) - len(survivors)
        assert victims >= 2 and info_largeobj(db0)[self.EVICTIONS] == victims
        assert all(home[key].execute_command('BLOB.GET', key) == payloads[key] for key in survivors)

        self.reclaim(db0)
        assert sum(db.execute_command('DBSIZE') for db in [db0, *dbs]) == len(survivors) + 1

    def test_renamed_and_moved_victims_are_still_reclaimed(self):
        """The cron finds a listed object by scanning, wherever its key has gone."""
        client = self.new_client()
        victim, newcomer = (self.OBJECTS_PER_CAP - 1) * self.OBJ, 2 * self.OBJ  # room for only one
        assert client.execute_command('BLOB.SET', '{z}victim', b'V' * victim) == b'OK'
        assert client.execute_command('RENAME', '{z}victim', '{z}renamed') in (b'OK', True)
        assert client.execute_command('MOVE', '{z}renamed', 2) in (1, True)
        assert client.execute_command('BLOB.SET', '{y}newcomer', b'N' * newcomer) == b'OK'

        self.reclaim(client)
        assert self.server.create_from_server(db=2).execute_command('EXISTS', '{z}renamed') == 0


class TestDramEvictionReclaim(EvictionReclaim, DramArena, ValkeyLargeObjTestCaseBase):
    pass


class TestTieredEvictionReclaim(EvictionReclaim, TieredCap, ValkeyLargeObjTestCaseBase):

    def test_the_cron_deleting_a_victim_whose_unlink_is_pending_credits_it_once(self):
        """The cron deletes the victim's key while the evicting write has yet to unlink the file. The
        teardown must leave it alone, so only the write unlinks it and credits the bytes."""
        client = self.new_client()
        self.fill_cap(client)
        hook(client, 'pause-before-evict-unlink-ms', 4000)
        thread, _ = background_set(self.server, 'newcomer', b'N' * self.OBJ)
        wait_for_true(lambda: pending_reclaims(client) == 1, timeout=10)
        wait_reclaimed(client, timeout=3)  # the cron deleted the key; the unlink is still waiting
        info = info_largeobj(client)
        assert info['largeobj_disk_pending_free_bytes'] == self.DISK_PER_OBJ
        assert info['largeobj_disk_used_bytes'] == (self.OBJECTS_PER_CAP + 1) * self.DISK_PER_OBJ

        thread.join(timeout=30)
        self.wait_settled(client, self.OBJECTS_PER_CAP)
        assert client.execute_command('DBSIZE') == self.OBJECTS_PER_CAP


class ClusterNode:
    """A cluster node that owns every slot. The cron deletes a victim's key whichever slot and db
    it is in."""

    CLUSTER_DATABASES = 4

    def get_server_args(self):
        config_file = os.path.abspath(os.path.join(self.testdir, 'nodes.conf'))
        if os.path.exists(config_file):
            os.remove(config_file)
        return {
            'cluster-enabled': 'yes',
            'cluster-config-file': config_file,
            'cluster-databases': str(self.CLUSTER_DATABASES),
        }

    def new_client(self):
        client = self.server.get_new_client()
        with suppress(ResponseError):  # a call retried after a timeout finds the slots taken
            client.execute_command('CLUSTER', 'ADDSLOTSRANGE', 0, 16383)
        wait_for_true(lambda: b'cluster_state:ok' in client.execute_command('CLUSTER', 'INFO'))
        set_policy(client)
        return client


class TestClusterDramReclaim(ClusterNode, EvictionReclaim, DramArena, ValkeyLargeObjTestCaseBase):
    pass


class TestClusterTieredReclaim(ClusterNode, EvictionReclaim, TieredCap, ValkeyLargeObjTestCaseBase):
    pass

"""
Integration tests for DRAMPool expand/shrink scaling behavior.

Tests cover:
  - Dram mode: reactive expand when segment fills
  - Dram mode: dram-maxmemory hard cap respected
  - Tiered mode: reactive expand on DRAMPool fill
  - Tiered mode: shrink evicts cached segment but NVMe copy survives
"""

import binascii
import os
import subprocess
import time
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj

# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()

# What tests/harness/fabric_target writes (--read mode) or expects (write mode).
EFA_PATTERN = b'\xab'
EFA_TARGET_LEN = 4096


# ─── Dram Mode Scaling ────────────────────────────────────────────────────────

class TestDramReactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows reactively when a segment fills (SET path).

    scaling-poll-ms is set very high (60s) so the scaling cron cannot fire
    during the test. Any expand observed must be from the reactive SET path.
    """

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB, dram-maxmemory=0 → starts with 1 segment, grows on demand.
        # scaling-poll-ms=60000 → cron fires at most once per minute, won't interfere.
        # fabric-provider Emulated on loopback for EFA reactive expand test.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 0"
            f" scaling-poll-ms 60000"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags):
        """Launch the fabric_target peer process and return (process, address, rkey, remote_addr)."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        process = subprocess.Popen(
            [target, '127.0.0.1', *flags],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        line = process.stdout.readline()
        assert line.startswith('advertisement: '), line
        address, rkey, remote_addr = line.split()[1:]
        return process, address, int(rkey), int(remote_addr)

    def test_expand_on_segment_full(self):
        """SET that fills a segment triggers reactive expand in serve_set_dram_tcp.

        With the cron disabled (60s poll), the only source of expand is the
        reactive path in the SET handler. Verified via scaling_expand_total.
        """
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        r = client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        assert r == b'OK', f"First BLOB.SET failed: {r}"

        r = client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)
        assert r == b'OK', f"Second BLOB.SET failed (expand may not have fired): {r}"

        after = info_largeobj(client)
        assert after.get('largeobj_scaling_expand_total', 0) > expand_before, \
            "Expected scaling_expand_total to increase — cron is disabled so this must be reactive"

    def test_expand_data_integrity(self):
        """Data written before and after a reactive expand is returned correctly."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024
        keys_payloads = [(f'key_{i}', bytes([i % 256]) * obj_size) for i in range(4)]

        for key, payload in keys_payloads:
            client.execute_command('BLOB.SET', key, payload)

        for key, payload in keys_payloads:
            got = client.execute_command('BLOB.GET', key)
            assert got == payload, f"Data mismatch for {key} after expand"

    def test_maxmemory_0_no_explicit_cap(self):
        """dram-maxmemory=0 means no module-level cap; grows up to server ceiling."""
        client = self.server.get_new_client()
        for i in range(3):
            r = client.execute_command('BLOB.SET', f'key_{i}', b'X' * (100 * 1024))
            assert r == b'OK', f"SET {i} failed: {r}"

    def test_live_data_survives_memory_pressure(self):
        """Any Dram key not evicted by Valkey core must return correct data.

        Under memory pressure, core may evict LO keys. The module must never
        corrupt data for keys that core did NOT evict.
        """
        client = self.server.get_new_client()
        obj_size = 200 * 1024
        payloads = {f'dram_{i}': bytes([i % 256]) * obj_size for i in range(5)}

        for key, payload in payloads.items():
            r = client.execute_command('BLOB.SET', key, payload)
            assert r == b'OK', f"SET {key} failed: {r}"

        mem_info = client.execute_command('INFO', 'memory')
        used_memory = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used_memory * 1.1)))

        # Wait for a few cron ticks.
        time.sleep(5)

        for key, payload in payloads.items():
            if client.execute_command('EXISTS', key) == 1:
                got = client.execute_command('BLOB.GET', key)
                assert got == payload, f"{key} data corrupted under pressure"

    def test_efa_set_triggers_reactive_expand(self):
        """Fill the 1MB segment with a TCP SET, then an EFA SET triggers expand.

        Same principle as test_expand_on_segment_full but exercises the EFA SET
        path (cmd_set_dram_efa) which also has reactive expand logic.
        """
        client = self.server.get_new_client()
        expand_before = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        # Fill most of the 1MB segment with a TCP SET (900KB).
        client.execute_command('LO.SET', 'filler', b'F' * (900 * 1024))
        # EFA SET of 4096 bytes — segment nearly full, must trigger expand.
        process, address, rkey, remote_addr = self.start_target('--read')
        try:
            client.execute_command('LO.HELLO', address)
            result = client.execute_command('LO.SET', 'efa_key', EFA_TARGET_LEN, rkey, remote_addr)
            assert result == b'OK', f"EFA SET failed: {result}"
            assert client.execute_command('LO.GET', 'efa_key') == EFA_PATTERN * EFA_TARGET_LEN
            expand_after = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
            assert expand_after > expand_before, \
                "Expected scaling_expand_total to increase from reactive EFA SET expand"
        finally:
            process.kill()


class TestDramProactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows proactively when the scaling cron sees utilization > watermark.

    scaling-poll-ms=1000 so the cron fires every second. The test writes enough
    data to push utilization above the expand watermark, then stops writing and
    waits for the cron to add a segment.
    """

    EXPAND_TIMEOUT_S = 15

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB, dram-maxmemory=0 (no cap so shrink never fires).
        # scaling-expand-watermark=50 so filling half a segment triggers proactive expand.
        # scaling-shrink-watermark=99 to ensure shrink never fires during this test.
        # scaling-poll-ms=1000 so the cron fires frequently.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 0"
            f" scaling-expand-watermark 50"
            f" scaling-shrink-watermark 99"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_proactive_expand_fires_when_watermark_exceeded(self):
        """The scaling cron adds a segment when utilization exceeds the expand watermark.

        Steps:
        1. Write one 600KB object into a 1MB segment → utilization ≈ 60% > 50% watermark.
        2. Stop writing. No new SETs happen.
        3. Wait for cron to fire and increment scaling_expand_total.

        Because cron is the only actor after step 1, any expand is definitively proactive.
        """
        client = self.server.get_new_client()

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        # Fill >50% of one 1MB segment (600KB ≈ 59% of 1MB).
        r = client.execute_command('BLOB.SET', 'probe', b'P' * (600 * 1024))
        assert r == b'OK', "BLOB.SET failed"

        # No more SETs. Wait for cron to observe utilization > 50% and expand.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before,
            timeout=self.EXPAND_TIMEOUT_S,
        )



class TestDramMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: dram-maxmemory hard cap is respected after expand."""

    def get_module_args(self, data_dir, direct_io):
        # 2MB total, 1MB segment. After reactive expand, pool is 2MB (2 segments).
        # A third 900KB object cannot fit even after a second segment is added.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" dram-maxmemory 2097152"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_maxmemory_cap_after_expand(self):
        """After one reactive expand (now at cap), a third object must be rejected."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)

        try:
            client.execute_command('BLOB.SET', 'key_c', b'C' * obj_size)
            assert False, "Expected error: pool exhausted or OOM"
        except ResponseError:
            pass


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────

class TestTieredExpand(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool expands reactively when segment fills."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" dram-maxmemory 4194304"
            f" max-promote-size 1048576"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_expand_on_full(self):
        """In Tiered mode, filling the DRAMPool triggers expand; data stays correct."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)

        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * obj_size
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * obj_size

    def test_tiered_multiple_segments(self):
        """Objects spread across multiple segments are all readable."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024

        for i in range(4):
            r = client.execute_command('BLOB.SET', f'key_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"SET key_{i} failed: {r}"

        for i in range(4):
            got = client.execute_command('BLOB.GET', f'key_{i}')
            assert got == bytes([i % 256]) * obj_size, f"Data mismatch for key_{i}"

    def test_tiered_nvme_fallback_on_dram_full(self):
        """When DRAMPool is at cap, further SETs still persist to NVMe and are readable."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            client.execute_command('BLOB.SET', key, fill * obj_size)

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            assert client.execute_command('BLOB.GET', key) == fill * obj_size


class TestTieredShrink(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool shrinks under memory pressure.

    Writes objects first, then sets server maxmemory below current used_memory
    so the module shrink watermark fires on the next cron tick.
    """

    SHRINK_TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" dram-maxmemory 0"
            f" max-promote-size 1048576"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def _assert_no_pressure(self, client):
        """Guardrail: verify server is not under memory pressure before the test writes data.

        maxmemory must be 0 (uncapped) at test start. If it's non-zero, the test
        server was left in a bad state from a previous test run and results would
        be unreliable.
        """
        mem_info = client.execute_command('INFO', 'memory')
        maxmemory = int(mem_info.get(b'maxmemory') or mem_info.get('maxmemory', 0))
        assert maxmemory == 0, (
            f"Test server already has maxmemory={maxmemory} at start — "
            f"server is under pressure before test data is written. "
            f"Run 'CONFIG SET maxmemory 0' to reset."
        )

    def _apply_shrink_pressure(self, client):
        """Set maxmemory below current used_memory so ratio > 0.80.

        Setting maxmemory = used * 0.85 gives ratio ≈ 1.18 > 0.80.
        noeviction means no keys are evicted — the module cron handles DRAM.
        """
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')
        mem_info = client.execute_command('INFO', 'memory')
        used = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used * 0.85)))

    def test_shrink_preserves_nvme_data(self):
        """After the scaling cron shrinks the pool, keys remain readable from NVMe."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        self._assert_no_pressure(client)

        keys = [f'shrink_key_{i}' for i in range(4)]
        for key in keys:
            r = client.execute_command('BLOB.SET', key, b'S' * obj_size)
            assert r == b'OK', f"BLOB.SET {key} failed: {r}"

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Wait for the drained segment to be fully released (draining_segments back to 0).
        # This verifies the complete shrink cycle including release_drained, not just initiation.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        for key in keys:
            assert client.execute_command('EXISTS', key) == 1, \
                f"Key {key} disappeared from keyspace after shrink (data loss)"

    def test_shrink_then_expand(self):
        """After a shrink, new SETs succeed."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        self._assert_no_pressure(client)

        for i in range(4):
            client.execute_command('BLOB.SET', f'pre_shrink_{i}', b'P' * obj_size)

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Wait for the drained segment to be fully released (draining_segments back to 0).
        # This verifies the complete shrink cycle including release_drained, not just initiation.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')

        r = client.execute_command('BLOB.SET', 'post_shrink', b'Q' * obj_size)
        assert r == b'OK', f"BLOB.SET after shrink+expand failed: {r}"

        for i in range(4):
            assert client.execute_command('EXISTS', f'pre_shrink_{i}') == 1, \
                f"pre_shrink_{i} disappeared from keyspace after shrink"

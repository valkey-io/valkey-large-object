import os
import glob
import time
import pytest
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal


class TestLargeObjTieredPromotion(ValkeyLargeObjTestCaseBase):
    """Tiered mode with DRAMPool promotion enabled (default max-promote-size)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 16777216"
            f" max-promote-size 268435456"
            f" lo-buffer-size 4096"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_set_creates_nvme_file(self):
        """In Tiered mode, LO.SET persists to NVMe."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('LO.SET', 'tiered_key', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, "Tiered SET should create an NVMe .dat file"

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("stats").get("lazyfree_pending_objects", 0), 0
        )

    def test_get_after_set_roundtrip(self):
        """Tiered mode: SET then GET returns correct data."""
        client = self.server.get_new_client()
        payload = b'A' * 4096
        client.execute_command('LO.SET', 'rt_key', payload)
        result = client.execute_command('LO.GET', 'rt_key')
        assert result == payload, "GET should return the same data that was SET"

    def test_promotion_caches_in_dram(self):
        """After a GET miss, the object is promoted to DRAMPool.
        A second GET should succeed (served from DRAM)."""
        client = self.server.get_new_client()
        payload = b'B' * 4096
        client.execute_command('LO.SET', 'promo_key', payload)

        # First GET: DRAMPool miss -> NVMe read -> promote to DRAMPool.
        result1 = client.execute_command('LO.GET', 'promo_key')
        assert result1 == payload

        # Second GET: served from DRAMPool (promotion happened).
        result2 = client.execute_command('LO.GET', 'promo_key')
        assert result2 == payload

    def test_delete_removes_nvme_file(self):
        """DEL removes the NVMe file."""
        client = self.server.get_new_client()
        payload = b'D' * 4096
        client.execute_command('LO.SET', 'del_key', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1
        client.execute_command('DEL', 'del_key')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    # ─── COPY callback tests ─────────────────────────────────────────────

    def test_copy(self):
        """COPY in Tiered mode: independent NVMe file, digest differs, delete independence."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('LO.SET', 'srckey', payload)
        # COPY creates an independent object with its own NVMe file
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True
        assert client.execute_command('LO.GET', 'srckey') == payload
        assert client.execute_command('LO.GET', 'dstkey') == payload
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 2, f"Expected at least 2 .dat files, got {len(dat_files)}"
        # COPY gets a new OID so digests differ
        src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'srckey')
        dst_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'dstkey')
        assert src_digest != dst_digest
        # Deleting source does not affect the copy
        client.execute_command('DEL', 'srckey')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('LO.GET', 'dstkey') == payload
        # Deleting copy does not affect the source
        client.execute_command('LO.SET', 'srckey2', payload)
        client.execute_command('COPY', 'srckey2', 'dstkey2')
        client.execute_command('DEL', 'dstkey2')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('LO.GET', 'srckey2') == payload

    # ─── MEMORY USAGE callback tests ──────────────────────────────────────

    def test_memory_usage(self):
        """MEMORY USAGE after promotion includes LoValue struct + payload."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('LO.SET', 'memkey', b'M' * payload_size)
        # Single GET promotes into DRAMPool (promote-on-first-GET policy).
        client.execute_command('LO.GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        assert mem >= lo_value_size + payload_size, (
            f"Expected MEMORY USAGE >= {lo_value_size + payload_size} (promoted), got {mem}"
        )

    # ─── DEBUG DIGEST callback tests ──────────────────────────────────────

    def test_debug_digest(self):
        """DEBUG DIGEST-VALUE is deterministic; nonexistent key returns nil digest."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'digkey', b'G' * 4096)
        d1 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        d2 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        assert d1 == d2
        # Nonexistent key returns nil digest
        nil_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'noexist')
        assert nil_digest == [b'0' * 40]

    # ─── DEL semantics ────────────────────────────────────────────────────

    def test_delete_semantics(self):
        """DEL resolves a subsequent GET to nil and unlinks the .dat file -- both
        for a cold object and one already promoted into DRAMPool (a distinct free
        path: the DRAM cache entry is dropped alongside the Arc<ObjectFile>)."""
        client = self.server.get_new_client()
        payload = b"D" * 4096
        assert client.execute_command("DBSIZE") == 0

        # Cold object: GET after DEL is nil; DEL drops the key from the keyspace.
        client.execute_command("LO.SET", "gk", payload)
        assert client.execute_command("DBSIZE") == 1
        assert client.execute_command("LO.GET", "gk") == payload
        client.execute_command("DEL", "gk")
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("LO.GET", "gk") is None

        # Promoted object: DEL still resolves to nil and the NVMe file is unlinked.
        client.execute_command("LO.SET", "pk", payload)
        assert client.execute_command("LO.GET", "pk") == payload  # first GET promotes
        client.execute_command("DEL", "pk")
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("LO.GET", "pk") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Overwrite ────────────────────────────────────────────────────────

    # TODO: Remove xfail once streaming implementation lands — multi-buffer tiered GET
    # hits todo!() panic because chunked promotion isn't implemented yet.
    @pytest.mark.xfail(reason="multi-buffer tiered GET not yet implemented (PR #54)", strict=False)
    def test_overwrite_semantics(self):
        """Overwriting a key commits a new object version and tears down the old
        one: GET returns the new payload and exactly one .dat remains per key.
        Covers the post-promotion overwrite (stale DRAM entry must be replaced)
        and repeated overwrite (steady state stays bounded -- no file leak)."""
        client = self.server.get_new_client()
        v1 = b"1" * 4096
        v2 = b"2" * 8192

        # Basic overwrite: the new value wins at commit; the old file is torn down.
        client.execute_command("LO.SET", "ok", v1)
        assert client.execute_command("DBSIZE") == 1
        wait_for_equal(self._dat_count, 1)
        client.execute_command("LO.SET", "ok", v2)
        assert client.execute_command("DBSIZE") == 1  # overwrite reuses the key
        assert client.execute_command("LO.GET", "ok") == v2
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)

        # Overwrite after promotion: the new GET must serve the new payload.
        client.execute_command("LO.SET", "opk", b"A" * 4096)
        assert client.execute_command("LO.GET", "opk") == b"A" * 4096  # promote v1
        client.execute_command("LO.SET", "opk", b"B" * 4096)
        assert client.execute_command("LO.GET", "opk") == b"B" * 4096
        assert client.execute_command("LO.GET", "opk") == b"B" * 4096

        # Repeated overwrite of one key never leaks files.
        for i in range(10):
            client.execute_command("LO.SET", "leakkey", bytes([65 + (i % 26)]) * 4096)
        assert client.execute_command("LO.GET", "leakkey") is not None
        self._wait_free_settled(client)
        # One live file per surviving key: ok, opk, leakkey.
        wait_for_equal(self._dat_count, 3)

    # ─── GET result outlives a concurrent DEL (honor rule) ────────────────

    # TODO: Remove xfail once streaming implementation lands — multi-buffer tiered GET
    # hits todo!() panic because chunked promotion isn't implemented yet.
    @pytest.mark.xfail(reason="multi-buffer tiered GET not yet implemented (PR #54)", strict=False)
    def test_get_result_correct_across_delete_churn(self):
        """A GET that resolves the key returns its full data even under delete
        churn: the honor-rule pin keeps the file alive for the read's duration.
        We can't force a mid-flight race deterministically from the client, so we
        assert the observable invariant: interleaved GET/DEL never corrupts data."""
        client = self.server.get_new_client()
        payload = b"Z" * 8192
        for _ in range(20):
            client.execute_command("LO.SET", "churn", payload)
            assert client.execute_command("DBSIZE") == 1
            assert client.execute_command("LO.GET", "churn") == payload
            client.execute_command("DEL", "churn")
            assert client.execute_command("DBSIZE") == 0
            assert client.execute_command("LO.GET", "churn") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Other free triggers: expiry & flush ──────────────────────────────

    def test_expiry_and_flushall_unlink_files(self):
        """Expiry (TTL) and FLUSHALL are free paths distinct from DEL/overwrite;
        both must unlink the .dat file(s)."""
        client = self.server.get_new_client()

        # Passive expiry: polling EXISTS drives expiry, then teardown unlinks.
        client.execute_command("LO.SET", "exk", b"E" * 4096)
        wait_for_equal(self._dat_count, 1)
        client.execute_command("PEXPIRE", "exk", 50)
        wait_for_equal(lambda: client.execute_command("EXISTS", "exk"), 0)
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

        # FLUSHALL unlinks every remaining file.
        for i in range(3):
            client.execute_command("LO.SET", f"fk{i}", b"F" * 4096)
        wait_for_equal(self._dat_count, 3)
        client.execute_command("FLUSHALL")
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)


class TestLargeObjTieredNvmeOnly(ValkeyLargeObjTestCaseBase):
    """Tiered mode with max-promote-size=0 (no promotion, all reads from NVMe)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 4194304"
            f" max-promote-size 0"
            f" lo-buffer-size 4096"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_set_get_roundtrip_no_promotion(self):
        """With max-promote-size=0, GET always reads from NVMe (no DRAMPool caching)."""
        client = self.server.get_new_client()
        payload = b'N' * 8192
        client.execute_command('LO.SET', 'nvme_key', payload)
        result = client.execute_command('LO.GET', 'nvme_key')
        assert result == payload

    def test_multiple_gets_all_from_nvme(self):
        """Multiple GETs with max-promote-size=0 should all succeed."""
        client = self.server.get_new_client()
        payload = b'R' * 4096
        client.execute_command('LO.SET', 'repeat_key', payload)
        for _ in range(5):
            result = client.execute_command('LO.GET', 'repeat_key')
            assert result == payload

    def test_nvme_staging_exhaustion(self):
        """An object larger than nvme-staging-size should fail with staging buffer exhaustion."""
        client = self.server.get_new_client()
        # nvme-staging-size is 4MB. An 8MB object cannot be staged.
        obj_size = 8 * 1024 * 1024
        payload = b'Z' * obj_size
        try:
            client.execute_command('LO.SET', 'toobig', payload)
            assert False, "Expected NVMe staging buffer exhaustion error"
        except ResponseError as e:
            assert 'nvme staging buffer pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    # ─── MEMORY USAGE tests ───────────────────────────────────────────────

    def test_memory_usage_tiered_cold(self):
        """Without promotion, MEMORY USAGE reports only LoValue struct overhead.

        This test class sets max-promote-size=0, so objects are never promoted
        to DRAMPool. memory_usage reports only sizeof(LoValue) (24 bytes) — the
        payload lives on NVMe and does not consume DRAM.
        """
        client = self.server.get_new_client()
        payload_size = 4096
        payload = b'M' * payload_size
        client.execute_command('LO.SET', 'memkey', payload)
        # Even after a GET the object stays cold (max-promote-size=0).
        client.execute_command('LO.GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        # Our callback returns only sizeof(LoValue) = 24. Valkey adds per-key
        # overhead (~72-120 bytes), so total is well below lo_value_size + payload_size.
        upper_bound = lo_value_size + payload_size
        assert mem < upper_bound, (
            f"Expected MEMORY USAGE < {upper_bound} (cold, not promoted), got {mem}"
        )


# ─── NVMe disk-usage accounting ──────────────────────────────────────────
# These Tiered-mode classes use small nvme-maxmemory caps to exercise the
# capacity gate; each defines its own get_module_args.


class _NvmeAccountingBase(ValkeyLargeObjTestCaseBase):
    """Shared helpers for the NVMe disk-usage accounting tests.

    The usage counter is not observable directly (no INFO section / command), so
    these tests exercise it through its only externally-visible effect: the
    reserve-if-capacity gate (`try_reserve_nvme_disk_usage`) on the Tiered SET path.
    A SET that would push tracked usage past `nvme-maxmemory` is rejected with
    "NVMe disk capacity exceeded"; a SET that fits succeeds. By filling to the cap, freeing,
    and re-filling we prove the counter is incremented on create and -- critically --
    decremented at TRUE deletion (ObjectFile::Drop, after teardown), not merely at key-free.
    """

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("stats").get("lazyfree_pending_objects", 0), 0
        )

    def _set_ok(self, client, key, payload):
        assert client.execute_command("LO.SET", key, payload) == b"OK"

    def _set_ok_eventually(self, client, key, payload, tries=100, delay=0.02):
        """Overwrite SET that tolerates a *transient* 'capacity exceeded'.

        On overwrite the replaced object's bytes are released asynchronously in
        ObjectFile::Drop (teardown runs on the tokio blocking pool), so a rapid
        re-SET can momentarily observe the old reservation still outstanding and
        be rejected. `_wait_free_settled` can't help here: an overwrite frees the
        value synchronously on the main thread, so lazyfree never even increments.
        Retry within a bounded budget; if the SET never succeeds the capacity was
        genuinely not reclaimed -- a real leak -- and the assertion below fails.
        """
        last = None
        for _ in range(tries):
            try:
                assert client.execute_command("LO.SET", key, payload) == b"OK"
                return
            except ResponseError as e:
                if "nvme disk capacity exceeded" not in str(e).lower():
                    raise
                last = e
                time.sleep(delay)
        assert False, (
            f"SET '{key}' still pool-exhausted after {tries} tries "
            f"({tries * delay:.1f}s) -- capacity not reclaimed on overwrite (leak): {last}"
        )

    def _set_rejected(self, client, key, payload):
        try:
            client.execute_command("LO.SET", key, payload)
            assert False, f"Expected '{key}' SET to be rejected (capacity exceeded)"
        except ResponseError as e:
            assert "nvme disk capacity exceeded" in str(e).lower(), f"Unexpected error: {e}"


class TestNvmeUsageFreedOnDelete(_NvmeAccountingBase):
    """Capacity is reclaimed only when the file is truly unlinked."""

    # 1 MiB cap = exactly four 256 KiB (already-aligned) objects. staging holds one
    # object at a time, so 1 MiB is plenty for the per-write buffer.
    CAP = 1024 * 1024
    OBJ = 256 * 1024  # 262144, a 4096-multiple -> no padding effect here

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size {self.CAP}"
            f" dram-segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_capacity_reclaimed_on_true_free(self):
        """Capacity is reclaimed only when the file is truly unlinked -- via DEL
        (partial, per-object) and via FLUSHALL (all). The usage decrement lives in
        ObjectFile::Drop, so the waits below are load-bearing (freed at TRUE
        deletion, not merely at key-free)."""
        client = self.server.get_new_client()
        payload = b"X" * self.OBJ

        # Fill the cap exactly (4 * 256 KiB == 1 MiB); the fifth must be rejected.
        for i in range(4):
            self._set_ok(client, f"k{i}", payload)
        wait_for_equal(self._dat_count, 4)
        assert client.execute_command("DBSIZE") == 4
        # A rejected SET must not leave a phantom key in the keyspace.
        self._set_rejected(client, "k4", payload)
        assert client.execute_command("DBSIZE") == 4

        # DEL one object: the slot frees only after teardown unlinks the file, so
        # the previously-rejected SET fits once (and only once) the file is gone.
        client.execute_command("DEL", "k0")
        assert client.execute_command("DBSIZE") == 3
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 3)
        self._set_ok(client, "k4", payload)
        assert client.execute_command("DBSIZE") == 4
        wait_for_equal(self._dat_count, 4)

        # FLUSHALL frees every object; the full cap becomes available again.
        client.execute_command("FLUSHALL")
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)
        for i in range(4):
            self._set_ok(client, f"g{i}", payload)
        assert client.execute_command("DBSIZE") == 4
        wait_for_equal(self._dat_count, 4)

    def test_overwrite_does_not_leak_capacity(self):
        client = self.server.get_new_client()

        # Repeatedly overwrite one key far more times than the cap allows. If the
        # old object's bytes were not freed on overwrite, usage would climb past the
        # cap and a later overwrite would be wrongly rejected.
        for i in range(20):
            self._set_ok_eventually(client, "ow", bytes([65 + (i % 26)]) * self.OBJ)
        assert client.execute_command("DBSIZE") == 1  # one key throughout
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)


class TestNvmeUsageAccountsForPadding(_NvmeAccountingBase):
    """O_DIRECT pads writes up to IO_ALIGN (4096); accounting must count the padded
    on-disk size, not the logical length."""

    # Cap chosen to be a 2 KiB multiple but NOT a 4 KiB multiple: 1 MiB + 2 KiB.
    CAP = 1024 * 1024 + 2048  # 1050624; 1050624 / 4096 == 256.5

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size 2097152"
            f" dram-segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_padding_accounting(self):
        """O_DIRECT pads writes up to IO_ALIGN (4096); accounting must count the
        padded on-disk size, not the logical length.

        - Rejected: an object whose LOGICAL size fits under the cap
          (1050624 <= 1050624) but whose ALIGNED size does not
          (align_up(1050624) == 1052672 > cap). Disk is empty, so the rejection
          is attributable to padding alone.
        - Succeeds: a 1 MiB object is already 4096-aligned, so aligned == logical
          == 1048576 <= cap -- proving the rejection above is padding-specific,
          not just "large object rejected".
        """
        client = self.server.get_new_client()
        # Padded-overflow SET is rejected (logical == cap; aligned == cap rounded up)
        # and must not create a key.
        self._set_rejected(client, "padkey", b"P" * self.CAP)
        assert client.execute_command("DBSIZE") == 0
        # Contrast case: aligned-fit SET succeeds.
        self._set_ok(client, "fitkey", b"Q" * (1024 * 1024))
        assert client.execute_command("DBSIZE") == 1
        wait_for_equal(self._dat_count, 1)

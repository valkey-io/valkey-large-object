import os
import glob
import time
import threading
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true


class TestLargeObjTieredPromotion(ValkeyLargeObjTestCaseBase):
    """Tiered mode with DRAMPool promotion enabled (default max-promote-size)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 268435456"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
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
        """Tiered mode: SET then GET returns correct data, including multi-chunk objects.
        Validates FileHeader presence and data offset in the .dat file."""
        client = self.server.get_new_client()
        # chunk-size is 4096 in this class.
        # Single chunk: 4096 bytes.
        payload_single = b'A' * 4096
        client.execute_command('LO.SET', 'rt_single', payload_single)
        assert client.execute_command('LO.GET', 'rt_single') == payload_single
        # Multi-chunk with partial last chunk: 8192 + 1 = 8193 → 3 chunks.
        payload_multi = b'M' * 8193
        client.execute_command('LO.SET', 'rt_multi', payload_multi)
        # GET via serve-and-discard (first GET triggers promotion, verify data).
        result = client.execute_command('LO.GET', 'rt_multi')
        assert result == payload_multi
        # Second GET from DRAM after promotion.
        assert client.execute_command('LO.GET', 'rt_multi') == payload_multi
        # Validate FileHeader on disk: first 4 bytes should be b"LOBJ",
        # data starts at offset 4096.
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected at least one .dat file"
        # Check the most recent file (highest OID = last in sorted hex names).
        with open(dat_files[-1], 'rb') as f:
            header_page = f.read(4096)
            assert header_page[:4] == b'LOBJ', "FileHeader magic mismatch"
            assert header_page[4] == 1, "FileHeader version mismatch"
            # Data starts at offset 4096.
            f.seek(4096)
            data_on_disk = f.read(len(payload_multi))
            assert data_on_disk == payload_multi, "On-disk data at offset 4096 mismatch"

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
        """MEMORY USAGE after promotion returns a non-zero value for the key."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('LO.SET', 'memkey', b'M' * payload_size)
        # Single GET promotes into DRAMPool (promote-on-first-GET policy).
        client.execute_command('LO.GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        assert mem > 0, f"Expected non-zero MEMORY USAGE after promotion, got {mem}"

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

    # ─── Streaming tests ─────────────────────────────────────────────────
    # CRC integrity, delete-during-SET, nvme-maxmemory.
    # These use the same config as the promotion tests above.

    def test_tiered_delete_during_set(self):
        """Deterministic delete-during-SET: the test-pause hook freezes the
        SET's tokio task after NVMe data writes complete but before set_finalize
        commits the key. While the SET is paused we fire a DEL from a second
        client, guaranteeing the interleaving:
            Thread A (tokio):  write chunks → [PAUSE] → set_finalize (commits)
            Thread B (main) :                  DEL key (removes v1)
        The DEL removes the existing v1. The paused SET then resumes: set_finalize
        finds no existing key (DEL cleared it), so it commits payload2 as a fresh
        key. No crash, no corruption, and GET returns payload2."""
        client = self.server.get_new_client()
        del_client = self.server.get_new_client()
        payload = b'D' * 32768
        client.execute_command('LO.SET', 'delset_key', payload)
        assert client.execute_command('LO.GET', 'delset_key') == payload
        # Enable the test hook: pause tiered SET for 2s after writing chunks.
        client.execute_command(
            'CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '2000'
        )
        payload2 = b'E' * 32768
        set_result = [None]
        set_error = [None]
        def background_set():
            try:
                # This SET blocks for ~2s (paused after NVMe write, before commit).
                set_result[0] = client.execute_command(
                    'LO.SET', 'delset_key', payload2
                )
            except Exception as e:
                set_error[0] = e
        t = threading.Thread(target=background_set)
        t.start()
        # Wait long enough for the SET to begin its NVMe writes and enter the
        # pause window (chunk writes are fast for 32KB at 4KB chunks).
        time.sleep(0.5)
        # DEL fires while SET is paused — deterministically hits the race window.
        del_result = del_client.execute_command('DEL', 'delset_key')
        assert del_result == 1, f"Expected DEL to find key, got {del_result}"
        t.join(timeout=10)
        assert not t.is_alive(), "SET thread did not finish"
        assert set_error[0] is None, f"SET raised: {set_error[0]}"
        # Disable the hook.
        client.execute_command(
            'CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '0'
        )
        # The paused SET's set_finalize sees no existing key (DEL removed v1)
        # and commits payload2 as a fresh key.
        wait_for_equal(
            lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0
        )
        assert client.execute_command('LO.GET', 'delset_key') == payload2

    def test_zero_length_object_rejected(self):
        """LO.SET with zero-length payload is rejected."""
        client = self.server.get_new_client()
        try:
            client.execute_command('LO.SET', 'empty_key', b'')
            assert False, "Expected error for zero-length object"
        except ResponseError as e:
            assert 'object length must be > 0' in str(e).lower(), f"Unexpected: {e}"

    def test_nvme_maxmemory_exhaustion(self):
        """SET that would exceed nvme-maxmemory is rejected."""
        client = self.server.get_new_client()
        # nvme-maxmemory minimum is 1MB. Set to 1MB.
        client.execute_command('CONFIG', 'SET', 'largeobj.nvme-maxmemory', '1048576')
        # Each 256KB object has disk_len = 4096 (header) + 256KB = 266240 bytes.
        # Three fit (798720 < 1MB), fourth exceeds (1064960 > 1MB).
        payload = b'A' * (256 * 1024)
        for i in range(3):
            result = client.execute_command('LO.SET', f'nvme_cap_{i}', payload)
            assert result == b'OK'
        # Fourth SET should fail (would exceed 1MB).
        try:
            client.execute_command('LO.SET', 'nvme_cap_3', payload)
            assert False, "Expected capacity exceeded error from nvme-maxmemory"
        except ResponseError as e:
            assert 'nvme disk capacity exceeded' in str(e).lower(), f"Unexpected error: {e}"

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
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
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

    def test_reject_invalid_buffer_configs(self):
        """CONFIG SET rejects min-buffers-per-op > max-buffers-per-op and
        vice-versa, exercising both validation callbacks."""
        client = self.server.get_new_client()
        # Direction 1: raise min above current max (default max=8).
        client.execute_command('CONFIG', 'SET', 'largeobj.max-buffers-per-op', '2')
        try:
            client.execute_command('CONFIG', 'SET', 'largeobj.min-buffers-per-op', '3')
            assert False, "Expected CONFIG SET rejection (min > max)"
        except ResponseError as e:
            assert 'min-buffers-per-op' in str(e).lower(), f"Unexpected error: {e}"
        # Direction 2: lower max below current min (default min=2).
        try:
            client.execute_command('CONFIG', 'SET', 'largeobj.max-buffers-per-op', '1')
            assert False, "Expected CONFIG SET rejection (max < min)"
        except ResponseError as e:
            assert 'max-buffers-per-op' in str(e).lower(), f"Unexpected error: {e}"

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
    "pool exhausted"; a SET that fits succeeds. By filling to the cap, freeing,
    and re-filling we prove the counter is incremented on create and -- critically --
    decremented at TRUE deletion (ObjectFile::Drop, after teardown), not merely at key-free.
    """

    # Must match storage::FILE_HEADER_SIZE and storage::IO_ALIGN in mod.rs.
    FILE_HEADER_SIZE = 4096
    IO_ALIGN = 4096

    def _align_up(self, n):
        return (n + self.IO_ALIGN - 1) & ~(self.IO_ALIGN - 1)

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
        """Overwrite SET that tolerates a *transient* 'pool exhausted'.

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
                if "pool exhausted" not in str(e).lower():
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
            err = str(e).lower()
            assert "pool exhausted" in err or "capacity exceeded" in err, \
                f"Unexpected error: {e}"


class TestNvmeUsageFreedOnDelete(_NvmeAccountingBase):
    """Capacity is reclaimed only when the file is truly unlinked."""

    # 2 MiB cap. Each 256 KiB object has disk_len = 4096 (header) + 256 KiB = 266240.
    # Seven fit (1863680 < 2 MiB), eighth would exceed (2129920 > 2 MiB).
    CAP = 2 * 1024 * 1024
    OBJ = 256 * 1024

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size {self.CAP}"
            f" segment-size 1048576"
            f" chunk-size {self.OBJ}"
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

        # Fill the cap (7 * 266240 = 1863680 < 2 MiB); the eighth must be rejected.
        for i in range(7):
            self._set_ok(client, f"k{i}", payload)
        wait_for_equal(self._dat_count, 7)
        assert client.execute_command("DBSIZE") == 7
        # A rejected SET must not leave a phantom key in the keyspace.
        self._set_rejected(client, "k7", payload)
        assert client.execute_command("DBSIZE") == 7

        # DEL one object: the slot frees only after teardown unlinks the file, so
        # the previously-rejected SET fits once (and only once) the file is gone.
        client.execute_command("DEL", "k0")
        assert client.execute_command("DBSIZE") == 6
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 6)
        self._set_ok(client, "k7", payload)
        assert client.execute_command("DBSIZE") == 7
        wait_for_equal(self._dat_count, 7)

        # FLUSHALL frees every object; the full cap becomes available again.
        client.execute_command("FLUSHALL")
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)
        for i in range(7):
            self._set_ok(client, f"g{i}", payload)
        assert client.execute_command("DBSIZE") == 7
        wait_for_equal(self._dat_count, 7)

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
    on-disk size (FILE_HEADER_SIZE + align_up(logical_len)), not the logical length."""

    # Cap = FILE_HEADER_SIZE + 1 MiB = 1052672.
    # An aligned 1 MiB object fits exactly: disk_len = 4096 + 1048576 = 1052672 == cap.
    # A non-aligned object of 1048577 bytes does NOT fit:
    #   disk_len = 4096 + align_up(1048577) = 4096 + 1052672 = 1056768 > cap.
    #   The extra 4095 bytes of padding push it over — that IS the padding effect.
    CAP = 4096 + 1024 * 1024  # 1052672

    def get_module_args(self, data_dir, direct_io):
        # segment-size must exceed the largest staged object (object < segment):
        # this test stages 1 MiB and ~1 MiB+2KiB objects, so use 2 MiB segments.
        # nvme-staging-size 4 MiB -> ceil(4MiB / 2MiB) = 2 NVMe staging segments.
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" nvme-staging-size 4194304"
            f" segment-size 2097152"
            f" chunk-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_padding_accounting(self):
        """Prove that alignment padding (not just the header) is accounted for.
        - Aligned 1 MiB object fits exactly: disk_len = 4096 + 1048576 = cap.
        - Non-aligned 1 MiB + 1 byte does NOT fit: align_up rounds up to the
          next 4 KiB boundary, pushing disk_len past the cap. The rejection is
          attributable to the padding bytes, not the header (both objects have
          the same header overhead).
        """
        client = self.server.get_new_client()
        aligned_payload = b"Q" * (1024 * 1024)       # 1048576 — already 4096-aligned
        unaligned_payload = b"P" * (1024 * 1024 + 1)  # 1048577 — needs padding
        # Verify the relationship between payload sizes and nvme-maxmemory cap.
        assert self.FILE_HEADER_SIZE + len(aligned_payload) == self.CAP
        assert self.FILE_HEADER_SIZE + self._align_up(len(unaligned_payload)) > self.CAP
        # Aligned case succeeds: disk_len = 4096 + 1048576 = 1052672 == cap.
        self._set_ok(client, "fitkey", aligned_payload)
        assert client.execute_command("DBSIZE") == 1
        wait_for_equal(self._dat_count, 1)
        # Clean up so the unaligned case has full cap available.
        client.execute_command("DEL", "fitkey")
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)
        # Unaligned case rejected: disk_len = 4096 + 1052672 = 1056768 > cap.
        # Both objects differ by 1 byte logically, but 4095 bytes on disk (padding).
        self._set_rejected(client, "padkey", unaligned_payload)
        assert client.execute_command("DBSIZE") == 0


# ─── Corruption Detection ─────────────────────────────────────────────────
# Each test triggers a server panic by corrupting on-disk data, verified via
# expect_crash. Separate class so the crash doesn't affect other tests.

class TestTieredCorruptionCrcMismatch(ValkeyLargeObjTestCaseBase):
    """CRC mismatch: server must crash on corrupt data, not serve it."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 4194304"
            f" max-promote-size 0"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_file_header_crc_mismatch(self):
        """Corrupt CRC in .dat header → GET triggers panic."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('LO.SET', 'crc_key', payload)
        # Corrupt the CRC field in the FileHeader (bytes 21-24 in packed layout).
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected .dat file after SET"
        with open(dat_files[-1], 'r+b') as f:
            f.seek(21)
            f.write(b'\xff\xff\xff\xff')
        # GET should trigger panic (CRC mismatch in read_and_verify_file_header).
        with self.server.expect_crash(self):
            try:
                client.execute_command('LO.GET', 'crc_key')
            except Exception:
                pass


class TestTieredCorruptionMagic(ValkeyLargeObjTestCaseBase):
    """FileHeader magic corruption: server must crash on corrupt data."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" dram-segment-size 4194304"
            f" max-promote-size 0"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_file_header_magic_corruption(self):
        """Corrupt magic bytes → GET triggers panic."""
        client = self.server.get_new_client()
        payload = b'Y' * 4096
        client.execute_command('LO.SET', 'magic_key', payload)
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected .dat file after SET"
        with open(dat_files[-1], 'r+b') as f:
            f.seek(0)
            f.write(b'BAAD')
        with self.server.expect_crash(self):
            try:
                client.execute_command('LO.GET', 'magic_key')
            except Exception:
                pass

class TestLargeObjSmartlog(ValkeyLargeObjTestCaseBase):
    """SMART log INFO section in Tiered mode (default poll interval)."""

    def test_smartlog_section_present(self):
        """Tiered mode: the smartlog section appears once the poller's first
        read lands, with well-formed per-device field groups."""
        client = self.server.get_new_client()
        wait_for_true(
            lambda: 'largeobj_snapshot_age_seconds' in client.info('largeobj_smartlog')
        )
        info = client.info('largeobj_smartlog')

        # Each enumerated controller reports either health fields or a
        # read error (CI hosts usually lack /dev/nvme* access), never both.
        prefixes = set()
        for key in info:
            m = key.removeprefix('largeobj_')
            if m.startswith('nvme'):
                prefixes.add(m.split('_')[0])
        for dev in prefixes:
            has_error = f'largeobj_{dev}_read_error' in info
            has_health = f'largeobj_{dev}_critical_warning' in info
            assert has_error != has_health, (
                f"{dev} must report exactly one of read_error / health fields"
            )
            if has_health:
                # Usage + warning fields all present for a healthy read.
                for field in ('data_units_read', 'data_units_written',
                              'percentage_used', 'available_spare_pct',
                              'temperature_kelvin', 'media_read_only',
                              'media_errors', 'unsafe_shutdowns'):
                    assert f'largeobj_{dev}_{field}' in info


class TestLargeObjSmartlogDisabled(ValkeyLargeObjTestCaseBase):
    """smartlog-poll-secs 0: no poller, no INFO section, even in Tiered mode."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" smartlog-poll-secs 0"
        )

    def test_smartlog_absent_when_disabled(self):
        """The poller never starts at 0, so absence is immediate and permanent
        (nothing to wait out). Module load succeeding with the arg already
        proves the config is registered and accepts 0."""
        client = self.server.get_new_client()
        assert 'largeobj_snapshot_age_seconds' not in client.info('largeobj_smartlog')
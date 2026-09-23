import os
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestLargeObjDram(ValkeyLargeObjTestCaseBase):
    """Dram-only mode: all objects live in DRAMPool, no NVMe."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_set_get_roundtrip(self):
        """Basic SET + GET in Dram mode."""
        client = self.server.get_new_client()
        payload = b'A' * 4096
        result = client.execute_command('LO.SET', 'dramkey', payload)
        assert result == b'OK'
        data = client.execute_command('LO.GET', 'dramkey')
        assert data == payload

    def test_get_nonexistent_key(self):
        """GET on nonexistent key returns nil in Dram mode."""
        client = self.server.get_new_client()
        result = client.execute_command('LO.GET', 'nokey')
        assert result is None

    def test_overwrite_key(self):
        """SET same key twice returns OK both times."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'overkey', b'X' * 4096)
        client.execute_command('LO.SET', 'overkey', b'Y' * 4096)
        data = client.execute_command('LO.GET', 'overkey')
        assert data == b'Y' * 4096

    def test_delete_key(self):
        """DEL removes the key, subsequent GET returns nil."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'delkey', b'Z' * 4096)
        client.execute_command('DEL', 'delkey')
        result = client.execute_command('LO.GET', 'delkey')
        assert result is None

    def test_dram_pool_exhaustion(self):
        """An object larger than segment-size fails with pool exhausted."""
        client = self.server.get_new_client()
        # segment-size is 1MB. A 2MB object cannot be allocated.
        obj_size = 2 * 1024 * 1024
        payload = b'D' * obj_size
        try:
            client.execute_command('LO.SET', 'toobig', payload)
            assert False, "Expected pool exhausted error"
        except ResponseError as e:
            assert 'pool exhausted' in str(e).lower(), f"Unexpected error: {e}"

    def test_multiple_objects(self):
        """Multiple small objects can coexist in DRAMPool."""
        client = self.server.get_new_client()
        for i in range(10):
            payload = bytes([i % 256]) * 4096
            client.execute_command('LO.SET', f'multi{i}', payload)
        for i in range(10):
            data = client.execute_command('LO.GET', f'multi{i}')
            expected = bytes([i % 256]) * 4096
            assert data == expected, f"Key multi{i} mismatch"

    # ─── COPY callback tests ─────────────────────────────────────────────

    def test_copy(self):
        """COPY in DRAM-only mode: independent object, digest differs, delete independence."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('LO.SET', 'srckey', payload)
        # COPY creates an independent object
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True
        assert client.execute_command('LO.GET', 'srckey') == payload
        assert client.execute_command('LO.GET', 'dstkey') == payload
        # COPY gets a new OID so digests differ
        src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'srckey')
        dst_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'dstkey')
        assert src_digest != dst_digest
        # Deleting source does not affect the copy
        client.execute_command('DEL', 'srckey')
        assert client.execute_command('LO.GET', 'dstkey') == payload
        # Deleting copy does not affect the source
        client.execute_command('LO.SET', 'srckey2', payload)
        client.execute_command('COPY', 'srckey2', 'dstkey2')
        client.execute_command('DEL', 'dstkey2')
        assert client.execute_command('LO.GET', 'srckey2') == payload

    def test_copy_pool_exhausted(self):
        """COPY fails when DRAMPool cannot fit the duplicate."""
        client = self.server.get_new_client()
        # Fill most of the 1MB pool with a large object.
        payload = b'F' * (900 * 1024)
        client.execute_command('LO.SET', 'bigkey', payload)
        # COPY needs another 900KB — pool is only 1MB total.
        try:
            client.execute_command('COPY', 'bigkey', 'bigcopy')
            assert False, "Expected COPY to fail with pool exhausted"
        except ResponseError:
            pass  # Expected — pool cannot fit two 900KB objects

    # ─── MEMORY USAGE callback tests ──────────────────────────────────────

    def test_memory_usage(self):
        """MEMORY USAGE in DRAM-only mode includes LoValue struct + payload."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('LO.SET', 'memkey', b'M' * payload_size)
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        assert mem >= lo_value_size + payload_size, (
            f"Expected MEMORY USAGE >= {lo_value_size + payload_size}, got {mem}"
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

    # ─── LO.INFO tests ───────────────────────────────────────────────────

    def test_info(self):
        client = self.server.get_new_client()
        payload = bytes(range(256)) * 16  # 4096 bytes, non-uniform so CRC is meaningful
        client.execute_command('LO.SET', 'infokey', payload)
        # Check the specfic fields for info
        assert client.execute_command('LO.INFO', 'infokey', 'LEN') == len(payload)
        # CRC is a u32: in range, stable across calls, and identical for identical payloads.
        crc = client.execute_command('LO.INFO', 'infokey', 'CRC')
        assert 0 <= crc <= 0xFFFFFFFF
        assert client.execute_command('LO.INFO', 'infokey', 'CRC') == crc
        client.execute_command('LO.SET', 'infokey2', payload)
        assert client.execute_command('LO.INFO', 'infokey2', 'CRC') == crc
        assert client.execute_command('LO.INFO', 'infokey', 'TIER') == b'dram'
        # Check full info call
        result = client.execute_command('LO.INFO', 'infokey')
        assert result == [
            b'len', len(payload),
            b'crc', crc,
            b'tier', b'dram',
        ], f"Unexpected LO.INFO reply: {result!r}"

    def test_info_crc_changes_on_overwrite(self):
        """Overwriting a key updates LEN and CRC."""
        client = self.server.get_new_client()
        first = b'A' * 4096
        second = b'B' * 8192
        client.execute_command('LO.SET', 'owkey', first)
        first_crc = client.execute_command('LO.INFO', 'owkey', 'CRC')
        client.execute_command('LO.SET', 'owkey', second)
        assert client.execute_command('LO.INFO', 'owkey', 'LEN') == 8192
        assert client.execute_command('LO.INFO', 'owkey', 'CRC') != first_crc

    def test_info_errors(self):
        """LO.INFO errors are correct"""
        client = self.server.get_new_client()
        # Nonexistant key
        self.verify_error_response(client, 'LO.INFO nokey', 'not found')
        self.verify_error_response(client, 'LO.INFO nokey LEN', 'not found')
        # Wrong type error
        client.execute_command('SET', 'strkey', 'plain')
        try:
            client.execute_command('LO.INFO', 'strkey')
            assert False, "Expected WRONGTYPE error"
        except ResponseError as e:
            assert 'existing key has wrong valkey type' in str(e).lower(), f"Unexpected error: {e}"
        # Wrong number of arguments error
        client.execute_command('LO.SET', 'badkey', b'x' * 4096)
        try:
            client.execute_command('LO.INFO', 'badkey', 'LEN', 'CRC')
            assert False, "Expected arity error"
        except ResponseError as e:
            assert 'wrong number of arguments' in str(e).lower(), f"Unexpected error: {e}"
        # Bad field error
        try:
            client.execute_command('LO.INFO', 'badkey', 'NOTREAL')
            assert False, "Expected wrong information field error"
        except ResponseError as e:
            assert 'invalid information value' in str(e).lower(), f"Unexpected error: {e}"

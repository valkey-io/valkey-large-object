import binascii
import collections
import crc32c
import os
import pytest
import subprocess
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj


# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1. The server only records it until
# the first transfer, so any well-formed address will do.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()


class TestLargeObjFabric(ValkeyLargeObjTestCaseBase):
    """BLOB.HELLO against real fabric services over the tcp provider on loopback."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 520192"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_hello_returns_one_address_per_server(self):
        """One server was pinned to lo, so HELLO returns exactly one address, and it decodes."""
        client = self.server.get_new_client()
        reply = client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        assert isinstance(reply, list) and len(reply) == 1
        address = binascii.unhexlify(reply[0])
        assert 0 < len(address)

    def test_hello_is_once_per_connection(self):
        """A second HELLO on the same connection is refused; a new connection may HELLO again."""
        client = self.server.get_new_client()
        first = client.execute_command('BLOB.HELLO', PEER_ADDRESS)
        self.verify_error_response(
            client, f'BLOB.HELLO {PEER_ADDRESS}',
            'DMA session already established (one BLOB.HELLO per connection)')
        assert self.server.get_new_client().execute_command('BLOB.HELLO', PEER_ADDRESS) == first

    def test_hello_rejects_bad_hex(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, 'BLOB.HELLO zz', 'invalid peer address hex')
        try:
            client.execute_command('BLOB.HELLO', '')
            assert False, "Expected an error for an empty address"
        except ResponseError as e:
            assert str(e) == 'peer address must not be empty'

    def test_efa_get_needs_hello(self):
        """The EFA arity of BLOB.GET is refused until this client has a session."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.SET', 'key', b'A' * 4096)
        self.verify_error_response(
            client, 'BLOB.GET key 999 0 4096', 'no DMA session (call BLOB.HELLO first)')

    def test_arity_gaps_are_refused(self):
        """Arg counts that are neither TCP nor EFA with complete triples are rejected.

        GET: 2=TCP, >=5=EFA (triplets). So 3 or 4 args is invalid.
        SET: 3=TCP, >=6=EFA (total_len + triplets). So 4 or 5 args is invalid.
        Trailing args that break a triplet boundary are also refused."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.SET', 'key', b'A' * 4096)
        for command in ('BLOB.GET key 999',
                        'BLOB.GET key 999 0',
                        'BLOB.SET key 4096 999',
                        'BLOB.SET key 4096 999 0'):
            name = command.split()[0]
            self.verify_error_response(
                client, command, f"wrong number of arguments for '{name}' command")
        # Trailing arg past a well-formed single-address list: 6 args enters the EFA
        # parser (>=5), but 4 tail fields is not divisible by 3.
        self.verify_error_response(
            client, 'BLOB.GET key 999 0 4096 7',
            'address args must be rkey, addr, len triples')


class TestLargeObjFabricUnavailable(ValkeyLargeObjTestCaseBase):
    """A domain that doesn't exist leaves the module TCP-only rather than refusing to load."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 520192"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces no-such-interface"
        )

    def test_hello_reports_unavailable(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, f'BLOB.HELLO {PEER_ADDRESS}', 'EFA unavailable on this instance')
        assert client.execute_command('BLOB.SET', 'key', b'A' * 4096) == b'OK'

# Position-dependent payload for fabric transfers: cycling 0x00..0xFF so that byte-ordering
# across multi-region splits is verified, not just fill. Must match fabric_target's
# generate_pattern().
TARGET_LEN = 4096
PATTERN = bytes(i % 256 for i in range(TARGET_LEN))

# One advertised client memory address: the fabric address to HELLO with, plus the
# (rkey, addr, len) triple that BLOB.GET / BLOB.SET carries per address.
Region = collections.namedtuple('Region', 'address rkey addr len')


def address_args(regions):
    """The addresses as the EFA commands carry them: a (rkey, addr, len) triple each.

    Order is load-bearing — the object's bytes are laid across the addresses in this
    order, so it must match the order the target registered them."""
    args = []
    for region in regions:
        args += [region.rkey, region.addr, region.len]
    return args


class TestLargeObjFabricTransfer(ValkeyLargeObjTestCaseBase):
    """Bytes actually move: tests/harness/fabric_target, a passive libfabric peer on tcp loopback, is
    the client's buffer. Its advertisement is what a real client would carry into BLOB.HELLO and the
    per-request rkey / remote address."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 520192"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags, split=None):
        """Launch the passive peer and return it with the regions it advertised.

        `split` is a list of region sizes totalling TARGET_LEN; the target then registers
        one separate buffer per region, each with its own rkey. Omitted, it registers the
        single whole-buffer region."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        command = [target, '127.0.0.1', *flags]
        if split is not None:
            command.append('--split=' + ','.join(str(size) for size in split))
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        regions = []
        for _ in range(1 if split is None else len(split)):
            line = process.stdout.readline()
            assert line.startswith('advertisement: '), line
            address, rkey, remote_addr, length = line.split()[1:]
            regions.append(Region(address, int(rkey), int(remote_addr), int(length)))
        return process, regions

    def test_get_writes_into_the_target(self):
        process, regions = self.start_target()
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.SET', 'key', PATTERN)
            client.execute_command('BLOB.HELLO', regions[0].address)
            reply = client.execute_command('BLOB.GET', 'key', *address_args(regions))
            assert reply == [TARGET_LEN, crc32c.crc32c(PATTERN)]
            # The target exits once every byte of the pattern has landed.
            output = process.communicate(timeout=30)[0]
            assert 'payload verified' in output, output
        finally:
            process.kill()

    def test_set_reads_from_the_target(self):
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.HELLO', regions[0].address)
            assert client.execute_command(
                'BLOB.SET', 'key', TARGET_LEN, *address_args(regions)) == b'OK'
            assert client.execute_command('BLOB.GET', 'key') == PATTERN
        finally:
            process.kill()

    def test_multi_transfer_mixed_protocol(self):
        """Do mixed sets and gets, read the server value into the client, set it back, and
        read it back in the test"""
        payload = b'\x5a' * TARGET_LEN
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.SET', 'key', payload)
            client.execute_command('BLOB.HELLO', regions[0].address)
            reply = client.execute_command('BLOB.GET', 'key', *address_args(regions))
            assert reply == [TARGET_LEN, crc32c.crc32c(payload)]
            assert client.execute_command(
                'BLOB.SET', 'copy', TARGET_LEN, *address_args(regions)) == b'OK'
            assert client.execute_command('BLOB.GET', 'copy') == payload
        finally:
            process.kill()

    def test_multi_address_transfer(self):
        """GET and SET across several separate client addresses.

        Covers equal and unequal splits, and a boundary that falls mid-chunk: 1024 is not
        a multiple of chunk-size 4096, so the first chunk must be scattered across both
        addresses. The target reports 'payload verified' only once EVERY address has filled,
        so a transfer that wrote the head and dropped the tail fails here."""
        payload = PATTERN
        for sizes in ([2048, 2048], [1024, 3072]):
            process, regions = self.start_target(split=sizes)
            try:
                assert [region.len for region in regions] == sizes
                # Separate registrations, so distinct keys — the multi-rkey path.
                assert regions[0].rkey != regions[1].rkey
                client = self.server.get_new_client()
                client.execute_command('BLOB.SET', 'key', payload)
                client.execute_command('BLOB.HELLO', regions[0].address)
                reply = client.execute_command('BLOB.GET', 'key', *address_args(regions))
                assert reply == [TARGET_LEN, crc32c.crc32c(payload)]
                output = process.communicate(timeout=30)[0]
                assert 'payload verified' in output, output
            finally:
                process.kill()
        # SET gathers the object back out of unequal addresses, verified byte for byte.
        process, regions = self.start_target('--read', split=[1024, 3072])
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.HELLO', regions[0].address)
            assert client.execute_command(
                'BLOB.SET', 'copy', TARGET_LEN, *address_args(regions)) == b'OK'
            assert client.execute_command('BLOB.GET', 'copy') == payload
        finally:
            process.kill()

    def test_address_coverage_is_validated(self):
        """Addresses must cover the object, and may exceed it.

        Validation is sum(len_i) >= obj_len, so surplus space is accepted and the reply's
        obj_len is how the client knows where the object ends. A shortfall is refused
        before any transfer is issued."""
        short_len = 2048
        process, regions = self.start_target(split=[1024, 3072])
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.HELLO', regions[0].address)
            # 4096 bytes of advertised space for a 2048-byte object.
            client.execute_command('BLOB.SET', 'short', PATTERN[:short_len])
            assert client.execute_command('BLOB.GET', 'short', *address_args(regions)) == [
                short_len, crc32c.crc32c(PATTERN[:short_len])]
            # The 1024-byte address alone cannot hold a 4096-byte object.
            client.execute_command('BLOB.SET', 'key', PATTERN)
            first = f'{regions[0].rkey} {regions[0].addr} {regions[0].len}'
            self.verify_error_response(
                client, f'BLOB.GET key {first}',
                'client address space smaller than object length')
            self.verify_error_response(
                client, f'BLOB.SET key {TARGET_LEN} {first}',
                'client address space smaller than object length')
        finally:
            process.kill()

    def test_efa_drain_on_partial_failure(self):
        """When an EFA submit fails partway through, already-posted transfers are drained
        inline before the error is returned, and efa_drain_count increments."""
        process, regions = self.start_target(split=[2048, 2048])
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.SET', 'key', PATTERN)
            client.execute_command('BLOB.HELLO', regions[0].address)
            before = info_largeobj(client).get('largeobj_efa_drain_count', 0)
            client.execute_command(
                'CONFIG', 'SET', 'largeobj.test-efa-fail-partial', 'yes')
            with pytest.raises(ResponseError, match="EFA write"):
                client.execute_command('BLOB.GET', 'key', *address_args(regions))
            after = info_largeobj(client)
            assert after['largeobj_efa_drain_count'] - before == 1, \
                f"expected exactly 1 drained transfer, got {after['largeobj_efa_drain_count'] - before}"
        finally:
            client.execute_command(
                'CONFIG', 'SET', 'largeobj.test-efa-fail-partial', 'no')
            process.kill()


class TestLargeObjFabricTieredTransfer(TestLargeObjFabricTransfer):
    """Tiered mode with promotion off to run the NVMe paths."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" chunk-size 4096"
            f" max-promote-size 0"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_set_over_efa_persists_to_nvme(self):
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.HELLO', regions[0].address)
            assert client.execute_command(
                'BLOB.SET', 'key', TARGET_LEN, *address_args(regions)) == b'OK'
            assert len(self._object_files()) == 1
        finally:
            process.kill()


class TestLargeObjFabricTieredPromotedTransfer(TestLargeObjFabricTransfer):
    """Tiered mode with promotion on."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 520192"
            f" chunk-size 4096"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_get_over_efa_cold_then_warm_on_one_session(self):
        payload = PATTERN
        process, regions = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('BLOB.HELLO', regions[0].address)
            assert client.execute_command(
                'BLOB.SET', 'key', TARGET_LEN, *address_args(regions)) == b'OK'
            # Cold load into dram
            assert client.execute_command(
                'BLOB.GET', 'key', *address_args(regions)) == [TARGET_LEN, crc32c.crc32c(payload)]
            # Hot load from dram
            assert client.execute_command(
                'BLOB.GET', 'key', *address_args(regions)) == [TARGET_LEN, crc32c.crc32c(payload)]
            # Read back from client and verify literal bytes
            assert client.execute_command(
                'BLOB.SET', 'copy', TARGET_LEN, *address_args(regions)) == b'OK'
            assert client.execute_command('BLOB.GET', 'copy') == payload
        finally:
            process.kill()

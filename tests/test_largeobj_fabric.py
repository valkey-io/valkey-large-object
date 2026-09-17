import binascii
import os
import subprocess
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1. The server only records it until
# the first transfer, so any well-formed address will do.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()


class TestLargeObjFabric(ValkeyLargeObjTestCaseBase):
    """LO.HELLO against real fabric services over the tcp provider on loopback."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" lo-buffer-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_hello_returns_one_address_per_server(self):
        """One server was pinned to lo, so HELLO returns exactly one address, and it decodes."""
        client = self.server.get_new_client()
        reply = client.execute_command('LO.HELLO', PEER_ADDRESS)
        assert isinstance(reply, list) and len(reply) == 1
        address = binascii.unhexlify(reply[0])
        assert 0 < len(address)

    def test_hello_is_once_per_connection(self):
        """A second HELLO on the same connection is refused; a new connection may HELLO again."""
        client = self.server.get_new_client()
        first = client.execute_command('LO.HELLO', PEER_ADDRESS)
        self.verify_error_response(
            client, f'LO.HELLO {PEER_ADDRESS}',
            'DMA session already established (one LO.HELLO per connection)')
        assert self.server.get_new_client().execute_command('LO.HELLO', PEER_ADDRESS) == first

    def test_hello_rejects_bad_hex(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, 'LO.HELLO zz', 'invalid peer address hex')
        try:
            client.execute_command('LO.HELLO', '')
            assert False, "Expected an error for an empty address"
        except ResponseError as e:
            assert str(e) == 'peer address must not be empty'

    def test_efa_get_needs_hello(self):
        """The EFA arity of LO.GET is refused until this client has a session."""
        client = self.server.get_new_client()
        client.execute_command('LO.SET', 'key', b'A' * 4096)
        self.verify_error_response(client, 'LO.GET key 1 0', 'no DMA session (call LO.HELLO first)')


class TestLargeObjFabricUnavailable(ValkeyLargeObjTestCaseBase):
    """A domain that doesn't exist leaves the module TCP-only rather than refusing to load."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" lo-buffer-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces no-such-interface"
        )

    def test_hello_reports_unavailable(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, f'LO.HELLO {PEER_ADDRESS}', 'EFA unavailable on this instance')
        assert client.execute_command('LO.SET', 'key', b'A' * 4096) == b'OK'

# What examples/fabric_target waits for (write) or serves (--read): one buffer of this byte.
PATTERN = b'\xab'
TARGET_LEN = 4096


class TestLargeObjFabricTransfer(ValkeyLargeObjTestCaseBase):
    """Bytes actually move: examples/fabric_target, a passive libfabric peer on tcp loopback, is
    the client's buffer. Its advertisement is what a real client would carry into LO.HELLO and the
    per-request rkey / remote address."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" lo-buffer-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags):
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'examples', 'fabric_target')
        process = subprocess.Popen(
            [target, '127.0.0.1', *flags],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        line = process.stdout.readline()
        assert line.startswith('advertisement: '), line
        address, rkey, remote_addr = line.split()[1:]
        return process, address, int(rkey), int(remote_addr)

    def test_get_writes_into_the_target(self):
        process, address, rkey, remote_addr = self.start_target()
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.SET', 'key', PATTERN * TARGET_LEN)
            client.execute_command('LO.HELLO', address)
            assert client.execute_command('LO.GET', 'key', rkey, remote_addr) == TARGET_LEN
            # The target exits once every byte of the pattern has landed.
            output = process.communicate(timeout=30)[0]
            assert 'payload verified' in output, output
        finally:
            process.kill()

    def test_set_reads_from_the_target(self):
        process, address, rkey, remote_addr = self.start_target('--read')
        try:
            client = self.server.get_new_client()
            client.execute_command('LO.HELLO', address)
            assert client.execute_command('LO.SET', 'key', TARGET_LEN, rkey, remote_addr) == b'OK'
            assert client.execute_command('LO.GET', 'key') == PATTERN * TARGET_LEN
        finally:
            process.kill()

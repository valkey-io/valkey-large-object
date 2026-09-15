import binascii
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1. The server only records it until
# the first transfer, so any well-formed address will do.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()


class TestLargeObjFabric(ValkeyLargeObjTestCaseBase):
    """LO.HELLO against real fabric servers over the tcp provider on loopback."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" lo-buffer-size 4096"
            f" fabric-provider Tcp"
            f" fabric-interfaces lo"
        )

    def test_hello_returns_one_address_per_server(self):
        """One server was pinned to lo, so HELLO returns exactly one address, and it decodes."""
        client = self.server.get_new_client()
        reply = client.execute_command('LO.HELLO', PEER_ADDRESS)
        assert isinstance(reply, list) and len(reply) == 1
        address = binascii.unhexlify(reply[0])
        assert 0 < len(address)

    def test_hello_is_repeatable(self):
        """A second HELLO on the same client replaces the session and answers the same."""
        client = self.server.get_new_client()
        first = client.execute_command('LO.HELLO', PEER_ADDRESS)
        second = client.execute_command('LO.HELLO', PEER_ADDRESS)
        assert first == second

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
        client.execute_command('LO.HELLO', PEER_ADDRESS)
        # Still the stubbed data path: it completes without moving bytes and replies the length.
        assert client.execute_command('LO.GET', 'key', 1, 0) == 4096


class TestLargeObjFabricUnavailable(ValkeyLargeObjTestCaseBase):
    """A domain that doesn't exist leaves the module TCP-only rather than refusing to load."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" dram-segment-size 1048576"
            f" lo-buffer-size 4096"
            f" fabric-provider Tcp"
            f" fabric-interfaces no-such-interface"
        )

    def test_hello_reports_unavailable(self):
        client = self.server.get_new_client()
        self.verify_error_response(client, f'LO.HELLO {PEER_ADDRESS}', 'EFA unavailable on this instance')
        assert client.execute_command('LO.SET', 'key', b'A' * 4096) == b'OK'
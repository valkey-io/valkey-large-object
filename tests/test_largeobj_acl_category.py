import binascii

from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase

# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1. The server only records it until
# the first transfer, so any well-formed address will do.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()

# Sentinel for BLOB.HELLO, whose reply is host-specific and checked by shape.
HELLO_ADDRESSES = object()

class TestLargeObjACLCategory(ValkeyLargeObjTestCaseBase):

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 520192"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_large_obj_acl_category(self):
        # (args, expected reply for the permitted user). HELLO returns host-specific
        # fabric addresses, so it gets a sentinel and a shape check instead.
        large_obj_commands = [
            (['BLOB.SET', 'aclkey', 'val'], b'OK'),
            (['BLOB.GET', 'aclkey'], b'val'),
            (['BLOB.INFO', 'aclkey', 'LEN'], 3),
            (['BLOB.HELLO', PEER_ADDRESS], HELLO_ADDRESSES),
        ]
        client = self.server.get_new_client()
        listed = set(client.execute_command("COMMAND LIST FILTERBY ACLCAT largeobj"))
        # Every largeobj command is covered, and nothing else is in the category.
        assert listed == {args[0].lower().encode() for args, _ in large_obj_commands} or \
            listed == {args[0].encode() for args, _ in large_obj_commands}, listed

        client.execute_command("ACL SETUSER nonlargeobjuser on >blob_pass +@all -@largeobj ~*")
        client.execute_command("ACL SETUSER largeobjuser on >blob_pass +@largeobj ~*")

        # Denied user: every command fails the ACL check before the handler runs.
        denied = self.server.get_new_client()
        denied.execute_command("AUTH nonlargeobjuser blob_pass")
        for args, _ in large_obj_commands:
            try:
                denied.execute_command(*args)
                assert False, f"nonlargeobjuser should not be able to run {args[0]}"
            except Exception as e:
                assert "no permissions to run the" in str(e), str(e)
                assert args[0].lower() in str(e).lower(), str(e)

        # Allowed user on a fresh connection, so HELLO has no prior session.
        allowed = self.server.get_new_client()
        allowed.execute_command("AUTH largeobjuser blob_pass")
        for args, expected in large_obj_commands:
            result = allowed.execute_command(*args)
            if expected is HELLO_ADDRESSES:
                # One hex-encoded fabric address per service.
                assert isinstance(result, list) and result, f"{args[0]} returned {result!r}"
                for address in result:
                    assert address and binascii.unhexlify(address), f"bad address {address!r}"
            else:
                assert result == expected, f"{args[0]} returned {result!r}"


    def test_large_obj_command_acl_categories(self):
        # List of large object commands and their acl categories
        large_object_commands = [
            ('BLOB.HELLO', [b'module'], [b'@connection', b'@largeobj']),
            ('BLOB.INFO', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@largeobj']),
            ('BLOB.SET', [b'write', b'denyoom', b'module'], [b'@write', b'@slow', b'@largeobj']),
            ('BLOB.GET', [b'readonly', b'module'], [b'@read', b'@slow', b'@largeobj']),
        ]
        for cmd in large_object_commands:
            # Get the info of the commands and compare the acl categories
            cmd_info = self.client.execute_command(f'COMMAND INFO {cmd[0]}')
            assert cmd_info[0][2] == cmd[1], f'{cmd[0]} Categories: {cmd_info[0][2]}'
            for category in cmd[2]:
                assert set(cmd_info[0][6]) == set(cmd[2]), f'{cmd[0]} ACL categories: {cmd_info[0][6]}'


import threading
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_equal
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestSelectDbTiered(ValkeyLargeObjTestCaseBase):
    """A SET must land in the client's selected db, not db 0.
    Tiered TCP SET commits asynchronously (NVMe write, then commit off main)."""

    @pytest.mark.parametrize("size", [16, 64 * 1024])
    def test_set_lands_in_selected_db(self, size):
        client = self.server.get_new_client()
        client.execute_command('SELECT', 1)
        payload = b'X' * size
        assert client.execute_command('BLOB.TCP_SET', 'blob', payload) == b'OK'

        assert client.execute_command('DBSIZE') == 1
        assert client.execute_command('BLOB.TCP_GET', 'blob') == payload

        client.execute_command('SELECT', 0)
        assert client.execute_command('DBSIZE') == 0
        assert client.execute_command('EXISTS', 'blob') == 0


class TestSelectDbDram(TestSelectDbTiered):
    """Same contract in Dram mode (TCP SET commits on the main thread)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            "operating-mode Dram"
            " segment-size 2097152"
            " max-object-size 1044480"
            " bench-mode no"
            " direct-io no"
            " chunk-size 4096"
            " fabric-provider Emulated"
        )


class TestSelectDbSwapDuringSet(ValkeyLargeObjTestCaseBase):
    """SWAPDB swaps db contents, not clients: a SET in flight across SWAPDB commits
    to the client's db index, where its next command looks."""

    def get_module_args(self, data_dir, direct_io):
        return (super().get_module_args(data_dir, direct_io)
                + " test-pause-before-finalize-set-ms 2000")

    def test_swapdb_during_set(self):
        client = self.server.get_new_client()
        admin = self.server.get_new_client()
        admin.set('db0_marker', 'x')
        client.execute_command('SELECT', 1)
        payload = b'X' * 16
        result = {}
        setter = threading.Thread(target=lambda: result.update(
            reply=client.execute_command('BLOB.TCP_SET', 'blob', payload)))
        setter.start()
        wait_for_equal(
            lambda: admin.info('largeobj')['largeobj_inflight_requests'], 1)
        admin.execute_command('SWAPDB', 0, 1)
        setter.join(timeout=10)
        assert result.get('reply') == b'OK'

        # The client is still on db 1, which now also holds db 0's old contents.
        assert client.execute_command('BLOB.TCP_GET', 'blob') == payload
        assert sorted(client.execute_command('KEYS', '*')) == [b'blob', b'db0_marker']
        admin.execute_command('SELECT', 0)
        assert admin.execute_command('DBSIZE') == 0


class TestNoBlockingContextTiered(ValkeyLargeObjTestCaseBase):
    """BLOB data commands may block, so MULTI and scripts reject them up front
    and nothing is written."""

    @pytest.mark.parametrize("command", [
        ('BLOB.TCP_SET', 'blob', b'X' * 16),
        ('BLOB.TCP_GET', 'blob'),
    ])
    def test_multi_rejects(self, command):
        client = self.server.get_new_client()
        pipe = client.pipeline(transaction=True)
        pipe.execute_command(*command)
        with pytest.raises(ResponseError, match='not allowed'):
            pipe.execute()
        assert client.execute_command('DBSIZE') == 0

    def test_script_rejects(self):
        client = self.server.get_new_client()
        with pytest.raises(ResponseError, match='not allowed'):
            client.execute_command(
                'EVAL', "return server.call('BLOB.TCP_SET', 'blob', 'x')", 0)
        assert client.execute_command('DBSIZE') == 0


class TestNoBlockingContextDram(TestNoBlockingContextTiered):
    """Same in Dram mode, where TCP commands are sync: whether a command blocks
    must not decide whether it works in MULTI."""

    get_module_args = TestSelectDbDram.get_module_args

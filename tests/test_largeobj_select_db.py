from concurrent.futures import ThreadPoolExecutor
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_equal
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
import test_largeobj_dram


class TestClientContextTiered(ValkeyLargeObjTestCaseBase):
    """Tiered TCP SET commits off the main thread."""

    def test_set_lands_in_selected_db(self):
        client = self.server.get_new_client()
        client.execute_command('SELECT', 1)
        client.execute_command('BLOB.TCP_SET', 'blob', b'x')
        assert client.execute_command('BLOB.TCP_GET', 'blob') == b'x'
        client.execute_command('SELECT', 0)
        assert client.execute_command('DBSIZE') == 0

    def test_multi_rejects_without_writing(self):
        client = self.server.get_new_client()
        pipe = client.pipeline(transaction=True)
        pipe.execute_command('BLOB.TCP_SET', 'blob', b'x')
        with pytest.raises(ResponseError, match='not allowed'):
            pipe.execute()
        assert client.execute_command('DBSIZE') == 0

    def test_script_rejects_without_writing(self):
        client = self.server.get_new_client()
        with pytest.raises(ResponseError, match='not allowed'):
            client.execute_command('EVAL', "return server.call('BLOB.TCP_SET', 'blob', 'x')", 0)
        assert client.execute_command('DBSIZE') == 0


class TestClientContextDram(TestClientContextTiered):
    """Dram TCP SET is sync; MULTI and scripts still reject it."""

    get_module_args = test_largeobj_dram.TestLargeObjDram.get_module_args


class TestSwapDbDuringSet(ValkeyLargeObjTestCaseBase):
    """SWAPDB moves db contents, not clients: an in-flight SET commits to the
    client's db index."""

    def get_module_args(self, data_dir, direct_io):
        return super().get_module_args(data_dir, direct_io) + " test-pause-before-finalize-set-ms 2000"

    def test_set_commits_to_client_db_index(self):
        client = self.server.get_new_client()
        admin = self.server.get_new_client()
        client.execute_command('SELECT', 1)
        with ThreadPoolExecutor(1) as pool:
            pending_set = pool.submit(client.execute_command, 'BLOB.TCP_SET', 'blob', b'x')
            wait_for_equal(lambda: admin.info('largeobj')['largeobj_inflight_requests'], 1)
            admin.execute_command('SWAPDB', 0, 1)
            assert pending_set.result(timeout=10) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', 'blob') == b'x'
        assert admin.execute_command('DBSIZE') == 0

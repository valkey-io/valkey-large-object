import os
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import logging


class ValkeyLargeObjTestCaseBase(ValkeyTestCase):
    """Base test class for valkey-largeobj module integration tests.

    Uses valkey-test-framework (same pattern as valkey-bloom).
    Spawns a valkey-server with the module loaded per test.

    Env vars:
        MODULE_PATH: path to libvalkey_largeobj.so
        SERVER_VERSION: valkey-server version directory name
    """

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        module_path = os.getenv('MODULE_PATH')
        # Use absolute path for data-dir so file assertions work regardless of cwd
        data_dir = os.path.abspath(self.testdir)
        # Disable O_DIRECT in ASAN builds — ASAN tests focus on memory safety,
        # not I/O bypass correctness. Avoids EINVAL from O_DIRECT alignment edge cases.
        direct_io = "no" if os.environ.get("ASAN_BUILD") else "yes"
        args = {
            'enable-debug-command': 'yes',
            'loadmodule': f"{module_path} data-dir {data_dir} pool-buf-size 4096 pool-buf-count 128 bench-mode no direct-io {direct_io}",
        }
        server_path = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=server_path,
            args=args,
        )
        self.data_dir = data_dir
        logging.info("startup args are: %s", args)

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False, f"Expected error but command succeeded"
        except ResponseError as e:
            assert str(e) == expected_err_reply, (
                f"Actual error '{str(e)}' != expected '{expected_err_reply}'"
            )
            return str(e)

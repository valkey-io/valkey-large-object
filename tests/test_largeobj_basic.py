import os
import glob
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestLargeObjBasic(ValkeyLargeObjTestCaseBase):

    def test_module_loaded(self):
        """Verify the largeobj module is loaded."""
        client = self.server.get_new_client()
        module_list = client.execute_command('MODULE LIST')
        module_names = [m[b'name'] for m in module_list]
        assert b'largeobj' in module_names or b'largeob-k' in module_names

    def test_lo_set_get_roundtrip(self):
        """DMA.SET writes data, DMA.GET retrieves it (TCP fallback path)."""
        client = self.server.get_new_client()
        payload = b'A' * 4096
        result = client.execute_command('DMA.SET', 'testkey', '4096', payload)
        assert result == b'OK'
        # GET returns the object bytes
        data = client.execute_command('DMA.GET', 'testkey')
        assert len(data) == 4096
        assert data == payload

    def test_lo_get_nonexistent_key(self):
        """DMA.GET on a nonexistent key returns nil."""
        client = self.server.get_new_client()
        result = client.execute_command('DMA.GET', 'nokey')
        assert result is None

    def test_lo_set_creates_nvme_file(self):
        """DMA.SET creates a .dat file in data-dir."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('DMA.SET', 'filekey', '4096', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, f"Expected .dat file in {self.data_dir}, found: {os.listdir(self.data_dir)}"

    def test_delete_removes_nvme_file(self):
        """DEL on an LO key removes the .dat file."""
        client = self.server.get_new_client()
        payload = b'Y' * 4096
        client.execute_command('DMA.SET', 'delkey', '4096', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1, "DMA.SET didn't create a .dat file"
        client.execute_command('DEL', 'delkey')
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    def test_pool_exhaustion_error(self):
        """Exceeding pool-buf-count returns an error."""
        client = self.server.get_new_client()
        # pool-buf-size is 4096, so 8192 should fail.
        self.verify_error_response(
            client,
            'DMA.SET bigkey 8192 ' + 'A' * 8192,
            'object exceeds buffer size',
        )

    def test_bench_mode_reply_format(self):
        """With bench-mode=yes, DMA.GET returns integer size."""
        # This test requires server started with bench-mode=yes.
        # Skip if not configured — the base setup uses bench-mode=no.
        pass  # TODO: parametrize setup_test with bench-mode=yes variant

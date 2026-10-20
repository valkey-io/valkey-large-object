import subprocess
import pytest
import sys
import os

# Set the path to find and use the valkey-test-framework
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'build')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'build/valkeytestframework')))

from valkeytestframework.conftest import resource_port_tracker


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'skip_for_asan: skipped in the ASAN job (ci.yml runs -m "not skip_for_asan")')

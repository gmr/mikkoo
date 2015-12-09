"""Tests for the MCP"""
import mock

from helper import config

from mikkoo import mcp
from . import test_state


class TestMCP(test_state.TestState):

    CONFIG = {'wake_interval': 30.0, 'workers': {}}

    def setUp(self):
        with mock.patch('multiprocessing.Queue') as stats_queue:
            self._stats_queue = stats_queue
            self.cfg = config.Config()
            self.cfg.application.update(self.CONFIG)
            self._obj = mcp.MasterControlProgram(self.cfg)

    def test_mcp_init_workers_dict(self):
        self.assertIsInstance(self._obj.workers, dict)

    def test_mcp_init_workers_dict_empty(self):
        self.assertTrue(not self._obj.workers, dict)

    def test_mcp_init_queue_initialized(self):
        self.assertIsInstance(self._obj.stats_queue, mock.MagicMock)

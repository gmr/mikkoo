"""Tests for mikkoo.worker"""
import copy
import multiprocessing
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from pika import channel
import pika
import signal

from mikkoo import worker
from mikkoo import __version__

from . import test_state


class TestProcess(test_state.TestState):

    MOCK_ARGS = {
            'config': {
                'statsd': {
                    'enabled': False
                },
                'worker': {
                    'postgres_url': 'postgresql://localhost:5432/postgres',
                    'rabbitmq_url': 'rabbitmq://localhost:5672/%2f',
                    'processes': 1,
                    'confirm': False
                }
            },
            'daemon': False,
            'name': 'test-process',
            'stats_queue': None,
            'worker_name': 'test-process-1',
        }

    def setUp(self):
        with mock.patch('multiprocessing.Process'):
            self.mock_args = self.new_kwargs()
            self._obj = self.new_process(self.mock_args)

    def tearDown(self):
        del self._obj

    def new_kwargs(self, **kwargs):
        args = copy.deepcopy(self.MOCK_ARGS)
        args.update(kwargs)
        args['stats_queue'] = multiprocessing.Queue()
        return args

    def new_process(self, kwargs=None):
        with mock.patch('multiprocessing.Process'):
            return worker.Process(group=None,
                                  name='MockProcess',
                                  kwargs=kwargs or self.new_kwargs())

    def new_mock_channel(self):
        return mock.Mock(spec=channel.Channel)

    def new_mock_connection(self):
        return mock.Mock(spec=pika.TornadoConnection)

    def test_app_id(self):
        expectation = 'mikkoo/%s' % __version__
        self.assertEqual(self._obj.AMQP_APP_ID, expectation)

    def test_startup_state(self):
        new_process = self.new_process()
        self.assertEqual(new_process.state, worker.Process.STATE_INITIALIZING)

    def test_startup_time(self):
        mock_time = 123456789.012345
        with mock.patch('time.time', return_value=mock_time):
            new_process = self.new_process()
            self.assertEqual(new_process.state_start, mock_time)

    def test_startup_channel_is_none(self):
        new_process = self.new_process()
        self.assertIsNone(new_process.channel)

    def test_setup_signal_handlers(self):
        signals = [mock.call(signal.SIGPROF, self._obj.on_sigprof),
                   mock.call(signal.SIGABRT, self._obj.stop)]
        with mock.patch('signal.signal') as signal_signal:
            self._obj.setup_signal_handlers()
            signal_signal.assert_has_calls(signals, any_order=True)

    def test_is_idle_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_idle)

    def test_is_running_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertTrue(self._obj.is_running)

    def test_is_shutting_down_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_stopped_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_stopped)

    def test_state_processing_desc(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_PROCESSING])

"""
Statsd Client that takes configuration first from the rejected configuration
file, falling back to environment variables, and finally default values.

Environment Variables:

 - STATSD_HOST
 - STATSD_PORT
 - STATSD_PREFIX

"""
import logging
import os
import socket

LOGGER = logging.getLogger(__name__)


class StatsdClient(object):
    """A simple statsd client"""
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 8125
    DEFAULT_PREFIX = 'mikkoo'
    PAYLOAD_FORMAT = '{0}.{1}.{2}.{3}:{4}|{5}'

    def __init__(self, name, settings):
        """

        :param str name: The name of the worker for this client
        :param dict settings: statsd Settings

        """
        self._name = name
        self._hostname = socket.gethostname().split('.')[0]
        self._settings = settings

        self._address = (self._setting('host', self.DEFAULT_HOST),
                         int(self._setting('port', self.DEFAULT_PORT)))
        self._prefix = self._setting('prefix', self.DEFAULT_PREFIX)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                                     socket.IPPROTO_UDP)

    def stop(self):
        self._counter_flush.stop()
        self._on_counter_flush()

    def _setting(self, key, default):
        """Return the setting, checking config, then the appropriate
        environment variable, falling back to the default.

        :param str key: The key to get
        :param any default: The default value if not set
        :return: str

        """
        env = 'STATSD_{}'.format(key).upper()
        return self._settings.get(key, os.environ.get(env, default))

    def add_timing(self, key, value=0):
        """Add a timer value to statsd for the specified key

        :param str key: The key to add the timing to
        :param int|float value: The value of the timing in milliseconds

        """
        self._send(key, value * 1000, 'ms')

    def incr(self, key, value=1):
        """Increment the counter value in statsd by grouping up the counters
        and sending them out in chunks.

        :param str key: The key to increment
        :param int value: The value to increment by, defaults to 1

        """
        self._send(key, value, 'c')

    def set_gauge(self, key, value=0):
        """Set a gauge value in statsd for the specified key

        :param str key: The key to set the gauge value for
        :param int|float value: The value of the gauge

        """
        self._send(key, value, 'g')

    def _send(self, key, value, metric_type):
        """Send the specified value to the statsd daemon via UDP without a
        direct socket connection.

        :param str key: The key name to send
        :param int|float value: The value for the key

        """
        try:
            payload = self.PAYLOAD_FORMAT.format(self._prefix, self._hostname,
                                                 self._name, key,
                                                 value, metric_type).encode()
            self._socket.sendto(payload, self._address)
        except socket.error:
            LOGGER.exception('Error sending statsd metric')

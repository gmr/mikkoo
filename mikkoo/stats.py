"""
Stats class that wraps the collections.Counter object and transparently
passes calls to increment and add_timing if statsd is enabled.

"""
import collections
import contextlib
import socket
import time
import typing

try:
    from sprockets_statsd import statsd
except ImportError:
    statsd = None


class Stats:

    DEFAULT_PREFIX = 'mikkoo'
    PAYLOAD_HOSTNAME = '{}.{}.{}.{}:{}'
    PAYLOAD_NO_HOSTNAME = '{}.{}.{}:{}'

    def __init__(self, name: str, worker_name: str, statsd_cfg: dict):
        self.name = name
        self.worker_name = worker_name
        self.statsd: typing.Optional[statsd.Connector] = None
        if statsd_cfg.get('enabled', False) and statsd:
            self.statsd = statsd.Connector(
                host=statsd_cfg.get('host'), port=statsd_cfg.get('port'),
                prefix=self._statsd_prefix(statsd_cfg),
                ip_protocol=socket.IPPROTO_TCP if statsd_cfg.get('tcp')
                else socket.IPPROTO_TCP)
        self.counter = collections.Counter()
        self.previous = None

    def __getitem__(self, item):
        return self.counter.get(item)

    def __setitem__(self, item, value):
        self.counter[item] = value

    async def start(self):
        if self.statsd:
            await self.statsd.start()

    async def stop(self):
        if self.statsd:
            await self.statsd.stop()

    def add_timing(self, item, duration):
        if self.statsd:
            self.statsd.timing(item, duration)

    def set_gauge(self, item, value):
        if self.statsd:
            self.statsd.gauge(item, value)
        self.counter[item] = value

    def diff(self, item):
        return self.counter.get(item, 0) - self.previous.get(item, 0)

    def get(self, item):
        return self.counter.get(item)

    def incr(self, key, value=1):
        self.counter[key] += value
        if self.statsd:
            self.statsd.incr(key, value)

    def report(self):
        """Submit the stats data to both the MCP stats queue and statsd"""
        if not self.previous:
            self.previous = {}
            for key in self.counter:
                self.previous[key] = 0
        values = {
            'name': self.name,
            'counts': dict(self.counter),
            'previous': self.previous,
            'worker_name': self.worker_name
        }
        self.previous = dict(self.counter)
        return values

    @contextlib.contextmanager
    def track_duration(self, key):
        """Time around a context and emit a statsd metric.
        :param str key: The key for the timing to track
        """
        start_time = time.time()
        try:
            yield
        finally:
            finish_time = max(start_time, time.time())
            self.add_timing(key, finish_time - start_time)

    def _statsd_prefix(self, config: dict) -> str:
        if config.get('include_hostname', False):
            return '{}.{}.{}'.format(
                config.get('prefix', self.DEFAULT_PREFIX),
                socket.gethostname().split('.')[0],
                self.name)
        return '{}.{}'.format(
            config.get('prefix', self.DEFAULT_PREFIX), self.name)

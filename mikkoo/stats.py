"""
Stats class that wraps the collections.Counter object and transparently
passes calls to increment and add_timing if statsd is enabled.

"""
try:
    import backport_collections as collections
except ImportError:
    import collections

from mikkoo import statsd


class Stats(object):

    def __init__(self, name, worker_name, statsd_cfg):
        self.name = name
        self.worker_name = worker_name
        self.statsd = None
        if statsd_cfg.get('enabled', False):
            self.statsd = statsd.StatsdClient(name, statsd_cfg)
        self.counter = collections.Counter()
        self.previous = None

    def __getitem__(self, item):
        return self.counter.get(item)

    def __setitem__(self, item, value):
        self.counter[item] = value

    def add_timing(self, item, duration):
        if self.statsd:
            self.statsd.add_timing(item, duration)

    def set_gauge(self, item, value):
        if self.statsd:
            self.statsd.set_gauge(item, value)
        self.counter[item] = value

    def get(self, item):
        return self.counter.get(item)

    def diff(self, item):
        return self.counter.get(item, 0) - self.previous.get(item, 0)

    def incr(self, key, value=1):
        self.counter[key] += value
        if self.statsd:
            self.statsd.incr(key, value)

    def report(self):
        """Submit the stats data to both the MCP stats queue and statsd"""
        if not self.previous:
            self.previous = dict()
            for key in self.counter:
                self.previous[key] = 0
        if self.statsd:
            for item in self.counter.keys():
                self.statsd.incr(item, self.diff(item))
        values = {
            'name': self.name,
            'counts': dict(self.counter),
            'previous': self.previous,
            'worker_name': self.worker_name
        }
        self.previous = dict(self.counter)
        return values

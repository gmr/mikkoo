"""Mikkoo is a PgQ to RabbitMQ Relay

Named for the rabbit in the clever rabbit and the elephant fable.

"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version('mikkoo')
except PackageNotFoundError:
    __version__ = '0.0.0'

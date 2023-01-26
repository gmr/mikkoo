"""
Mikkoo Worker Process
=====================
Connects to Postgres and processes PgQ batches, publishing the events in
the batch to RabbitMQ.

"""
import asyncio
import copy
import datetime
import json
import logging
import multiprocessing
import os
import signal
import socket
import sys
import time
import typing
import uuid

import arrow
import helper.config
import pika
import pika.channel
import psycopg
from pika import connection, exceptions, frame, spec
from pika.adapters import asyncio_connection
from psycopg import rows, sql
from uuid_extensions import uuid7

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None

from mikkoo import __version__, state, stats

LOGGER = logging.getLogger(__name__)


class Process(multiprocessing.Process, state.State):

    AMQP_APP_ID = 'mikkoo/%s' % __version__
    DEFAULT_CONSUMER_NAME = 'mikkoo'
    DEFAULT_MAX_FAILURES = 10
    DEFAULT_RETRY_INTERVAL = 10
    DEFAULT_WAIT_DURATION = 1
    BATCHES = 'batches'
    ERROR = 'failed'
    PROCESSED = 'processed'
    PENDING = 'pending_events'
    STATE_PROCESSING = 0x04
    VALID_PROPERTIES = [
        'app_id',
        'content_encoding',
        'content_type',
        'correlation_id',
        'delivery_mode',
        'expiration',
        'headers',
        'message_id',
        'priority',
        'timestamp',
        'type',
        'user_id'
    ]

    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = {}
        super(Process, self).__init__(group, target, name, args, kwargs)
        self.config = kwargs['config']
        self.confirm = kwargs['config']['worker'].get('confirm', False)
        self.consumer_name = kwargs['config']['worker'].get(
            'consumer_name', self.DEFAULT_CONSUMER_NAME)
        self.current_batch: typing.Optional[int] = None
        self.current_event: typing.Optional[dict] = None
        self.event_list: typing.Optional[typing.List[dict]] = None
        self.event_processed: typing.Optional[asyncio.Future] = None
        self.ioloop: typing.Optional[asyncio.AbstractEventLoop] = None
        self.last_stats_time: typing.Optional[float] = None
        self.maximum_failures = kwargs['config']['worker'].get(
            'max_failures', self.DEFAULT_MAX_FAILURES)
        self.postgres: typing.Optional[psycopg.AsyncConnection] = None
        self.rabbitmq: typing.Optional[
            asyncio_connection.AsyncioConnection] = None
        self.rabbitmq_channel: typing.Optional[pika.channel.Channel] = None
        self.retry_interval = kwargs['config']['worker'].get(
            'retry_interval', self.DEFAULT_RETRY_INTERVAL)
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.stats = stats.Stats(
            self.name,
            kwargs['worker_name'],
            kwargs['config'].get('statsd', {}))
        self.stats_queue: multiprocessing.Queue = kwargs['stats_queue']
        self.wait_duration = kwargs['config']['worker'].get(
            'wait_duration', self.DEFAULT_WAIT_DURATION)
        self.worker_config = kwargs['config']['worker']
        self.worker_name = kwargs['worker_name']

        if sentry_sdk and 'sentry_dsn' in self.config:
            sentry_sdk.init(
                self.config['sentry_dsn'],
                release=__version__,
                environment=os.getenv('ENVIRONMENT', 'unknown'),
                in_app_include=['arrow',
                                'mikkoo',
                                'pika',
                                'psycopg'])
            sentry_sdk.set_context('mikkoo', {'worker': self.name})

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    async def connect_to_postgres(self) -> bool:
        """Connects to Postgres, shutting down the worker on failure"""
        LOGGER.debug('Connecting to Postgres')
        try:
            self.postgres = await psycopg.AsyncConnection.connect(
                self.worker_config.get('postgres_url'),
                autocommit=True)
        except psycopg.OperationalError as error:
            self.send_exception_to_sentry()
            LOGGER.error('Error connecting to Postgres: %s', error)
            return False
        return True

    def build_sql(self, proc_name: str, *args) -> str:
        return sql.SQL('SELECT * FROM {proc_name}({values})'.format(
            proc_name=proc_name,
            values=','.join('%s' for _a in range(0, len(args))))).as_string(
                self.postgres)

    async def callproc(self, proc_name: str, *args) -> typing.Sequence[dict]:
        """Call the stored procedure in Postgres with the specified
        arguments.

        """
        sentry_sdk.set_context('mikkoo', {'function': proc_name})
        with self.stats.track_duration(f'db.{proc_name}.query_time'):
            async with self.postgres.cursor(
                    row_factory=rows.dict_row) as cursor:
                try:
                    await cursor.execute(
                        self.build_sql(proc_name, *args), args or [])
                    return await cursor.fetchall()
                except psycopg.InternalError as error:
                    self.send_exception_to_sentry()
                    self.log_db_error(proc_name, error)
                    raise PgQError(str(error))
                except psycopg.Error as error:
                    self.send_exception_to_sentry()
                    self.log_db_error(proc_name, error)

    def log_db_error(self, name: str, error: Exception) -> typing.NoReturn:
        """Log database errors and increment the stats counter"""
        LOGGER.error('Error executing %s.callproc: (%s) %s', name,
                     error.__class__.__name__, error)
        self.stats.incr('db.{0}.error.{1}'.format(name, str(error)))

    def connect_to_rabbitmq(self):
        """Connect to RabbitMQ returning the connection handle."""
        self.set_state(self.STATE_CONNECTING)
        return asyncio_connection.AsyncioConnection(
            pika.URLParameters(self.worker_config.get('rabbitmq_url')),
            on_open_callback=self.on_rabbitmq_open,
            on_open_error_callback=self.on_rabbitmq_open_error,
            on_close_callback=self.on_rabbitmq_closed)

    def on_rabbitmq_open(self,
                         conn: asyncio_connection.AsyncioConnection) \
            -> typing.NoReturn:
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        """
        LOGGER.info('Connection to RabbitMQ established')
        self.stats.incr('amqp.connection_opened')
        self.rabbitmq = conn

        try:
            self.rabbitmq.channel(
                on_open_callback=self.on_rabbitmq_channel_open)
        except exceptions.ConnectionClosed as err:
            LOGGER.warning('Channel open on closed connection')
            self.on_rabbitmq_closed(self.rabbitmq, err)
            return

        self.rabbitmq.add_on_connection_blocked_callback(
            self.on_rabbitmq_blocked)
        self.rabbitmq.add_on_connection_unblocked_callback(
            self.on_rabbitmq_unblocked)

    def on_rabbitmq_open_error(self,
                               _c: connection.Connection,
                               exc: typing.Union[str, Exception]) -> None:
        """Connection to RabbitMQ failed, so shut things down."""
        LOGGER.critical('Could not connect to RabbitMQ: %r', exc)
        self.stats.incr('amqp.connection_failed')
        self.on_ready_to_stop()

    def on_rabbitmq_blocked(self,
                            _c: connection.Connection,
                            _f: frame.Method) -> typing.NoReturn:
        """Invoked when the RabbitMQ connection has notified us that it is
        blocking publishing.

        """
        LOGGER.warning('RabbitMQ is blocking the connection')
        self.stats.incr('amqp.connection_blocked')
        self.set_state(self.STATE_BLOCKED)

    def on_rabbitmq_unblocked(self,
                              _c: connection.Connection,
                              _f: frame.Method) -> typing.NoReturn:
        """Invoked when the RabbitMQ connection has notified us that
        publishing is no longer unblocked.

        """
        LOGGER.info('RabbitMQ is no longer blocking the connection')
        self.stats.incr('amqp.connection_unblocked')
        if self.event_list:
            LOGGER.debug('Resuming the processing of %i events',
                         len(self.event_list))
            self.set_state(self.STATE_PROCESSING)
            self.async_call_soon(self.process_event)
        else:
            self.set_state(self.STATE_IDLE)
            self.async_call_soon(self.process_batch)

    def on_rabbitmq_closed(self,
                           _conn: asyncio_connection.AsyncioConnection,
                           error: Exception) -> typing.NoReturn:
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Shutdown if not already doing so.

        """
        LOGGER.critical('Connection from RabbitMQ closed in state %s (%)',
                        self.state_description, error)
        self.set_state(self.STATE_CLOSED)
        self.rabbitmq_channel = None
        self.stats.incr('amqp.connection_closed')
        if self.is_shutting_down or self.is_waiting_to_shutdown:
            self.on_ready_to_stop()
        else:
            LOGGER.info('Reconnecting to RabbitMQ')
            self.ioloop.call_soon(self.connect_to_rabbitmq)

    def open_rabbitmq_channel(self):
        """Open a channel on the existing open connection to RabbitMQ"""
        LOGGER.debug('Opening a new channel')
        self.rabbitmq.channel(on_open_callback=self.on_rabbitmq_channel_open)

    def on_rabbitmq_channel_open(self, channel: pika.channel.Channel) \
            -> typing.NoReturn:
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and set up the channel
        to start consuming.

        """
        LOGGER.debug('Channel %i opened', channel.channel_number)
        self.stats.incr('amqp.channel_opened')
        self.rabbitmq_channel = channel
        self.rabbitmq_channel.add_on_close_callback(
            self.on_rabbitmq_channel_closed)
        if self.confirm:
            LOGGER.info('Enabling publisher confirmations')
            self.rabbitmq_channel.add_on_return_callback(
                self.on_rabbitmq_publish_return)
            self.rabbitmq_channel.confirm_delivery(
                self.on_rabbitmq_publish_confirm)

        # Schedule the processing of the first batch if it's not reopening
        if not self.is_reconnecting:
            self.set_state(self.STATE_IDLE)
            self.async_call_soon(self.process_batch)

    def on_rabbitmq_channel_closed(self,
                                   channel: pika.channel.Channel,
                                   error: Exception) -> typing.NoReturn:
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shut down the object.

        """
        LOGGER.warning('Channel %i closed: %s',
                       channel.channel_number, error)
        self.stats.incr('amqp.channel_closed')
        self.rabbitmq_channel = None
        if self.event_processed:
            self.set_state(self.STATE_RECONNECTING)
            self.open_rabbitmq_channel()
        else:
            self.on_ready_to_stop()

    def on_rabbitmq_publish_confirm(self,
                                    _f: pika.frame.Method) -> typing.NoReturn:
        """Invoked by pika when a delivery confirmation is received."""
        if self.event_processed and self.current_event:
            LOGGER.debug('Event %s confirmed', self.current_event['ev_id'])
            self.stats.incr('amqp.publisher_confirm')
            self.event_processed.set_result(True)

    def on_rabbitmq_publish_return(self,
                                   _c: pika.channel.Channel,
                                   method: spec.Basic.Return,
                                   _p: spec.BasicProperties,
                                   _b: bytes) -> typing.NoReturn:
        """Invoked by pika when a delivery failure is received. Setting the
        current confirmation future exception to a
        :class:`mikkoo.worker.EventError`.

        """
        LOGGER.debug('Event %s was returned as (%s) %s from RabbitMQ',
                     self.current_event['ev_id'], method.reply_code,
                     method.reply_text)
        self.stats.incr('amqp.message_returned')
        self.event_processed.set_exception(
            EventError(self.current_event, method.reply_text))

    async def process_batch(self):
        """Query PgQ for a batch and process it, scheduling the next execution
        of itself with the IOLoop.

        """
        if not self.is_idle:
            LOGGER.warning('Process batch invoked while %s',
                           self.state_description)
            return

        self.set_state(self.STATE_PROCESSING)
        try:
            batch = await self.callproc(
                'pgq.next_batch', self.worker_name, self.consumer_name)
        except PgQError:
            batch = {}

        if batch and batch[0].get('next_batch') is None:
            self.stats.incr('empty_queue')
            LOGGER.debug('Sleeping for %.2f seconds', self.wait_duration)
            self.set_state(self.STATE_IDLE)
            self.stats.add_timing('sleep', self.wait_duration)
            self.ioloop.call_later(
                self.wait_duration,
                lambda: asyncio.ensure_future(self.process_batch()))
            return

        self.current_batch = batch[0]['next_batch']
        LOGGER.debug('Grabbing events for %s', self.current_batch)

        try:
            self.event_list = await self.callproc(
                'pgq.get_batch_events', self.current_batch)
        except PgQError as error:
            LOGGER.error('Error getting batch: %s', error)
            self.set_state(self.STATE_IDLE)
            return self.async_call_soon(self.process_batch)
        LOGGER.debug('Received %i rows to process', len(self.event_list))
        self.async_call_soon(self.process_event)

    @property
    def is_processing(self) -> bool:
        """Returns a bool specifying if the consumer is currently processing"""
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    async def on_batch_complete(self):
        LOGGER.debug('Batch %s complete', self.current_batch)
        await self.callproc('pgq.finish_batch', self.current_batch)
        self.stats.incr(self.BATCHES)
        self.current_batch = None
        if self.is_waiting_to_shutdown:
            return self.on_ready_to_stop()
        self.set_state(self.STATE_IDLE)
        self.async_call_soon(self.process_batch)

    def process_event(self):
        if not self.event_list:
            return self.on_batch_complete()
        elif not self.is_processing:
            LOGGER.debug('Processing of %i events paused due to %s state',
                         len(self.event_list), self.state_description)
            return

        self.event_processed = asyncio.Future()
        self.event_processed.add_done_callback(self.on_event_processed)
        self.current_event = event = self.event_list.pop(0)

        self.stats.incr(f'publish.{event["ev_extra1"]}.{event["ev_type"]}')
        try:
            self.rabbitmq_channel.basic_publish(
                event['ev_extra1'], event['ev_type'], event['ev_data'],
                self.build_properties_kwargs(event), mandatory=True)
        except TypeError as error:
            self.send_exception_to_sentry()
            self.event_processed.set_exception(
                EventError(
                    event, f'Error building kwargs for the event: {error}'))
        if not self.confirm:
            self.event_processed.set_result(True)

    def build_properties_kwargs(self, event: dict) -> spec.BasicProperties:
        """Build the :class:`~pika.BasicProperties` object to use when
        publishing the AMQP message

        """
        kwargs = {
            'app_id': self.AMQP_APP_ID,
            'content_type': event['ev_extra2'],
            'correlation_id': str(uuid.uuid4()),
            'timestamp': self.get_timestamp(event['ev_time'])
        }
        if event['ev_extra4']:
            kwargs['headers'] = json.loads(event['ev_extra4'].encode('utf-8'))
        if event['ev_extra3']:
            try:
                properties = json.loads(event['ev_extra3'])
            except ValueError:
                LOGGER.warning('Failed to decode ev_extra3: %r',
                               event['ev_extra3'])
                properties = {}
            for key in properties:
                if key.encode('ascii') in self.VALID_PROPERTIES:
                    kwargs[key.encode('ascii')] = properties[key]
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers'].setdefault('origin', socket.getfqdn())
        kwargs['headers'].setdefault('sequence', str(uuid7()))
        kwargs['headers'].setdefault('txid', event['ev_txid'])
        kwargs['headers'].setdefault(
            'timestamp', arrow.get(event['ev_time']).to('utc').isoformat())
        return pika.BasicProperties(**kwargs)

    @staticmethod
    def get_timestamp(value: datetime.datetime) -> int:
        """Return the timestamp in UTC"""
        return int(arrow.get(value).to('utc').timestamp())

    def on_event_processed(self, future):
        """Invoked once the message is published or an exception is raised
        when processing the message.

        :param future: Future representing the disposition of the processing
        :type future: :class:`~tornado.concurrent.TracebackFuture`

        """
        exc = future.exception()
        if exc:
            self.stats.incr(self.ERROR)
            LOGGER.error(str(exc))
            if exc.event.get('ev_retry', 0) >= self.maximum_failures:
                LOGGER.warning('Discarding event %s: %r',
                               exc.event['ev_id'],
                               exc.event)
            else:
                self.async_call_soon(
                    self.on_event_error,  self.current_batch,
                    exc.event['ev_id'])
        else:
            self.stats.incr(self.PROCESSED)
        self.current_event = None
        self.async_call_soon(self.process_event)

    async def on_event_error(self, batch, ev_id):
        LOGGER.debug('on_event_error %s %s', batch, ev_id)
        await self.callproc(
            'pgq.event_retry', batch, ev_id, self.retry_interval)

    def run(self):
        """Entry-point that is automatically invoked as part of the
        :class:`~multiprocess.Process` startup. The process will block here
        until it is shutdown.

        Processing of PgQ batches is started in
        :meth:`~mikkoo.worker.Process.on_channel_open`.

        """
        self.set_state(self.STATE_INITIALIZING)
        self.ioloop = asyncio.get_event_loop()
        self.ioloop.run_until_complete(self.setup())
        if not self.is_stopped:
            LOGGER.info('%s worker started', self.name)
            try:
                self.ioloop.run_forever()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')
            finally:
                self.ioloop.run_until_complete(
                    self.ioloop.shutdown_asyncgens())
                self.ioloop.close()

    async def setup(self) -> typing.NoReturn:
        """Ensure that all the things that are required are set up when the
        Process is started.

        """
        helper.config.LoggingConfig(self.config['logging']).configure()

        if not await self.connect_to_postgres():
            LOGGER.info('Could not connect to Postgres, stopping')
            self.set_state(self.STATE_STOPPED)
            return

        await self.stats.start()

        self.stats[self.ERROR] = 0
        self.stats[self.PROCESSED] = 0
        self.stats[self.PENDING] = 0

        await self.create_queue()
        await self.register_consumer()

        self.setup_signal_handlers()
        self.connect_to_rabbitmq()

    async def create_queue(self) -> typing.NoReturn:
        """Create the PgQ for the worker."""
        LOGGER.debug('Fetching PgQ information')
        result = await self.callproc('pgq.create_queue', self.worker_name)
        if not result:
            LOGGER.debug('Queue already exists')

    async def register_consumer(self) -> typing.NoReturn:
        """Register the consumer. If registration fails, shutdown the worker.

        """
        results = await self.callproc(
            'pgq.register_consumer', self.worker_name, self.consumer_name)
        if not results:
            LOGGER.critical('Registration of the consumer failed')
            self.async_call_soon(self.stop)

        elif not results[0]['register_consumer']:
            LOGGER.debug('Consumer is already registered')

    async def unregister_consumer(self) -> typing.NoReturn:
        """Unregister the consumer with PgQ"""
        await self.callproc(
            'pgq.unregister_consumer', self.worker_name, self.consumer_name)

    def on_ready_to_stop(self) -> typing.NoReturn:
        """Invoked when the worker is shutting down and is no longer processing
        a PgQ batch.

        """
        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # Reset any signal handlers
        self.ioloop.remove_signal_handler(signal.SIGABRT)
        self.ioloop.remove_signal_handler(signal.SIGPROF)

        # Unregister the consumer from PgQ
        if self.worker_config.get('unregister', True):
            self.unregister_consumer()

        # If the connection is still around, close it
        if self.rabbitmq and self.rabbitmq.is_open:
            LOGGER.debug('Closing connection to RabbitMQ')
            self.rabbitmq.close()

        # Stop the IOLoop
        if self.ioloop:
            LOGGER.debug('Stopping IOLoop')
            self.ioloop.stop()

        # Note that shutdown is complete and set the state accordingly
        self.set_state(self.STATE_STOPPED)
        LOGGER.debug('Shutdown complete')

    def setup_signal_handlers(self):
        """Set up the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        self.ioloop.add_signal_handler(signal.SIGABRT, self.on_sigabrt)
        self.ioloop.add_signal_handler(signal.SIGPROF, self.on_sigprof)
        LOGGER.debug('Signal handlers setup')

    def async_call_soon(self, func: typing.Callable, *args) -> typing.NoReturn:
        self.ioloop.call_soon(asyncio.ensure_future(func(*args)))

    def on_sigabrt(self) -> typing.NoReturn:
        """Invoked when the MCP sends a SIGABRT to shut down the worker"""
        LOGGER.debug('on_sigabrt when %s', self.state_description)
        self.async_call_soon(self.stop)

    def on_sigprof(self) -> typing.NoReturn:
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        """
        LOGGER.debug('on_sigprof')
        signal.siginterrupt(signal.SIGPROF, False)
        self.async_call_soon(self.submit_stats_report)

    async def submit_stats_report(self):
        """Invoked by the IOLoop"""
        LOGGER.debug('Submitting stats report')
        result = await self.callproc(
            'pgq.get_consumer_info', self.worker_name, self.consumer_name)
        self.stats.set_gauge(self.PENDING, result[0].get('pending_events', 0))
        self.stats_queue.put(self.stats.report(), True)
        self.last_stats_time = time.time()

    async def stop(self) -> typing.NoReturn:
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.
        """
        LOGGER.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            LOGGER.warning('Stop requested but worker is already stopped')
            return
        elif self.is_shutting_down:
            LOGGER.warning('Stop requested, worker is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            LOGGER.warning('Stop requested but already waiting to shut down')
            return

        # Wait until the consumer has finished processing to shut down
        if self.is_processing:
            LOGGER.info('Waiting for batch to finish processing')
            self.set_state(self.STATE_STOP_REQUESTED)
            return

        # Stop and flush the stats data
        await self.stats.stop()

        self.on_ready_to_stop()

    @staticmethod
    def send_exception_to_sentry() -> typing.NoReturn:
        """Send an exception to Sentry if enabled."""
        if sentry_sdk:
            sentry_sdk.capture_exception(sys.exc_info())

    @staticmethod
    def set_sentry_context(tag: str, value: str) -> typing.NoReturn:
        """Set a context tag in Sentry for the given key and value."""
        if sentry_sdk:
            LOGGER.debug('Setting sentry context for %s to %s', tag, value)
            sentry_sdk.set_tag(tag, value)


class MikkooError(Exception):
    """Base class for all Mikkoo exceptions"""
    pass


class EventError(MikkooError):
    """Raised when there is an error processing the event

    :param dict event: The event that is being processed
    :param str msg: The error message

    """
    def __init__(self, event, msg):
        self.event = copy.deepcopy(event)
        self.msg = msg

    def __repr__(self):
        return '<EventError event={0}>'.format(self.event['ev_id'])

    def __str__(self):
        return 'EventError ({0}): {1}'.format(self.event['ev_id'], self.msg)


class PgQError(MikkooError):
    pass

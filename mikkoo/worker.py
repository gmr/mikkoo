"""
Mikkoo Worker Process
=====================
Connects to PostgreSQL and processes PgQ batches, publishing the events in
the batch to RabbitMQ.

"""
import contextlib
import json
import logging
import multiprocessing
import signal
import time
import uuid

from tornado import concurrent
from tornado import gen
from tornado import ioloop
import pika
import queries
from pika import spec

try:
    import raven
except ImportError:
    raven = None

from mikkoo import __version__
from mikkoo import state
from mikkoo import stats

LOGGER = logging.getLogger(__name__)


class Process(multiprocessing.Process, state.State):

    AMQP_APP_ID = 'mikkoo/%s' % __version__
    DEFAULT_WAIT_DURATION = 5
    ERROR = 'failed'
    PROCESSED = 'processed'
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
        self.channel = None
        self.confirm_future = None
        self.connection = None
        self.consumer_id = None
        self.ioloop = None
        self.last_stats_time = None
        self.prepend_path = None
        self.sentry_client = None
        self.session = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.stats = None
        self.wait_duration = None

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    @property
    def config(self):
        """Return the configuration for the worker.

        :rtype: helper.config.Config

        """
        return self._kwargs['config']['worker']

    @property
    def confirm(self):
        """Indicates if the consumer has publisher confirmations enabled.

        :rtype: bool

        """
        return self._kwargs['config']['worker'].get('confirm', False)

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    @property
    def worker_name(self):
        """Return the queue to populate with runtime stats

        :rtype: multiprocessing.Queue

        """
        return self._kwargs['worker_name']

    def build_properties_kwargs(self, event):
        """Build the :class:`~pika.BasicProperties` object to use when
        publishing the AMQP message

        :param dict event: The event to build kwargs for
        :rtype: :class:`~pika.BasicProperties`

        """
        kwargs = {
            'app_id': self.AMQP_APP_ID,
            'content_type': event['ev_extra2'],
            'correlation_id': str(uuid.uuid4()),
            'timestamp': int(event['ev_time'].strftime('%s'))
        }
        if event['ev_extra4']:
            kwargs['headers'] = json.loads(event['ev_extra4'].encode('utf-8'))
        if event['ev_extra3']:
            for key in event['ev_extra3']:
                if key.encode('ascii') in self.VALID_PROPERTIES:
                    kwargs[key.encode('ascii')] = event['ev_extra3'][key]
        LOGGER.debug('Properties: %r', kwargs)
        return pika.BasicProperties(**kwargs)

    def callproc(self, name, args):
        """Call the stored procedure in PostgreSQL with the specified
        arguments.

        :param str name: The stored procedure name
        :param list args: The list of arguments to pass in to the stored proc
        :rtype: :class:`~queries.Result`

        """
        self.set_sentry_context('callproc', name)
        LOGGER.debug('Executing %s with %r', name, args)
        with self.statsd_track_duration('db.{0}.query_time'.format(name)):
            try:
                return self.session.callproc(name, args or [])
            except queries.Error as error:
                self.log_db_error(name, error)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self.connection.close()

    def connect_to_postgres(self):
        """Connects to PostgreSQL, shutting down the worker on failure"""
        LOGGER.debug('Connecting to PostgreSQL')
        try:
            self.session = queries.Session(self.config.get('postgres_url'),
                                           pool_max_size=1)
        except queries.OperationalError as error:
            LOGGER.error('Error connecting to PostgreSQL: %s', error)
            self.stop()

    def connect_to_rabbitmq(self):
        """Connect to RabbitMQ returning the connection handle."""
        self.set_state(self.STATE_CONNECTING)
        params = pika.URLParameters(self.config.get('rabbitmq_url'))
        LOGGER.debug('Connecting to %s:%i:%s as %s',
                     params.host, params.port, params.virtual_host,
                     params.credentials.username)
        return pika.TornadoConnection(params,
                                      self.on_connect_open,
                                      self.on_connect_failed,
                                      self.on_closed,
                                      False,
                                      self.ioloop)

    def create_queue(self):
        """Create the PgQ for the worker.

        :rtype: :class:`~queries.Result`

        """
        LOGGER.debug('Fetching PgQ information')
        result = self.callproc('pgq.create_queue', [self.worker_name])
        if not result:
            LOGGER.debug('Queue already exists')

    def get_queue_info(self):
        """Grab info from PgQ about the queue for this worker

        :rtype: :class:`~queries.Result`

        """
        LOGGER.debug('Fetching PgQ information')
        return self.callproc('pgq.get_queue_info', [self.worker_name])

    def log_db_error(self, name, error):
        """Log database errors and increment the statsd counter

        :param str name: The stored procedure name
        :param error: The exception
        :type error: :class:`~queries.Error`

        """
        LOGGER.error('Error executing %s.callproc: (%s) %s', name,
                     error.__class__.__name__, error)
        self.statsd_incr('db.{0}.error.{1}'.format(name, str(error)))

    def on_channel_closed(self, _channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel _channel: The AMQP Channel
        :param int reply_code: The AMQP reply code
        :param str reply_text: The AMQP reply text

        """
        LOGGER.critical('Channel was closed: (%s) %s', reply_code, reply_text)
        del self.channel
        self.on_ready_to_stop()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Channel opened')
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        if self.confirm:
            LOGGER.info('Enabling publisher confirmations')
            self.channel.confirm_delivery(self.on_confirm)

    def on_closed(self, _unused, code, text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Shutdown if not already doing so.

        :param pika.connection.Connection _unused: The closed connection
        :param int code: The AMQP reply code
        :param str text: The AMQP reply text

        """
        LOGGER.critical('Connection from RabbitMQ closed in state %s (%s, %s)',
                        self.state_description, code, text)
        self.channel = None
        if not self.is_shutting_down and not self.is_waiting_to_shutdown:
            self.on_ready_to_stop()

    def on_confirm(self, frame):
        LOGGER.debug('On confirmation: %r', frame)
        self.confirm_future.set_result(isinstance(frame, spec.Basic.Ack))

    def on_connect_failed(self, *args, **kwargs):
        LOGGER.critical('Could not connect to RabbitMQ: %r', (args, kwargs))
        self.on_ready_to_stop()

    def on_connect_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :type connection: pika.adapters.tornado_connection.TornadoConnection
        """
        LOGGER.debug('Connection opened')
        self.connection = connection
        self.open_channel()

    def on_ready_to_stop(self):
        """Invoked when the worker is shutting down and is no longer processing
        a PgQ batch.

        """
        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # Reset any signal handlers
        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # Unregister from PgQ
        self.unregister_consumer()

        # If the connection is still around, close it
        if self.connection and self.connection.is_open:
            LOGGER.debug('Closing connection to RabbitMQ')
            self.connection.close()

        # Stop the IOLoop
        if self.ioloop:
            LOGGER.debug('Stopping IOLoop')
            self.ioloop.stop()

        # Note that shutdown is complete and set the state accordingly
        self.set_state(self.STATE_STOPPED)
        LOGGER.info('Shutdown complete')

    def on_sigprof(self, _unused_signum, _unused_frame):
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int _unused_signum: The signal number
        :param frame _unused_frame: The python frame the signal was received at

        """
        self._kwargs['stats_queue'].put(self.stats.report(), True)
        self.last_stats_time = time.time()
        signal.siginterrupt(signal.SIGPROF, False)

    def open_channel(self):
        """Open a channel on the existing open connection to RabbitMQ"""
        LOGGER.debug('Opening a channel on %r', self.connection)
        self.connection.channel(self.on_channel_open)

        # Schedule the processing of the first batch
        self.ioloop.add_callback(self.process_batch)

    @gen.coroutine
    def process_batch(self):
        """Query PgQ for a batch and process it, scheduling the next execution
        with the IOLoop.

        """
        batch = self.callproc('pgq.next_batch',
                               [self.worker_name, self.name]).as_dict()
        if not batch['next_batch']:
            LOGGER.debug('Empty queue, sleeping for %.2f seconds',
                         self.wait_duration)
            self.ioloop.add_timeout(self.ioloop.time() + self.wait_duration,
                                    self.process_batch)
            return

        result = self.callproc('pgq.get_batch_events', [batch['next_batch']])
        LOGGER.debug('Processing %i events', result.count())
        for row in result:
            try:
                result = self.publish_event(row)
                if concurrent.is_future(result):
                    yield result
            except EventError as error:
                self.callproc('pgq.event_failed', [batch['next_batch'],
                                                   row['ev_id'], str(error)])

        # Mark the batch as complete
        self.callproc('pgq.finish_batch', [batch['next_batch']])

        # Process the next batch
        self.ioloop.add_callback(self.process_batch)

    def publish_event(self, event):
        """Publish the event to RabbitMQ. If publisher confirmations are
        enabled, a future is returned that will complete when the confirmation
        is received.

        :param dict event: The PgQ event to publish
        :rtype: None or :class:`~tornado.concurrent.Future`

        """

        LOGGER.debug('Publishing a %i byte message to %s using %s',
                     len(event['ev_data']), event['ev_extra1'],
                     event['ev_type'])
        self.statsd_incr('publish.{0}.{1}'.format(event['ev_extra1'],
                                                  event['ev_type']))
        try:
            properties = self.build_properties_kwargs(event)
        except TypeError as error:
            LOGGER.error('Error building kwargs for the event: %s', error)
            raise EventError(str(error))

        if self.confirm:
            self.confirm_future = concurrent.Future()

        self.channel.basic_publish(event['ev_extra1'],
                                   event['ev_type'],
                                   event['ev_data'],
                                   properties)
        return self.confirm_future

    def sleep(self):
        """Invoked when the queue is empty, sleeping for the wait duration

        :rtype: future

        """
        LOGGER.debug('Sleeping for %.2f seconds', self.wait_duration)
        future = concurrent.Future()
        self.ioloop.add_timeout(self.wait_duration,
                                lambda r: future.set_result(True))

    def register_consumer(self):
        """Register the consumer. If registration fails, shutdown the worker.

        :return:
        """
        results = self.callproc('pgq.register_consumer',
                                [self.worker_name, self.name])
        if results is None:
            LOGGER.critical('Registration of the consumer failed')
            self.stop()

        elif not results.as_dict()['register_consumer']:
            LOGGER.debug('Consumer is already registered')

    def run(self):
        """Entry-point that is automatically invoked as part of the
        :class:`~multiprocess.Process` startup. The process will block here
        until it is shutdown.

        """
        LOGGER.info('%s running', self.name)
        self.setup()
        if not self.is_stopped:
            try:
                self.ioloop.start()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')

    def set_sentry_context(self, tag, value):
        """Set a context tag in Sentry for the given key and value.
        :param str tag: The context tag name
        :param str value: The context value
        """
        if self.sentry_client:
            LOGGER.debug('Setting sentry context for %s to %s', tag, value)
            self.sentry_client.tags_context({tag: value})

    def setup(self):
        """Ensure that all the things that are required are setup when the
        Process is started.

        """
        self.setup_signal_handlers()
        self.ioloop = ioloop.IOLoop.current()
        self.ioloop.add_callback(self.connect_to_rabbitmq)
        self.stats = stats.Stats(self.name,
                                 self._kwargs['worker_name'],
                                 self._kwargs['config']['statsd'])
        self.connect_to_postgres()
        self.create_queue()
        self.register_consumer()
        self.wait_duration = \
            self._kwargs['config']['worker'].get('wait_duration',
                                                 self.DEFAULT_WAIT_DURATION)

    def setup_signal_handlers(self):
        """Setup the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.signal(signal.SIGABRT, self.stop)
        signal.siginterrupt(signal.SIGPROF, False)
        signal.siginterrupt(signal.SIGABRT, False)
        LOGGER.debug('Signal handlers setup')

    def statsd_add_timing(self, key, duration):
        """Add a timing to statsd
        :param str key: The key to add the timing to
        :param int|float duration: The timing value
        """
        if self.stats.statsd:
            self.stats.statsd.add_timing(key, duration)

    def statsd_incr(self, key, value=1):
        """Increment the specified key in statsd if statsd is enabled.
        :param str key: The key to increment
        :param int value: The value to increment the key by
        """
        if self.stats.statsd:
            self.stats.statsd.incr(key, value)

    @contextlib.contextmanager
    def statsd_track_duration(self, key):
        """Time around a context and emit a statsd metric.
        :param str key: The key for the timing to track
        """
        start_time = time.time()
        try:
            yield
        finally:
            finish_time = max(start_time, time.time())
            self.statsd_add_timing(key, finish_time - start_time)

    def stop(self, signum=None, _unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.
        :param int signum: The signal received
        :param frame _unused: The stack frame from when the signal was called
        """
        LOGGER.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            LOGGER.warning('Stop requested but consumer is already stopped')
            return
        elif self.is_shutting_down:
            LOGGER.warning('Stop requested, consumer is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            LOGGER.warning('Stop requested but already waiting to shut down')
            return

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            LOGGER.info('Waiting for consumer to finish processing')
            self.set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGTERM:
                signal.siginterrupt(signal.SIGTERM, False)
            return

        # Stop and flush the statds data
        if self.stats.statsd:
            self.stats.statsd.stop()

        self.on_ready_to_stop()

    def unregister_consumer(self):
        """Unregister the consumer with PgQ"""
        self.callproc('pgq.unregister_consumer', [self.worker_name, self.name])


class MikkooError(Exception): pass

class EventError(MikkooError): pass

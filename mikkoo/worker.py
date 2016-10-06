"""
Mikkoo Worker Process
=====================
Connects to PostgreSQL and processes PgQ batches, publishing the events in
the batch to RabbitMQ.

"""
import contextlib
import copy
import json
import logging
import multiprocessing
import signal
import sys
import time
import uuid

import arrow
from tornado import concurrent
from tornado import ioloop
import pika
import queries

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
        self.channel = None
        self.connection = None
        self.config = None
        self.confirm = True
        self.consumer_name = self.DEFAULT_CONSUMER_NAME
        self.current_batch = None
        self.current_event = None
        self.event_list = None
        self.event_processed = None
        self.ioloop = None
        self.last_stats_time = None
        self.maximum_failures = self.DEFAULT_MAX_FAILURES
        self.prepend_path = None
        self.retry_interval = self.DEFAULT_RETRY_INTERVAL
        self.sentry_client = None
        self.session = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.stats = None
        self.wait_duration = self.DEFAULT_WAIT_DURATION

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    @property
    def worker_name(self):
        """Return the queue to populate with runtime stats

        :rtype: multiprocessing.Queue

        """
        return self._kwargs['worker_name']

    def connect_to_postgres(self):
        """Connects to PostgreSQL, shutting down the worker on failure"""
        LOGGER.debug('Connecting to PostgreSQL')
        try:
            self.session = queries.Session(self.config.get('postgres_url'),
                                           pool_max_size=1)
        except queries.OperationalError as error:
            self.send_exception_to_sentry()
            LOGGER.error('Error connecting to PostgreSQL: %s', error)
            return False
        return True

    def callproc(self, name, *args):
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
            except queries.InternalError as error:
                self.send_exception_to_sentry()
                self.log_db_error(name, error)
                raise PgQError(str(error))
            except queries.Error as error:
                self.send_exception_to_sentry()
                self.log_db_error(name, error)

    def log_db_error(self, name, error):
        """Log database errors and increment the statsd counter

        :param str name: The stored procedure name
        :param error: The exception
        :type error: :class:`~queries.Error`

        """
        LOGGER.error('Error executing %s.callproc: (%s) %s', name,
                     error.__class__.__name__, error)
        self.statsd_incr('db.{0}.error.{1}'.format(name, str(error)))

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

    def on_connect_failed(self, *args, **kwargs):
        """Connection to RabbitMQ failed, so shut things down.

        :param list args: Positional arguments
        :param dict kwargs: Keyword arguments

        """
        LOGGER.critical('Could not connect to RabbitMQ: %r', (args, kwargs))
        self.statsd_incr('amqp.connection_failed')
        self.on_ready_to_stop()

    def on_connect_open(self, conn):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type conn: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.info('Connection to RabbitMQ established')
        self.statsd_incr('amqp.connection_opened')
        self.connection = conn
        conn.add_on_connection_blocked_callback(self.on_connection_blocked)
        conn.add_on_connection_unblocked_callback(self.on_connection_unblocked)
        self.open_channel()

    def on_connection_blocked(self, _frame):
        """Invoked when the RabbitMQ connection has notified us that it is
        blocking publishing.

        :type _frame: :class:`pika.spec.Connection.Blocked`

        """
        LOGGER.warning('RabbitMQ is blocking the connection')
        self.statsd_incr('amqp.connection_blocked')
        self.set_state(self.STATE_BLOCKED)

    def on_connection_unblocked(self, _frame):
        """Invoked when the RabbitMQ connection has notified us that it
        publishing is no longer unblocked.

        :type _frame: :class:`pika.spec.Connection.Unblocked`

        """
        LOGGER.info('RabbitMQ is no longer blocking the connection')
        self.statsd_incr('amqp.connection_unblocked')
        # If we were blocked while processing, resume
        if self.event_list:
            LOGGER.debug('Resuming the processing of %i events',
                         len(self.event_list))
            self.set_state(self.STATE_PROCESSING)
            self.ioloop.add_callback(self.process_event)
        else:
            self.set_state(self.STATE_IDLE)
            self.ioloop.add_callback(self.process_batch)

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
        self.statsd_incr('amqp.connection_closed')
        if self.is_shutting_down or self.is_waiting_to_shutdown:
            self.on_ready_to_stop()
        else:
            LOGGER.info('Reconnecting to RabbitMQ')
            self.ioloop.add_callback(self.connect_to_rabbitmq)

    def open_channel(self):
        """Open a channel on the existing open connection to RabbitMQ"""
        LOGGER.debug('Opening a new channel')
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Channel %i opened', channel.channel_number)
        self.statsd_incr('amqp.channel_opened')
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        if self.confirm:
            LOGGER.info('Enabling publisher confirmations')
            self.channel.add_on_return_callback(self.on_publish_return)
            self.channel.confirm_delivery(self.on_publish_confirm)

        # Schedule the processing of the first batch if it's not reopening
        if not self.is_reconnecting:
            self.set_state(self.STATE_IDLE)
            self.ioloop.add_callback(self.process_batch)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The AMQP Channel
        :param int reply_code: The AMQP reply code
        :param str reply_text: The AMQP reply text

        """
        LOGGER.warning('Channel %i closed: (%s) %s',
                       channel.channel_number, reply_code, reply_text)
        self.statsd_incr('amqp.channel_closed')
        if concurrent.is_future(self.event_processed):
            self.set_state(self.STATE_RECONNECTING)

            def on_open(new_channel):
                self.on_channel_open(new_channel)
                self.set_state(self.STATE_PROCESSING)
                exc = EventError(self.current_event, reply_text)
                self.event_processed.set_exception(exc)

            return self.connection.channel(on_open)

        del self.channel
        self.on_ready_to_stop()

    def process_batch(self):
        """Query PgQ for a batch and process it, scheduling the next execution
        of itself with the IOLoop.

        """
        if not self.is_idle:
            LOGGER.warning('Process batch invoked while %s',
                           self.state_description)
            return

        self.set_state(self.STATE_PROCESSING)
        try:
            batch = self.callproc('pgq.next_batch', self.worker_name,
                                  self.consumer_name).as_dict()
        except PgQError:
            batch = {}

        if batch.get('next_batch') is None:
            self.statsd_incr('empty_queue')
            LOGGER.debug('Sleeping for %.2f seconds', self.wait_duration)
            self.set_state(self.STATE_IDLE)
            self.statsd_add_timing('sleep', self.wait_duration)
            self.ioloop.add_timeout(self.ioloop.time() + self.wait_duration,
                                    self.process_batch)
            return

        self.current_batch = batch['next_batch']

        # Grab all of the events in the batch and convert the result to a list
        try:
            result = self.callproc('pgq.get_batch_events', self.current_batch)
        except PgQError as error:
            LOGGER.error('Error getting batch: %s', error)
            self.set_state(self.STATE_IDLE)
            return self.ioloop.add_callback(self.process_batch)

        LOGGER.debug('Processing %i events', result.count())
        self.event_list = [dict(row) for row in result]

        # Start the processing of the event list
        self.ioloop.add_callback(self.process_event)

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    def on_batch_complete(self):
        LOGGER.debug('Batch %s complete', self.current_batch)
        self.callproc('pgq.finish_batch', self.current_batch)
        self.stats.incr(self.BATCHES)
        self.current_batch = None
        if self.is_waiting_to_shutdown:
            return self.on_ready_to_stop()
        self.set_state(self.STATE_IDLE)
        self.ioloop.add_callback(self.process_batch)

    def process_event(self):
        if not self.event_list:
            return self.on_batch_complete()
        elif not self.is_processing:
            LOGGER.debug('Processing of %i events paused due to %s state',
                         len(self.event_list), self.state_description)
            return

        self.event_processed = concurrent.TracebackFuture()
        self.event_processed.add_done_callback(self.on_event_processed)
        self.current_event = event = self.event_list.pop(0)

        self.statsd_incr('publish.{0}.{1}'.format(event['ev_extra1'],
                                                  event['ev_type']))
        try:
            self.channel.basic_publish(event['ev_extra1'],
                                       event['ev_type'],
                                       event['ev_data'],
                                       self.build_properties_kwargs(event),
                                       mandatory=True)
        except TypeError as error:
            self.send_exception_to_sentry()
            message = 'Error building kwargs for the event: {0}'.format(error)
            self.event_processed.set_exception(EventError(event, message))
        if not self.confirm:
            self.event_processed.set_result(True)

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
            'timestamp': self.get_timestamp(event['ev_time'])
        }
        if event['ev_extra4']:
            kwargs['headers'] = json.loads(event['ev_extra4'].encode('utf-8'))
        if event['ev_extra3']:
            try:
                properties = json.loads(event['ev_extra3'])
            except ValueError:
                LOGGER.warning('Failed to decode properties from ev_extra3: %r',
                               event['ev_extra3'])
                properties = {}
            for key in properties:
                if key.encode('ascii') in self.VALID_PROPERTIES:
                    kwargs[key.encode('ascii')] = properties[key]
        return pika.BasicProperties(**kwargs)

    @staticmethod
    def get_timestamp(value):
        """Return the timestamp in UTC

        :param datetime.datetime value: The event time
        :rtype: int

        """
        return arrow.get(value).to('utc').timestamp

    def on_publish_confirm(self, _frame):
        """Invoked by pika when a delivery confirmation is received.

        :param _frame: The confirmation frame
        :type _frame: :class:`pika.spec.Basic.Ack`

        """
        # Acks occur event on a basic_return.
        if self.event_processed and self.current_event:
            LOGGER.debug('Event %s confirmed', self.current_event['ev_id'])
            self.statsd_incr('amqp.publisher_confirm')
            self.event_processed.set_result(True)

    def on_publish_return(self, _channel, method, _properties, _body):
        """Invoked by pika when a delivery failure is received. Setting the
        current confirmation future exception to a
        :class:`mikkoo.worker.EventError`.

        :param _channel: The channel the message was returned on
        :type _channel: :class:`~pika.channel.Channel`
        :param method: The ``Basic.Return`` method frame
        :type method: :class:`~pika.spec.Basic.Return`
        :param _properties: The message properties of the returned message
        :type _properties: :class:`~pika.spec.Basic.Properties`
        :param bytes _body: The message body that was returned

        """
        LOGGER.debug('Event %s was returned as (%s) %s from RabbitMQ',
                     self.current_event['ev_id'], method.reply_code,
                     method.reply_text)
        self.statsd_incr('amqp.message_returned')
        self.event_processed.set_exception(EventError(self.current_event,
                                                      method.reply_text))

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
                self.callproc('pgq.event_retry', self.current_batch,
                              exc.event['ev_id'], self.retry_interval)
        else:
            self.stats.incr(self.PROCESSED)
        self.current_event = None
        self.ioloop.add_callback(self.process_event)

    def run(self):
        """Entry-point that is automatically invoked as part of the
        :class:`~multiprocess.Process` startup. The process will block here
        until it is shutdown.

        Processing of PgQ batches is started in
        :meth:`~mikkoo.worker.Process.on_channel_open`.

        """
        self.setup()
        if not self.is_stopped:
            LOGGER.info('%s worker started', self.name)
            try:
                self.ioloop.start()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')
            except Exception:
                self.send_exception_to_sentry()

    def setup(self):
        """Ensure that all the things that are required are setup when the
        Process is started.

        """
        if raven and 'sentry_dsn' in self._kwargs['config']:
            options = {
                'include_paths': ['mikkoo', 'pika', 'psycopg2', 'queries',
                                  'tornado']
            }
            self.sentry_client = \
                raven.Client(self._kwargs['config']['sentry_dsn'], **options)
            self.set_sentry_context('worker', self.name)

        self.config = self._kwargs['config']['worker']
        self.confirm = self.config.get('confirm', False)
        self.consumer_name = self.config.get('consumer_name',
                                             self.DEFAULT_CONSUMER_NAME)
        self.maximum_failures = self.config.get('max_failures',
                                                self.DEFAULT_MAX_FAILURES)
        self.retry_interval = self.config.get('retry_interval',
                                              self.DEFAULT_RETRY_INTERVAL)
        self.wait_duration = self.config.get('wait_duration',
                                             self.DEFAULT_WAIT_DURATION)

        if not self.connect_to_postgres():
            LOGGER.info('Could not connect to PostgreSQL, stopping')
            self.set_state(self.STATE_STOPPED)
            return

        self.stats = stats.Stats(self.name,
                                 self._kwargs['worker_name'],
                                 self._kwargs['config']['statsd'])
        self.stats[self.ERROR] = 0
        self.stats[self.PROCESSED] = 0
        self.stats[self.PENDING] = 0

        self.create_queue()
        self.register_consumer()

        self.setup_signal_handlers()
        self.ioloop = ioloop.IOLoop.current()

        self.ioloop.add_callback(self.connect_to_rabbitmq)

    def create_queue(self):
        """Create the PgQ for the worker.

        :rtype: :class:`~queries.Result`

        """
        LOGGER.debug('Fetching PgQ information')
        result = self.callproc('pgq.create_queue', self.worker_name)
        if not result:
            LOGGER.debug('Queue already exists')

    def register_consumer(self):
        """Register the consumer. If registration fails, shutdown the worker.

        """
        results = self.callproc('pgq.register_consumer',
                                self.worker_name, self.consumer_name)
        if results is None:
            LOGGER.critical('Registration of the consumer failed')
            self.stop()

        elif not results.as_dict()['register_consumer']:
            LOGGER.debug('Consumer is already registered')

    def unregister_consumer(self):
        """Unregister the consumer with PgQ"""
        self.callproc('pgq.unregister_consumer',
                      self.worker_name, self.consumer_name)

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

        # Unregister the consumer from PgQ
        if self._kwargs['config']['worker'].get('unregister', True):
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
        LOGGER.debug('Shutdown complete')

    def setup_signal_handlers(self):
        """Setup the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.signal(signal.SIGABRT, self.stop)
        signal.siginterrupt(signal.SIGPROF, False)
        signal.siginterrupt(signal.SIGABRT, False)
        LOGGER.debug('Signal handlers setup')

    def on_sigprof(self, _unused_signum, _unused_frame):
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int _unused_signum: The signal number
        :param frame _unused_frame: The python frame the signal was received at

        """
        LOGGER.debug('on_sigprof')
        signal.siginterrupt(signal.SIGPROF, False)
        self.ioloop.add_callback_from_signal(self.submit_stats_report)

    def submit_stats_report(self):
        """Invoked by the IOLoop"""
        LOGGER.debug('Submitting stats report')
        result = self.callproc('pgq.get_consumer_info', self.worker_name,
                               self.consumer_name)
        LOGGER.debug('Post consumer info: %r', result)
        self.stats.set_gauge(self.PENDING, result[0].get('pending_events', 0))
        self._kwargs['stats_queue'].put(self.stats.report(), True)
        self.last_stats_time = time.time()

    def stop(self, signum=None, _unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.
        :param int signum: The signal received
        :param frame _unused: The stack frame from when the signal was called
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

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            LOGGER.info('Waiting for batch to finish processing')
            self.set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGTERM:
                signal.siginterrupt(signal.SIGTERM, False)
            return

        # Stop and flush the statds data
        if self.stats.statsd:
            self.stats.statsd.stop()

        self.on_ready_to_stop()

    def send_exception_to_sentry(self):
        """Send an exception to Sentry if enabled.
        :param tuple exc_info: exception information as returned from
            :func:`sys.exc_info`
        """
        if self.sentry_client:
            self.sentry_client.captureException(sys.exc_info())

    def set_sentry_context(self, tag, value):
        """Set a context tag in Sentry for the given key and value.
        :param str tag: The context tag name
        :param str value: The context value
        """
        if self.sentry_client:
            LOGGER.debug('Setting sentry context for %s to %s', tag, value)
            self.sentry_client.tags_context({tag: value})

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

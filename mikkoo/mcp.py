"""
Master Control Program

"""
import logging
import multiprocessing
import os
import psutil
try:
    import Queue as queue
except ImportError:
    import queue
import signal
import sys
import time

from mikkoo import state
from mikkoo import worker
from mikkoo import __version__

LOGGER = logging.getLogger(__name__)


class Worker(object):
    """Class used for keeping track of each worker type being managed by
    the MCP

    """
    def __init__(self, config, stats_queue):
        self.config = config
        self.process = None
        self.stats_queue = stats_queue
        self.unresponsive = 0


class MasterControlProgram(state.State):
    """Master Control Program keeps track of and manages worker processes."""

    MAX_UNRESPONSIVE = 3
    MAX_SHUTDOWN_WAIT = 10
    POLL_INTERVAL = 60.0
    POLL_RESULTS_INTERVAL = 3.0
    SHUTDOWN_WAIT = 1

    def __init__(self, config):
        """Initialize the Master Control Program

        :param helper.config.Config config: Mikkoo Configuration

        """
        self.set_process_name()
        LOGGER.info('Mikkoo v%s initializing', __version__)
        super(MasterControlProgram, self).__init__()
        self.config = config

        self.last_poll_results = dict()
        self.poll_data = {'time': 0, 'processes': list()}
        self.poll_timer = None
        self.results_timer = None
        self.stats = dict()
        self.stats_queue = multiprocessing.Queue()
        self.polled = False

        self.workers = dict()
        for name in config.application.workers.keys():
            self.workers[name] = Worker(config.application.workers[name],
                                        self.stats_queue)

        self.poll_interval = config.application.get('poll_interval',
                                                    self.POLL_INTERVAL)

    @property
    def active_processes(self):
        """Return a list of all active processes, pruning dead ones

        :rtype: list

        """
        active_processes, dead_processes = list(), list()
        for name in self.workers.keys():
            if self.workers[name].process.pid == os.getpid():
                continue
            try:
                proc = psutil.Process(self.workers[name].process.pid)
            except psutil.NoSuchProcess:
                dead_processes.append(name)
                continue

            if self.is_dead(proc):
                dead_processes.append(name)
            else:
                active_processes.append(self.workers[name].process)

        if dead_processes:
            LOGGER.debug('Removing %i dead process(es)', len(dead_processes))
            for name in dead_processes:
                self.remove_worker_process(name)
        return active_processes

    def calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        timestamp = data['timestamp']
        del data['timestamp']
        LOGGER.debug('Calculating stats for data timestamp: %i', timestamp)

        # Iterate through the last poll results
        stats = self.worker_stats_counter()
        worker_stats = dict()
        for name in data.keys():
            worker_stats[name] = self.worker_stats_counter()
            for key in stats:
                value = data[name]['counts'].get(key, 0)
                stats[key] += value
                worker_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self.active_processes)
        return {
            'last_poll': timestamp,
            'workers': worker_stats,
            'process_data': data,
            'counts': stats
        }

    def check_processes(self):
        """Check to make sure the worker processes are working..."""
        LOGGER.debug('Checking worker processes')
        for name in self.workers.keys():
            if self.workers[name].unresponsive >= self.MAX_UNRESPONSIVE:
                if self.workers[name].process:
                    LOGGER.info('Killing unresponsive %s worker', name)
                    os.kill(int(self.workers[name].process.pid),
                            signal.SIGKILL)
                self.workers[name].process = None
        for name in self.workers.keys():
            if (not self.workers[name].process or
                    not self.workers[name].process.is_alive()):
                self.start_process(name)

    def collect_results(self, data_values):
        """Receive the data from the workers polled and process it.

        :param dict data_values: The poll data returned from the worker
        :type data_values: dict

        """
        self.last_poll_results['timestamp'] = self.poll_data['timestamp']

        # Get the name and worker name and remove it from what is reported
        worker_name = data_values['worker_name']
        del data_values['worker_name']
        del data_values['name']

        # Add it to our last poll global data
        self.last_poll_results[worker_name] = data_values

        # Calculate the stats
        self.stats = self.calculate_stats(self.last_poll_results)

    @staticmethod
    def worker_stats_counter():
        """Return a new worker stats counter instance.

        :rtype: dict

        """
        return {
            worker.Process.BATCHES: 0,
            worker.Process.ERROR: 0,
            worker.Process.PROCESSED: 0,
            worker.Process.PENDING: 0
        }

    @staticmethod
    def is_dead(process):
        """Checks to see if the specified process is dead.

        :param psutil.Process process: The process to check
        :rtype: bool

        """
        status = process.status()
        LOGGER.debug('Process status (%r): %r', process.pid, status)
        try:
            if status == psutil.STATUS_ZOMBIE:
                process.terminate()
                return True
            elif status == psutil.STATUS_DEAD:
                return True
        except psutil.NoSuchProcess:
            LOGGER.debug('Process is dead and does not exist')
            return True
        return False

    def kill_processes(self):
        """Gets called on shutdown by the timer when too much time has gone by,
        calling the terminate method instead of nicely asking for the workers
        to stop.

        """
        LOGGER.critical('Max shutdown exceeded, forcibly exiting')
        processes = True
        while processes:
            processes = self.active_processes
            for process in processes:
                if int(process.pid) != int(os.getpid()):
                    LOGGER.warning('Killing %s (%s)',
                                   process.name, process.pid)
                    os.kill(int(process.pid), signal.SIGKILL)
            time.sleep(0.5)

        LOGGER.info('Killed all children')
        return self.set_state(self.STATE_STOPPED)

    def log_stats(self):
        """Output the stats to the LOGGER."""
        if not self.stats.get('counts'):
            LOGGER.info('Did not receive any stats data from children')
            return

        if self.poll_data['processes']:
            LOGGER.warning('%i process(es) did not respond with stats: %r',
                           len(self.poll_data['processes']),
                           self.poll_data['processes'])
            for name in self.poll_data['processes']:
                self.workers[name].unresponsive += 1

        for name in self.stats['workers'].keys():
            self.workers[name].unresponsive = 0
            LOGGER.info('%s worker processed %i events with %i errors '
                        'in %i batches with %i pending events', name,
                        self.stats['workers'][name][worker.Process.PROCESSED],
                        self.stats['workers'][name][worker.Process.ERROR],
                        self.stats['workers'][name][worker.Process.BATCHES],
                        self.stats['workers'][name][worker.Process.PENDING])

    def new_process(self, name):
        """Create a new worker instances

        :param str name: The name of the worker
        :return tuple: (str, process.Process)

        """
        LOGGER.debug('Creating a new %s process', name)
        kwargs = {
            'config': {
                'statsd': self.config.application.get('statsd', {}),
                'worker': self.workers[name].config
            },
            'daemon': False,
            'stats_queue': self.stats_queue,
            'worker_name': name,
        }
        return worker.Process(name=name, kwargs=kwargs)

    def on_abort(self, _signum, _unused_frame):
        LOGGER.debug('Abort signal received from child')
        time.sleep(1)
        if not self.active_processes:
            LOGGER.info('Stopping with no active processes and child error')
            self.set_state(self.STATE_STOPPED)

    def on_timer(self, _signum, _unused_frame):
        if self.is_shutting_down:
            LOGGER.debug('Polling timer fired while shutting down')
            return
        if not self.polled:
            self.poll()
            self.polled = True
            self.set_timer(5)  # Wait 5 seconds for results
        else:
            self.polled = False
            self.poll_results_check()
            self.set_timer(self.poll_interval)  # Wait poll interval duration
            self.log_stats()

    def poll(self):
        """Start the poll process by invoking the get_stats method of the
        workers. If we hit this after another interval without fully
        processing, note it with a warning.

        """
        self.set_state(self.STATE_ACTIVE)

        # If we don't have any active workers, spawn new ones
        if not self.total_process_count:
            LOGGER.debug('Did not find any active workers in poll')
            return self.check_processes()

        # Start our data collection dict
        self.poll_data = {'timestamp': time.time(), 'processes': list()}

        # Iterate through all of the workers
        for process in self.active_processes:
            LOGGER.debug('Checking runtime state of %s', process.name)
            if process == multiprocessing.current_process():
                LOGGER.debug('Matched current process in active_processes')
                continue

            # Send the profile signal
            os.kill(int(process.pid), signal.SIGPROF)
            self.poll_data['processes'].append(process.name)

        # Check if we need to start more processes
        self.check_processes()

    @property
    def poll_duration_exceeded(self):
        """Return true if the poll time has been exceeded.

        :rtype: bool

        """
        return (time.time() -
                self.poll_data['timestamp']) >= self.poll_interval

    def poll_results_check(self):
        """Check the polling results by checking to see if the stats queue is
        empty. If it is not, try and collect stats. If it is set a timer to
        call ourselves in _POLL_RESULTS_INTERVAL.

        """
        LOGGER.debug('Checking for poll results')
        while True:
            try:
                stats = self.stats_queue.get(False)
            except queue.Empty:
                break
            try:
                self.poll_data['processes'].remove(stats['name'])
            except ValueError:
                pass
            self.collect_results(stats)

        if self.poll_data['processes']:
            LOGGER.warning('Did not receive results from %r',
                           self.poll_data['processes'])

    def remove_worker_process(self, name):
        """Remove all details for the specified worker and process name.

        :param str name: The worker name

        """
        if self.workers[name].process.is_alive():
            try:
                self.workers[name].process.terminate()
            except OSError:
                pass
        self.workers[name].process = None

    def run(self):
        """When the worker is ready to start running, kick off all of our
        worker workers and then loop while we process messages.

        """
        self.set_state(self.STATE_ACTIVE)
        self.setup_workers()

        # Set the SIGABRT handler for child creation errors
        signal.signal(signal.SIGABRT, self.on_abort)

        # Set the SIGALRM handler for poll interval
        signal.signal(signal.SIGALRM, self.on_timer)

        # Kick off the poll timer
        signal.setitimer(signal.ITIMER_REAL, self.poll_interval, 0)

        # Loop for the lifetime of the app, pausing for a signal to pop up
        while self.is_running:
            if not self.is_sleeping:
                self.set_state(self.STATE_SLEEPING)
            signal.pause()

    @staticmethod
    def set_process_name():
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fashion.

        """
        proc = multiprocessing.current_process()
        for offset in range(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                proc.name = name.split('.')[0]
                break

    def set_timer(self, duration):
        """Setup the next alarm to fire and then wait for it to fire.

        :param int duration: How long to sleep

        """
        # Make sure that the application is not shutting down before sleeping
        if self.is_shutting_down:
            LOGGER.debug('Not sleeping, application is trying to shutdown')
            return

        # Set the signal timer
        signal.setitimer(signal.ITIMER_REAL, duration, 0)

    def start_process(self, name):
        """Start a new worker process for the given worker & connection name

        :param str name: The worker name

        """
        process = self.new_process(name)
        LOGGER.info('Spawning %s process', name)

        # Append the process to the worker process list
        self.workers[name].process = process

        # Start the process
        process.start()

    def setup_workers(self):
        """Iterate through each worker in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self.config.application.workers.keys():
            self.start_process(name)

    def stop_processes(self):
        """Iterate through all of the worker processes shutting them down."""
        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.info('Stopping worker processes')

        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.setitimer(signal.ITIMER_REAL, 0, 0)

        # Send SIGABRT
        LOGGER.info('Sending SIGABRT to active children')
        for proc in multiprocessing.active_children():
            if int(proc.pid) != os.getpid():
                os.kill(int(proc.pid), signal.SIGABRT)

        # Wait for them to finish up to MAX_SHUTDOWN_WAIT
        iterations = 0
        processes = self.total_process_count
        while processes:
            LOGGER.debug('Waiting on %i process(es) to shutdown', processes)
            try:
                time.sleep(0.5)
            except KeyboardInterrupt:
                LOGGER.info('Caught CTRL-C, Killing Children')
                self.kill_processes()
                self.set_state(self.STATE_STOPPED)
                return

            iterations += 1
            if iterations == self.MAX_SHUTDOWN_WAIT:
                self.kill_processes()
                break
            processes = self.total_process_count

        LOGGER.debug('All worker processes stopped')
        self.set_state(self.STATE_STOPPED)

    @property
    def total_process_count(self):
        """Returns the active worker process count

        :rtype: int

        """
        return len(self.active_processes)

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
    DEFAULT_QTY = 1

    def __init__(self, config, stats_queue):
        self.config = config
        self.desired_quantity = config.get('processes', self.DEFAULT_QTY)
        self.last_process = 0
        self.processes = {}
        self.stats_queue = stats_queue


class MasterControlProgram(state.State):
    """Master Control Program keeps track of and manages worker processes."""

    MAX_SHUTDOWN_WAIT = 10
    POLL_INTERVAL = 60.0
    POLL_RESULTS_INTERVAL = 3.0
    SHUTDOWN_WAIT = 1

    def __init__(self, config):
        """Initialize the Master Control Program

        :param helper.config.Config config: Mikkoo Configuration

        """
        self.set_process_name()
        LOGGER.info('mikkoo v%s initializing', __version__)
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
            for process in self.workers[name].processes.keys():
                if self.workers[name].processes[process].pid == os.getpid():
                    continue
                try:
                    proc = psutil.Process(
                        self.workers[name].processes[process].pid)
                except psutil.NoSuchProcess:
                    dead_processes.append((name, process))
                    continue

                if self.is_dead(proc):
                    dead_processes.append((name, process))
                else:
                    active_processes.append(
                        self.workers[name].processes[process])

        if dead_processes:
            LOGGER.debug('Removing %i dead process(es)', len(dead_processes))
            for process in dead_processes:
                self.remove_worker_process(*process)
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
            worker_stats[name]['processes'] = self.process_count(name)
            for process in data[name].keys():
                for key in stats:
                    value = data[name][process]['counts'].get(key, 0)
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

    def check_process_counts(self):
        """Check for the minimum worker process levels and start up new
        processes needed.

        """
        LOGGER.debug('Checking minimum worker process levels')
        for name in self.workers.keys():
            processes_needed = self.process_spawn_qty(name)
            LOGGER.debug('Need to spawn %i processes for %s',
                         processes_needed, name)
            if processes_needed:
                self.start_processes(name, processes_needed)

    def collect_results(self, data_values):
        """Receive the data from the workers polled and process it.

        :param dict data_values: The poll data returned from the worker
        :type data_values: dict

        """
        self.last_poll_results['timestamp'] = self.poll_data['timestamp']

        # Get the name and worker name and remove it from what is reported
        worker_name = data_values['worker_name']
        del data_values['worker_name']
        process_name = data_values['name']
        del data_values['name']

        # Add it to our last poll global data
        if worker_name not in self.last_poll_results:
            self.last_poll_results[worker_name] = dict()
        self.last_poll_results[worker_name][process_name] = data_values

        # Calculate the stats
        self.stats = self.calculate_stats(self.last_poll_results)

    @staticmethod
    def worker_keyword(counts):
        """Return worker or workers depending on the process count.

        :param dict counts: The count dictionary to use process count
        :rtype: str

        """
        return 'worker' if counts['processes'] == 1 else 'workers'

    @staticmethod
    def worker_stats_counter():
        """Return a new worker stats counter instance.

        :rtype: dict

        """
        return {
            worker.Process.ERROR: 0,
            worker.Process.PROCESSED: 0
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

        if self.stats['counts']['processes'] > 1:
            LOGGER.info('%i workers processed %i messages with %i errors',
                        self.stats['counts']['processes'],
                        self.stats['counts']['processed'],
                        self.stats['counts']['failed'])

        for key in self.stats['workers'].keys():
            LOGGER.info('%i %s %s processed %i messages with %i errors',
                        self.stats['workers'][key]['processes'], key,
                        self.worker_keyword(self.stats['workers'][key]),
                        self.stats['workers'][key]['processed'],
                        self.stats['workers'][key]['failed'])

    def new_process(self, name):
        """Create a new worker instances

        :param str name: The name of the worker
        :return tuple: (str, process.Process)

        """
        process_name = '%s-%s' % (name, self.new_process_number(name))
        LOGGER.debug('Creating a new process for %s: %s', name, process_name)
        kwargs = {
            'config': {
                'statsd': self.config.application.get('statsd', {}),
                'worker': self.workers[name].config
            },
            'daemon': False,
            'name': process_name,
            'stats_queue': self.stats_queue,
            'worker_name': name,
        }
        return process_name, worker.Process(name=process_name, kwargs=kwargs)

    def new_process_number(self, name):
        """Increment the counter for the process id number for a given worker
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self.workers[name].last_process += 1
        return self.workers[name].last_process

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
            return self.check_process_counts()

        # Start our data collection dict
        self.poll_data = {'timestamp': time.time(), 'processes': list()}

        # Iterate through all of the workers
        for proc in self.active_processes:
            LOGGER.debug('Checking runtime state of %s', proc.name)
            if proc == multiprocessing.current_process():
                LOGGER.debug('Matched current process in active_processes')
                continue

            # Send the profile signal
            os.kill(int(proc.pid), signal.SIGPROF)
            self.poll_data['processes'].append(proc.name)

        # Check if we need to start more processes
        self.check_process_counts()

    @property
    def poll_duration_exceeded(self):
        """Return true if the poll time has been exceeded.

        :rtype: bool

        """
        return (time.time() - self.poll_data['timestamp']) >= self.poll_interval

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

    def process_count(self, name):
        """Return the process count for the given worker name and connection.

        :param str name: The worker name
        :rtype: int

        """
        return len(self.workers[name].processes)

    def process_spawn_qty(self, name):
        """Return the number of processes to spawn for the given worker name
        and connection.

        :param str name: The worker name
        :rtype: int

        """
        return self.workers[name].desired_quantity - self.process_count(name)

    def remove_worker_process(self, name, process):
        """Remove all details for the specified worker and process name.

        :param str name: The worker name
        :param str process: The process to remove

        """
        mcp_pid = os.getpid()
        if process in self.workers[name].processes:
            if self.workers[name].processes[process].is_alive():
                if self.workers[name].processes[process].pid != mcp_pid:
                    try:
                        self.workers[name].processes[process].terminate()
                    except OSError:
                        pass
                else:
                    LOGGER.debug('Child has my pid? %r, %r', mcp_pid,
                                 self.workers[name].processes[process].pid)
            del self.workers[name].processes[process]

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

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

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
        process_name, process = self.new_process(name)
        LOGGER.info('Spawning %s process for %s', process_name, name)

        # Append the process to the worker process list
        self.workers[name].processes[process_name] = process

        # Start the process
        process.start()

    def start_processes(self, name, quantity):
        """Start the specified quantity of worker processes for the given
        worker.

        :param str name: The worker name
        :param int quantity: The quantity of processes to start

        """
        [self.start_process(name) for _i in range(0, quantity)]

    def setup_workers(self):
        """Iterate through each worker in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self.config.application.workers.keys():
            self.start_processes(name, self.workers[name].desired_quantity)

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
            LOGGER.info('Waiting on %i active processes to shut down',
                        processes)
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
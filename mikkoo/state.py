"""
Base State Tracking Class

"""
import logging
import time
import typing

LOGGER = logging.getLogger(__name__)


class State:
    """Class that is to be extended by MCP and process for maintaining the
    internal state of the application.

    """
    # State constants
    STATE_INITIALIZING = 0x01
    STATE_CONNECTING = 0x02
    STATE_IDLE = 0x03
    STATE_ACTIVE = 0x04
    STATE_SLEEPING = 0x05
    STATE_STOP_REQUESTED = 0x06
    STATE_SHUTTING_DOWN = 0x07
    STATE_STOPPED = 0x08
    STATE_RECONNECTING = 0x09
    STATE_BLOCKED = 0x10
    STATE_CLOSED = 0x11

    # For reverse lookup
    STATES = {
        0x01: 'Initializing',
        0x02: 'Connecting',
        0x03: 'Idle',
        0x04: 'Active',
        0x05: 'Sleeping',
        0x06: 'Stop Requested',
        0x07: 'Shutting down',
        0x08: 'Stopped',
        0x09: 'Reconnecting',
        0x10: 'Blocked',
        0x11: 'Closed'
    }

    def __init__(self):
        """Initialize the state of the object"""
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()

    def set_state(self, new_state: int) -> typing.NoReturn:
        """Assign the specified state to this consumer object.

        :raises: ValueError

        """
        if new_state not in self.STATES:
            raise ValueError(f'Invalid state value: {new_state}')
        LOGGER.debug('State changing from %s to %s', self.STATES[self.state],
                     self.STATES[new_state])
        self.state = new_state
        self.state_start = time.time()

    @property
    def is_blocked(self) -> bool:
        """Return True if the process is blocked by RabbitMQ"""
        return self.state == self.STATE_BLOCKED

    @property
    def is_connecting(self) -> bool:
        """Returns True if the process is currently connecting."""
        return self.state == self.STATE_CONNECTING

    @property
    def is_idle(self) -> bool:
        """Returns a bool specifying if the process is currently idle."""
        return self.state == self.STATE_IDLE

    @property
    def is_reconnecting(self) -> bool:
        """Returns True if the process is currently reconnecting"""
        return self.state == self.STATE_RECONNECTING

    @property
    def is_running(self) -> bool:
        """Indicates if the process is in a running state"""
        return self.state in [self.STATE_IDLE, self.STATE_ACTIVE,
                              self.STATE_SLEEPING]

    @property
    def is_shutting_down(self) -> bool:
        """Designates if the process is shutting down."""
        return self.state == self.STATE_SHUTTING_DOWN

    @property
    def is_sleeping(self) -> bool:
        """Returns a bool determining if the process is sleeping"""
        return self.state == self.STATE_SLEEPING

    @property
    def is_stopped(self) -> bool:
        """Returns a bool determining if the process is stopped or stopping"""
        return self.state == self.STATE_STOPPED

    @property
    def is_waiting_to_shutdown(self) -> bool:
        """Designates if the process is waiting to start shutdown"""
        return self.state == self.STATE_STOP_REQUESTED

    @property
    def state_description(self) -> str:
        """Return the string description of our running state."""
        return self.STATES[self.state]

    @property
    def time_in_state(self) -> float:
        """Return the time that has been spent in the current state."""
        return time.time() - self.state_start

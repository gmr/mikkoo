"""
Mikkoo
======
PgQ -> RabbitMQ Relay

"""
import logging
import signal

import helper
from helper import parser

from mikkoo import mcp

DESCRIPTION = 'Mikkoo is a PgQ to RabbitMQ Relay'
LOGGER = logging.getLogger(__name__)


class Controller(helper.Controller):
    """The Mikkoo controller application that invokes the MCP and handles all
    of the OS level concerns.

    """
    def __init__(self, *args, **kwargs):
        super(Controller, self).__init__(*args, **kwargs)
        self._mcp = None

    def _master_control_program(self):
        """Return an instance of the MasterControlProgram.

        :rtype: rejected.mcp.MasterControlProgram

        """
        return mcp.MasterControlProgram(self.config)

    def stop(self):
        """Shutdown the MCP and child processes cleanly"""
        LOGGER.info('Shutting down controller')
        self.set_state(self.STATE_STOP_REQUESTED)

        # Clear out the timer
        signal.setitimer(signal.ITIMER_PROF, 0, 0)

        self._mcp.stop_processes()

        if self._mcp.is_running:
            LOGGER.info('Waiting up to 3 seconds for MCP to shut things down')
            signal.setitimer(signal.ITIMER_REAL, 3, 0)
            signal.pause()

        # Force MCP to stop
        if self._mcp.is_running:
            LOGGER.warning('MCP is taking too long, requesting process kills')
            self._mcp.stop_processes()
            del self._mcp

        # Change our state
        self._stopped()
        LOGGER.info('Shutdown complete')

    def run(self):
        """Run the rejected Application"""
        self.setup()
        self._mcp = self._master_control_program()
        try:
            self._mcp.run()
        except KeyboardInterrupt:
            LOGGER.info('Caught CTRL-C, shutting down')
        if self.is_running:
            self.stop()


def main():
    """Called when invoking the command line script."""
    parser.description(DESCRIPTION)
    helper.start(Controller)

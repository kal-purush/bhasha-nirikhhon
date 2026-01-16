""" Helper functions for the project.

    Includes: dbus_connection_callback, dbus_disconnection_callback
"""

import logging

logger = logging.getLogger(__name__)


def dbus_connection_callback():
    logger.debug("Connection established")


def dbus_disconnection_callback():
    logger.debug("Connection lost")
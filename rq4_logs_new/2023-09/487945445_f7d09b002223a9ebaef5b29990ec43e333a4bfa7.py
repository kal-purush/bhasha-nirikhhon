# -*- coding: utf-8 -*-
#
#  lock_wrapper.py
#
#  This file is part of book_ease.
#
#  Copyright 2021 mark cole <mark@capstonedistribution.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#

"""
Wrapper for threading.Lock objects

Allows using Lock objects with weakrefs.
"""

import threading


class Lock:
    """
    Wrapper for threading.Lock objects
    Exists for the benefit of using Signal callbacks which makes use of weakrefs/weakmethods.
    """

    def __init__(self):
        self.lock = threading.Lock()

    def acquire(self, blocking=True, timeout=- 1) -> bool:
        """
        Acquire a lock, blocking or non-blocking.
        When invoked with the blocking argument set to True (the default), block until the
        lock is unlocked, then set it to locked and return True.

        When invoked with the blocking argument set to False, do not block.
        If a call with blocking set to True would block,
        return False immediately; otherwise, set the lock to locked and return True.

        When invoked with the floating-point timeout argument set to a positive value,
        block for at most the number of seconds specified by timeout and as long as the lock
        cannot be acquired. A timeout argument of -1 specifies an unbounded wait. It is
        forbidden to specify a timeout when blocking is False.

        The return value is True if the lock is acquired successfully,
        False if not (for example if the timeout expired).

        Changed in version 3.2: The timeout parameter is new.

        Changed in version 3.2: Lock acquisition can now be interrupted by signals on POSIX
        if the underlying threading implementation supports it.
        """
        return self.lock.acquire(blocking, timeout)

    def release(self) -> None:
        """
        Release a lock. This can be called from any thread, not only the thread which has acquired
        the lock.

        When the lock is locked, reset it to unlocked, and return. If any other threads are blocked
        waiting for the lock to become unlocked, allow exactly one of them to proceed.

        When invoked on an unlocked lock, a RuntimeError is raised.

        There is no return value.
        """
        self.lock.release()

    def locked(self) -> bool:
        """
        Return True if the lock is acquired.
        """
        return self.lock.locked()
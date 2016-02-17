#!/usr/bin/env python
# coding: utf8
#
# Copyright 2016 hdd
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Connection Manage Module
"""

import socket
import os
import threading
from itertools import chain
from beanstalk.exceptions import SocketError


class Connection(object):
    def __init__(self, host='localhost', port=11300, connect_timeout=socket.getdefaulttimeout()):
        self._connect_timeout = connect_timeout
        self.host = host
        self.port = port
        self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        """Connect to beanstalkd server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self._connect_timeout)
        SocketError.wrap(self.socket.connect, (self.host, self.port))
        self.socket.settimeout(None)
        self.socket_file = self.socket.makefile('rb')

    def close(self):
        """Close connection to server."""
        try:
            self.socket.sendall('quit\r\n')
        except socket.error:
            pass
        try:
            self.socket.close()
        except socket.error:
            pass

    def reconnect(self):
        """Re-connect to server."""
        self.close()
        self.connect()


class ConnectionPool(object):

    """
    Generic connection pool, shameless forked from
    `https://github.com/andymccurdy/redis-py/blob/master/redis/connection.py`
    """

    def __init__(self, connection_class=Connection, max_connections=None, **connection_kwargs):
        max_connections = max_connections or 2 ** 10

        if not isinstance(max_connections, (int, long)) or max_connections < 0:
            raise ValueError('''"max_connections" must be a positive integer''')

        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections

        self.reset()

    def reset(self):
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self._check_lock = threading.Lock()

    def disconnect(self):
        """Disconnects all connections in the pool"""
        all_conns = chain(self._available_connections, self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()

    def _check_pid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                self.disconnect()
                self.reset()

    def get_connection(self):
        self._check_pid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()

        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        """Create a new connection"""
        if self._created_connections >= self.max_connections:
            raise SocketError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    def release(self, connection):
        """Releases the connection back to the pool"""
        self._check_pid()
        if connection.pid != self.pid:
            return
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

#
# Copyright 2015 iXsystems, Inc.
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#####################################################################

import os
import socket
import enum
import logging
import threading


KEEPALIVE_INTERVAL = 30


class ChannelType(enum.Enum):
    CONTROL = 'control'
    DATA = 'data'


class Channel(object):
    def __init__(self, connection, id, type=ChannelType.DATA):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._id = id
        self._type = type
        self._connection = connection
        self._closed = False
        self._master, self._slave = socket.socketpair()
        self._send_thread = threading.Thread(
            target=self._worker,
            name='channel{0} send thread'.format(self.id),
            daemon=True
        )
        self._send_thread.start()

    @property
    def connection(self):
        return self._connection

    @property
    def type(self):
        return self._type

    @property
    def id(self):
        return self._id

    def on_data(self, data):
        if data == b'':
            self._logger.debug('Channel {0} closed'.format(self._id))
            self._slave.shutdown(socket.SHUT_WR)
            return

        self._slave.send(data)

    def recv(self, nbytes):
        return self._master.recv(nbytes)

    def recv_into(self, buffer, nbytes=None):
        return self._master.recv_into(buffer, nbytes)

    def send(self, buffer):
        self._master.send(buffer)

    def close(self):
        self._master.shutdown(socket.SHUT_RDWR)

    def _worker(self):
        while True:
            data = os.read(self._slave.fileno(), 1024)
            self._connection.send(self.id, data)
            if data == b'':
                return

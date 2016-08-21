#
# Copyright 2016 iXsystems, Inc.
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

import errno
import logging
import socket
import threading
import struct
import urllib.parse
from msock.channel import Channel
from msock.utils import recvall


HEADER_MAGIC = 0x5a5a5a5a
HEADER_FORMAT = 'III'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class Connection(object):
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.on_channel_created = lambda chan: None
        self.on_channel_destroyed = lambda chan: None
        self.on_closed = lambda: None
        self.channel_factory = lambda id: Channel(self, id)
        self._channels = {}
        self._recv_thread = None
        self._address = None
        self._socket = None
        self._closed = False
        self._lock = threading.RLock()

    @property
    def channels(self):
        return self._channels

    @property
    def remote_address(self):
        return self._address

    def create_channel(self, id=None):
        if id is None:
            id = max(self.channels.keys()) + 1

        chan = self.channel_factory(id)
        self._channels[id] = chan
        self.on_channel_created(chan)
        logging.debug('Created channel {0}'.format(id))
        return chan

    def destroy_channel(self, id):
        logging.debug('Destroying channel {0}'.format(id))
        del self._channels[id]

    def open(self):
        self._closed = False
        self._recv_thread = threading.Thread(target=self._recv, daemon=True, name='msock recv thread')
        self._recv_thread.start()

    def send(self, channel_id, data):
        if self._closed:
            return

        header = struct.pack(
            HEADER_FORMAT,
            HEADER_MAGIC,
            channel_id,
            len(data)
        )

        with self._lock:
            try:
                self._socket.sendall(header)
                self._socket.sendall(data)
            except OSError as err:
                if err.errno == errno.EPIPE:
                    return

    def _recv(self):
        while True:
            try:
                data = recvall(self._socket, HEADER_SIZE)
                if data == b'':
                    self._logger.debug('EOF received')
                    self._close()
                    return

                magic, channel_id, length = struct.unpack(HEADER_FORMAT, data)
                if magic != HEADER_MAGIC:
                    self._logger.debug('Wrong magic received ({0:04x})'.format(magic))
                    self._close()
                    return

                data = recvall(self._socket, length)
                if channel_id not in self._channels:
                    # discard the data
                    self._logger.warning('Data from unknown channel {0} received, discarding'.format(channel_id))
                    continue

                chan = self.channels[channel_id]
                chan.on_data(data)
            except OSError as err:
                self._logger.info('Read failed: {0}'.format(err))
                self._close()
                return

    def _close(self):
        self._closed = True
        self._logger.debug('Connection closed')
        for i in list(self.channels.values()):
            if not i.closed:
                i.close()

        self._channels.clear()

        with self._lock:
            self._socket.close()
            self.on_closed()

    def close(self):
        if self._closed:
            return

        self._socket.shutdown(socket.SHUT_RDWR)
        self._recv_thread.join()


class Client(Connection):
    def __init__(self):
        super(Client, self).__init__()
        self._uri = None

    def connect(self, uri):
        parsed = urllib.parse.urlparse(uri, 'tcp')
        if parsed.scheme == 'tcp':
            af = socket.AF_INET
            address = (parsed.hostname, parsed.port)
        elif parsed.scheme == 'unix':
            af = socket.AF_UNIX
            address = parsed.netloc
        else:
            raise RuntimeError('Unsupported scheme {0}'.format(parsed.scheme))

        self._socket = socket.socket(af, socket.SOCK_STREAM)
        self._socket.connect(address)
        print('main socket fd: {0}'.format(self._socket.fileno()))
        self.open()

    def disconnect(self):
        self.close()

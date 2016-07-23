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

import logging
import time
import enum
import json
import uuid
import queue
import threading


KEEPALIVE_INTERVAL = 30


class ChannelType(enum.Enum):
    CONTROL = 'control'
    DATA = 'data'


class RingBuffer(object):
    def __init__(self):
        self._buffers = queue.Queue()

    def push(self, buf):
        self._buffers.put(buf)

    def pop(self, nbytes=0):
        if nbytes == 0:
            return self._buffers.get()

        done = 0
        left = nbytes
        result = bytearray()

        while done < nbytes:
            buf = self._buffers.queue[0]
            if left >= len(buf):
                self._buffers.get()
                result.extend(buf)
                done += len(buf)
            else:
                result.extend(buf[left:])
                del buf[left:]
                done += left

        return result


class Channel(object):
    def __init__(self, connection, id, metadata=None, type=ChannelType.DATA):
        self._id = id
        self._type = type
        self._metadata = metadata
        self._connection = connection
        self._closed = False
        self.buffer = RingBuffer()

    @property
    def connection(self):
        return self._connection

    @property
    def type(self):
        return self._type

    @property
    def id(self):
        return self._id

    @property
    def metadata(self):
        return self._metadata

    def on_data(self, data):
        self.buffer.push(data)

    def read(self, nbytes):
        if self._closed:
            return b''

    def readmsg(self):
        if self._closed:
            return b''

        return self.buffer.pop()

    def readinto(self, buffer, nbytes=0):
        if self._closed:
            return 0

    def write(self, buf):
        self.connection.send(self.id, buf)

    def close(self):
        self.connection.destroy_channel(self.id)


class ControlChannel(Channel):
    class Call(object):
        def __init__(self, command, args, id=None):
            self.command = command
            self.args = args
            self.result = None
            self.id = str(id or uuid.uuid4())
            self.cv = threading.Event()

    class CallException(RuntimeError):
        pass

    def __init__(self, connection, id, client=False):
        super(ControlChannel, self).__init__(connection, id, type=ChannelType.CONTROL)
        self._calls = {}
        self._logger = logging.getLogger(self.__class__.__name__)
        self._worker = threading.Thread(target=self._worker, name='msock control thread', daemon=True)
        self._worker.start()
        if client:
            self._keepalive_worker = threading.Thread(
                target=self._keepalive,
                name='msock keepalive thread',
                daemon=True)
            self._keepalive_worker.start()

        self._logger.debug('Created, id={0}'.format(id))

    def open_channel(self, id, metadata, type=ChannelType.DATA):
        self._call('open_channel', id, metadata, str(type))

    def destroy_channel(self, id):
        self._call('destroy_channel', id)

    def _worker(self):
        while True:
            msg = self.readmsg()
            if msg == b'':
                return

            try:
                msg = json.loads(msg.decode('utf-8'))
            except ValueError:
                return

            id = msg['id']
            name = msg['name']

            if msg['type'] == 'command':
                func = getattr(self, '_cmd_{0}'.format(name), None)
                if func and callable(func):
                    try:
                        resp = func(*msg['args'])
                        self._send(id, 'response', name, resp)
                    except:
                        pass
                else:
                    pass

            if msg['type'] == 'response':
                if id in self._calls:
                    self._calls[id].cv.set()

    def _send(self, id, type, name, args):
        payload = {
            'id': id,
            'type': type,
            'name': name,
            'args': args
        }

        self.write(json.dumps(payload).encode('utf-8'))

    def _call(self, command, *args, timeout=None):
        call = self.Call(command, args)
        self._calls[call.id] = call
        self._send(call.id, 'command', call.command, call.args)
        call.cv.wait(timeout)
        ret = call.result
        self._calls.pop(call.id, None)
        return ret

    def _cmd_destroy_channel(self, id):
        self._logger.debug('Received close channel request: id={0}'.format(id))

    def _cmd_open_channel(self, id, metadata, type):
        self._logger.debug('Received open channel request: id={0}, metadata={1}, type={2}'.format(
            id,
            metadata,
            type
        ))

        chan = self.connection.channel_factory(id, metadata)
        self.connection._channels[id] = chan
        self.connection.on_channel_created(chan)

    def _cmd_ping(self):
        self._logger.debug('Received keepalive message')
        return 'pong'

    def _keepalive(self):
        while True:
            time.sleep(KEEPALIVE_INTERVAL)
            self._logger.debug('Sending keepalive message')
            if self._call('ping', 30) == 'pong':
                pass
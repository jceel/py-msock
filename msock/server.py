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

import logging
import socket
import threading
import urllib.parse
from msock.client import Connection


class Server(object):
    def __init__(self):
        self.on_connection = lambda conn: None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._uri = None
        self._socket = None
        self._connections = []
        self._lock = threading.RLock()

    def open(self, uri):
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
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(address)

    def run(self):
        self._logger.debug('Listening for client connections on {0}'.format(self._uri))
        self._socket.listen()
        while True:
            sock, addr = self._socket.accept()
            self._logger.debug('Accepted client from {0}'.format(addr))
            conn = Connection()
            conn._socket = sock
            conn._address = addr
            conn.open()
            self.on_connection(conn)

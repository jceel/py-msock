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

from threading import Condition


class RingBuffer(object):
    def __init__(self, size):
        self.data = bytearray(size)
        self.view = memoryview(self.data)
        self.size = size
        self.head = 0
        self.tail = 0
        self.closed = False
        self.cv = Condition()

    @property
    def empty(self):
        return self.head == self.tail

    @property
    def full(self):
        return self.head == (self.tail + 1) % self.size

    @property
    def used_space(self):
        if self.empty:
            return 0

        if self.tail > self.head:
            return self.tail - self.head

        if self.head > self.tail:
            return (self.size - self.head) + self.tail

    @property
    def avail_space(self):
        return self.size - self.used_space - 1

    def write(self, data):
        with self.cv:
            if self.full:
                self.cv.wait_for(lambda: not self.full or self.closed)
                if self.closed:
                    return 0

            towrite = min(len(data), self.avail_space)

            if self.tail >= self.head:
                first = min(towrite, self.size - self.tail)
                rest = towrite - first

                if first:
                    self.view[self.tail:self.tail+first] = data[:first]
                    self.tail = (self.tail + first) % self.size

                if rest:
                    self.view[:rest] = data[first:first+rest]
                    self.tail = (self.tail + rest) % self.size

                self.cv.notify_all()
                return towrite

            if self.head > self.tail:
                self.view[self.tail:self.head] = data[:towrite]
                self.tail = (self.tail + towrite) % self.size
                self.cv.notify_all()
                return towrite

    def read(self, count):
        with self.cv:
            if self.empty:
                if self.closed:
                    return b''

                self.cv.wait_for(lambda: not self.empty or self.closed)

            toread = min(count, self.used_space)

            if self.tail >= self.head:
                toread = min(toread, self.tail - self.head)
                result = bytes(self.view[self.head:self.head+toread])
                self.head = (self.head + toread) % self.size
                self.cv.notify_all()
                return result

            if self.head > self.tail:
                first = min(toread, self.size - self.head)
                rest = toread - first

                if first:
                    result = bytes(self.view[self.head:self.head+first])
                    self.head = (self.head + first) % self.size

                if rest:
                    result += bytes(self.view[:rest])
                    self.head = (self.head + rest) % self.size

                self.cv.notify_all()
                return result

    def close(self):
        with self.cv:
            self.closed = True
            self.cv.notify_all()

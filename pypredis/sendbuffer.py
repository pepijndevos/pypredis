from threading import Condition
from cStringIO import StringIO

class SendBuffer(object):

    def __init__(self, max_size):
        self.size = 0
        self.frontbuffer = ''
        self.full = Condition()
        self.backbuffer = StringIO()
        self.max_size = max_size

    def __len__(self):
        return len(self.frontbuffer) + self.size

    def write(self, data):
        dlen = len(data)
        with self.full:
            while self.size + dlen > self.max_size and dlen < self.max_size:
                # if a single write is bigger than the buffer, swallow hard.
                # No amount of waitng will make it fit.
                self.full.wait()
            self.backbuffer.write(data)
            self.size += dlen

    def to_sock(self, sock):
        if not self.frontbuffer:
            with self.full:
                buf, self.backbuffer = self.backbuffer, StringIO()
                self.size = 0
                self.full.notify_all()
            self.frontbuffer = buf.getvalue()

        written = sock.send(self.frontbuffer)
        if written < len(self.frontbuffer):
            print written, '/', len(self.frontbuffer)
        self.frontbuffer = self.frontbuffer[written:]

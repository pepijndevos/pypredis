from threading import Condition
from Queue import Queue, Empty
from cStringIO import StringIO

class SendBuffer(object):

    def __init__(self, max_size):
        self.mark = 0
        self.current = ''
        self.buf = Queue(max_size)

    def __len__(self):
        return self.buf.qsize() + int(bool(self.current))

    def write(self, data):
        self.buf.put(data)

    def to_sock(self, sock):
        if self.mark >= len(self.current):
            io = StringIO()
            while True:
                try:
                    io.write(self.buf.get_nowait())
                except Empty:
                    break

            self.current = io.getvalue()
            io.close()
            self.mark = 0

        self.mark += sock.send(self.current[self.mark:])

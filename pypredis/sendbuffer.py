from threading import Condition
from Queue import Queue, Empty
from cStringIO import StringIO

class SendBuffer(object):

    def __init__(self, max_size):
        self.count = 0
        self.mark = 0
        self.current = ''
        self.buf = Queue(max_size)

    def __len__(self):
        return self.buf.qsize() + self.count

    def write(self, data):
        self.buf.put(data)

    def to_sock(self, sock):
        if self.mark >= len(self.current):
            self.count = 0
            io = StringIO()
            while True:
                try:
                    io.write(self.buf.get_nowait())
                    self.count += 1
                except Empty:
                    break

            self.current = io.getvalue()
            io.close()
            self.mark = 0

        self.mark += sock.send(self.current[self.mark:])

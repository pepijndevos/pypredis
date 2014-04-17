from threading import Condition
from Queue import Queue, Empty

class SendBuffer(object):

    def __init__(self, max_size):
        self.mark = 0
        self.current = ''
        self.buf = Queue(max_size)

    def write(self, data):
        self.buf.put(data)

    def to_sock(self, sock):
        if self.mark >= len(self.current):
            try:
                self.current = self.buf.get_nowait()
                self.mark = 0
            except Empty:
                return
        self.mark += sock.send(self.current[self.mark:])

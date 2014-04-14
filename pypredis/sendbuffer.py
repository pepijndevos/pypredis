from array import array
from threading import Condition

class SendBuffer(object):

    def __init__(self, max_size):
        self.mark = 0
        self.buf = array('c')
        self.max_size = max_size
        self.full = Condition()

    def __len__(self):
        return len(self.buf) - self.mark


    def write(self, data):
        with self.full:
            while len(self) + len(data) > self.max_size:
                self.full.wait()
            self.buf.fromstring(data)

    def peek(self):
        return memoryview(self.buf)[self.mark:]

    def written(self, n):
        with self.full:
            self.mark += n
            self.full.notify_all()

            if self.mark >= len(self.buf):
                self.mark = 0
                self.buf = array('c')

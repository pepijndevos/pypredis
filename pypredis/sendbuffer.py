from array import array
from threading import Condition

class SendBuffer(object):

    def __init__(self, max_size):
        self.mark = 0
        self.buf = array('c')
        self.max_size = max_size
        self.full = Condition()

    def __len__(self):
        with self.full:
            return len(self.buf) - self.mark


    def write(self, data):
        with self.full:
            while len(self) >= self.max_size:
                # wait until data is written
                self.full.wait()

            self.buf.fromstring(data)

    def peek(self):
        with self.full:
            return buffer(self.buf, self.mark)

    def written(self, n):
        with self.full:
            self.mark += n
            self.full.notify_all()

            if self.mark >= len(self.buf):
                self.mark = 0
                self.buf = array('c')
            elif self.mark >= self.max_size:
                self.buf = self.buf[self.mark:]
                self.mark = 0

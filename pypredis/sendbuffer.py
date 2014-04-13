from array import array

class SendBuffer(object):

    def __init__(self):
        self.mark = 0
        self.buf = array('c')

    def write(self, data):
        self.buf.fromstring(data)

    def peek(self):
        return memoryview(self.buf)[self.mark:]

    def written(self, n):
        self.mark += n
        if self.mark >= len(self.buf):
            self.mark = 0
            self.buf = array('c')

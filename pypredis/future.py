import threading

class TimeoutError(Exception):
    pass

class Future(object):
    "Backport of concurrent.futures.Future"

    def __init__(self):
        self.event = threading.Event()
        self.callbacks = []
        self.lock = threading.Lock()
        self._result = None
        self._exception = None

    def done(self):
        return self.event.is_set()

    def result(self, timeout=None):
        if self.event.wait(timeout):
            if self._exception:
                raise self._exception
            else:
                return self._result
        else:
            raise TimeoutError()

    def exception(self, timeout=None):
        if self.event.wait(timeout):
            return self._exception
        else:
            raise TimeoutError()

    def add_done_callback(self, cb):
        with self.lock:
            if self.event.is_set():
                cb(self)
            else:
                self.callbacks.append(cb)

    def set_result(self, result):
        with self.lock:
            self._result = result
            self.event.set()
            for cb in self.callbacks:
                cb(self)

    def set_exception(self, exception):
        with self.lock:
            self._exception = exception
            self.event.set()
            for cb in self.callbacks:
                cb(self)

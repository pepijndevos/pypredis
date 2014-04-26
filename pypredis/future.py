import threading

class TimeoutError(Exception):
    pass

class Future(object):
    "Backport of concurrent.futures.Future"

    cond = threading.Condition()

    def __init__(self):
        self.callbacks = []
        self.lock = threading.Lock()
        self._result = None
        self._exception = None
        self._done = False

    def done(self):
        return self.done

    def _wait(self, timeout):
        with self.cond:
            while not self._done:
                self.cond.wait(timeout)
        #TODO actual time out

    def result(self, timeout=None):
        self._wait(timeout)
        if self._exception:
            raise self._exception
        else:
            return self._result

    def exception(self, timeout=None):
        self._wait(timeout)
        return self._exception

    def add_done_callback(self, cb):
        with self.lock:
            if self._done:
                cb(self)
            else:
                self.callbacks.append(cb)

    def set_result(self, result):
        with self.lock:
            self._result = result
            self._done = True
            for cb in self.callbacks:
                cb(self)

    def set_exception(self, exception):
        with self.lock:
            self._exception = exception
            self._done = True
            for cb in self.callbacks:
                cb(self)

    @classmethod
    def notify(cls):
        with cls.cond:
            cls.cond.notify_all()

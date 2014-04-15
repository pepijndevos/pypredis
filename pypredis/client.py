from __future__ import absolute_import
from pypredis.future import Future
from pypredis.reader import RedisReader
from pypredis.sendbuffer import SendBuffer
from select import poll, POLLIN, POLLPRI, POLLOUT, POLLERR, POLLHUP, POLLNVAL
from Queue import Queue, Empty
from collections import defaultdict, namedtuple, deque
from threading import Thread
import socket

#debug
from time import sleep

def pack_command(args):
    "Pack a series of arguments into a value Redis command"
    args_output = ''.join([
        ''.join(('$', str(len(k)), '\r\n', k, '\r\n'))
        for k in args])
    output = ''.join(
        ('*', str(len(args)), '\r\n', args_output))
    return output

def drain(q):
    while True:
        try:
            yield q.get_nowait()
        except Empty:
            break

RedisCommand = namedtuple("RedisCommand", ["result", "connection", "command"])

class BaseConnection(object):

    def __init__(self, buf_size=1000000):
        self.sock = None
        self.buf = SendBuffer(buf_size)
        self.resq = deque()
        self.reader = RedisReader()

    @property
    def fd(self):
        return self.sock.fileno()

    @property
    def waiting(self):
        return bool(self.resq)

    def write(self, cmd):
        self.resq.append(cmd.result)
        self.buf.write(cmd.command)

    def pump_out(self):
        data = self.buf.peek()
        if data:
            n = self.sock.send(data)
            self.buf.written(n)
    
    def pump_in(self):
        data = self.sock.recv(4096)
        self.reader.feed(data)
        while True:
            reply = self.reader.get_reply()
            if reply == False:
                break
            res = self.resq.popleft()
            res.set_result(reply)

class UnixConnection(BaseConnection):

    def __init__(self, path, **options):
        BaseConnection.__init__(self, **options)
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.sock.connect(path)


class EventLoop(Thread):
    
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.queue = Queue()
        self.timeout = 0.1
        self.poll = poll()
        self.fd_index = {}

    def send_command(self, conn, *args):
        cmdstr = pack_command(args)
        res = Future()
        cmd = RedisCommand(res, conn, cmdstr)
        conn.write(cmd)
        self.queue.put(cmd)
        return res

    def _register(self, conn):
        if conn.fd not in self.fd_index:
            self.poll.register(conn.fd)
            self.fd_index[conn.fd] = conn

    def _unregister(self, fd):
        self.poll.unregister(fd)
        del self.fd_index[fd]

    def _handle_events(self, events):
        for fd, e in events:
            if e & POLLOUT:
                self._handle_writes(fd)
            if e & (POLLIN | POLLPRI):
                self._handle_reads(fd)

    def _handle_writes(self, fd):
        conn = self.fd_index[fd]
        conn.pump_out()

    def _handle_reads(self, fd):
        conn = self.fd_index[fd]
        conn.pump_in()
        if not conn.waiting:
            self._unregister(fd)

    def run(self):
        while True:
            commands = drain(self.queue)
            for cmd in commands:
                self._register(cmd.connection)

            events = self.poll.poll(self.timeout)
            self._handle_events(events)

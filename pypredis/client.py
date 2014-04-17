from __future__ import absolute_import
from pypredis.future import Future
from pypredis.reader import RedisReader
from pypredis.sendbuffer import SendBuffer
from select import poll, POLLIN, POLLPRI, POLLOUT, POLLERR, POLLHUP, POLLNVAL
from Queue import Queue, Empty
from collections import defaultdict, namedtuple, deque
from threading import Thread
import socket
import os

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

RedisCommand = namedtuple("RedisCommand", ["result", "connection", "command"])

class BaseConnection(object):

    def __init__(self, buf_size=1000000, **connect_params):
        self.sock = None
        self.buf = SendBuffer(buf_size)
        self.resq = deque()
        self.reader = RedisReader()
        self.pid = os.getpid()
        self.connect_params = connect_params
        self.connect(**connect_params)

    @property
    def fd(self):
        return self.sock.fileno()

    @property
    def waiting(self):
        return bool(self.resq)

    def checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.connect(**self.connect_params)

    def disconnect(self):
        self.sock.close()

    def write(self, cmd):
        self.resq.append(cmd.result)
        self.buf.write(cmd.command)

    def pump_out(self):
        try:
            self.buf.to_sock(self.sock)
        except Exception as e:
            res = self.resq.popleft()
            res.set_exception(e)

    
    def pump_in(self):
        try:
            data = self.sock.recv(4096)
            self.reader.feed(data)
            while True:
                reply = self.reader.get_reply()
                if reply == False:
                    break
                res = self.resq.popleft()
                res.set_result(reply)
        except Exception as e:
            res = self.resq.popleft()
            res.set_exception(e)

class UnixConnection(BaseConnection):

    def connect(self, path):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(path)
        self.sock.setblocking(0)


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
        cmd = None
        return res

    def _register(self, conn):
        if conn.fd not in self.fd_index:
            conn.checkpid()
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
            while True:
                try:
                    cmd = self.queue.get_nowait()
                    self._register(cmd.connection)
                    cmd = None
                except Empty:
                    break # only inner

            events = self.poll.poll(self.timeout)
            self._handle_events(events)

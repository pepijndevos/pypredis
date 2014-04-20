from __future__ import absolute_import
from pypredis.future import Future
from pypredis.reader import RedisReader, NoReply
from pypredis.sendbuffer import SendBuffer
from select import poll, POLLIN, POLLPRI, POLLOUT, POLLERR, POLLHUP, POLLNVAL
from Queue import Queue, Empty
from collections import defaultdict, namedtuple, deque
from threading import Thread
import socket
import os
from cStringIO import StringIO

#debug
from time import sleep

def pack_command(args):
    out = StringIO()
    try:
        out.write("*%d\r\n" % len(args))
        for arg in args:
            val = str(arg)
            out.write("$%d\r\n%s\r\n" % (len(val), val))
        return out.getvalue()
    finally:
        out.close()

RedisCommand = namedtuple("RedisCommand", ["result", "connection", "command"])

class BaseConnection(object):

    def __init__(self, **params):
        self.sock = None
        self.buf = SendBuffer(params.get('buf_size', 100))
        self.resq = deque()
        self.reader = RedisReader()
        self.pid = os.getpid()
        self.params = params
        self.connect(**params)

    @property
    def fd(self):
        return self.sock.fileno()

    @property
    def flags(self):
        flags = 0
        if self.buf:
            flags |= POLLOUT
        if self.resq:
            flags |= POLLIN
            flags |= POLLPRI
        return flags

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(**self.params)

    def disconnect(self):
        self.sock.close()

    def write(self, cmd):
        self._checkpid()
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
                try:
                    reply = self.reader.get_reply()
                except NoReply:
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

class TCPConnection(BaseConnection):

    def connect(self, host='localhost', port=6379):
        self.sock = socket.create_connection((host, port))
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
        self.poll.register(conn.fd, conn.flags)
        self.fd_index[conn.fd] = conn

    def _unregister(self, conn):
        self.poll.unregister(conn.fd)
        del self.fd_index[conn.fd]

    def _handle_events(self, events):
        for fd, e in events:
            conn = self.fd_index[fd]
            if e & POLLOUT:
                conn.pump_out()
            if e & (POLLIN | POLLPRI):
                conn.pump_in()

            if conn.flags:
                self._register(conn)
            else:
                self._unregister(conn)

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

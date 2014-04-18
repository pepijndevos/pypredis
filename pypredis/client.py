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

    def __init__(self, buf_size=100, **connect_params):
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
    def flags(self):
        flags = 0
        if self.buf:
            flags |= POLLOUT
        if self.resq:
            flags |= POLLIN
            flags |= POLLPRI
        return flags

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
        conn.checkpid()
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
                self.poll.register(conn.fd, conn.flags)
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

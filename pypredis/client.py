from __future__ import absolute_import
from pypredis.future import Future
from pypredis.reader import RedisReader, NoReply
from pypredis.sendbuffer import SendBuffer
from select import poll, POLLIN, POLLPRI, POLLOUT, POLLERR, POLLHUP, POLLNVAL
from Queue import Queue, Empty
from collections import defaultdict, namedtuple, deque
from threading import Thread, RLock
from cStringIO import StringIO
import socket
import os
import errno

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

class BaseConnection(object):

    def __init__(self, **params):
        self.connect(**params)
        bufsize = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        self.buf = SendBuffer(bufsize)
        self.resq = deque()
        self.reader = RedisReader()
        self.pid = os.getpid()
        self.params = params
        # this lock protects buf and resq,
        # so that flags do not change
        # in the middle of things.
        self.write_lock = RLock()

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

    def write(self, res, cmd):
        self._checkpid()
        self.resq.append(res)
        self.buf.write(cmd)

    def pump_out(self):
        try:
            self.buf.to_sock(self.sock)
        except Exception as e:
            res = self.resq.popleft()
            res.set_exception(e)

    
    def pump_in(self):
        try:
            bufsize = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            try:
                while True:
                    data = self.sock.recv(bufsize)
                    self.reader.feed(data)
            except socket.error as e:
                if e.errno != errno.EWOULDBLOCK and e.errno != errno.EAGAIN:
                    raise
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

    def connect(self, path, **params):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(path)
        self.sock.setblocking(0)

class TCPConnection(BaseConnection):

    def connect(self, host='localhost', port=6379, **params):
        self.sock = socket.create_connection((host, port))
        self.sock.setblocking(0)


class EventLoop(Thread):
    
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.queue = Queue()
        self.timeout = 1
        self.poll = poll()
        self.readpipe, self.writepipe = os.pipe()
        self.fd_index = {}

    def stop(self):
        os.write(self.writepipe, "stop")

    def send_command(self, conn, *args):
        cmdstr = pack_command(args)
        res = Future()
        with conn.write_lock:
            conn.write(res, cmdstr)
            self._register(conn)
        return res

    def _register(self, conn):
        if conn.fd not in self.fd_index:
            self.fd_index[conn.fd] = conn
            self.poll.register(conn.fd, conn.flags)

    def _unregister(self, conn):
        self.poll.unregister(conn.fd)
        del self.fd_index[conn.fd]

    def _handle_events(self, events):
        for conn, e in events:
            if e & POLLOUT:
                conn.pump_out()
            if e & (POLLIN | POLLPRI):
                conn.pump_in()

            if conn.write_lock.acquire(False):
                if conn.flags:
                    self.poll.register(conn.fd, conn.flags)
                else:
                    self._unregister(conn)
                conn.write_lock.release()
            else: # someone is writing
                self.poll.register(conn.fd, POLLIN | POLLPRI | POLLOUT)
            #print conn.flags, len(conn.resq), conn.buf.count, conn.buf.buf.qsize()

    def run(self):
        # this pipe serves to stop the thread
        # but also to make sure the poll object is never empty.
        # an empty poll seems to return immeditately.
        self.poll.register(self.readpipe, POLLIN | POLLPRI)
        while True:
            events = self.poll.poll(self.timeout)
            conns = []
            for fd, e in events:
                if fd == self.readpipe:
                    return
                conns.append((self.fd_index[fd], e))
            self._handle_events(conns)

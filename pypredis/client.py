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

def connect_unix(path):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.setblocking(0)
    s.connect(path)
    return s

def drain(q):
    while True:
        try:
            yield q.get_nowait()
        except Empty:
            break

class EventLoop(Thread):

    RedisCommand = namedtuple("RedisCommand", ["result", "sockpath", "command"])
    
    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.queue = Queue()
        self.timeout = 0.1
        self.poll = poll()
        self.socks = {}
        self.fd_index = {}
        self.buffers = defaultdict(SendBuffer)
        self.results = defaultdict(deque)
        self.readers = defaultdict(RedisReader)

    def send_command(self, path, *args):
        cmdstr = pack_command(args)
        res = Future()
        cmd = self.RedisCommand(res, path, cmdstr)
        self.queue.put(cmd)
        return res

    def _get_or_create_socket(self, path):
        if path not in self.socks:
            sock = connect_unix(path)
            self.poll.register(sock)
            self.socks[path] = sock
            self.fd_index[sock.fileno()] = sock

        return self.socks[path]

    def _read_commands(self, commands):
        for cmd in commands:
            sock = self._get_or_create_socket(cmd.sockpath)
            self.buffers[sock.fileno()].write(cmd.command)
            self.results[sock.fileno()].append(cmd.result)

    def _handle_events(self, events):
        for fd, e in events:
            if e & POLLOUT:
                self._handle_writes(fd)
            if e & (POLLIN | POLLPRI):
                self._handle_reads(fd)
                self._deliver_replies(fd)

    def _handle_writes(self, fd):
        sock = self.fd_index[fd]
        buf = self.buffers[fd]
        data = buf.peek()
        if data:
            n = sock.send(data)
            buf.written(n)

    def _handle_reads(self, fd):
        sock = self.fd_index[fd]
        reader = self.readers[fd]
        data = sock.recv(4096)
        reader.feed(data)

    def _deliver_replies(self, fd):
        reader = self.readers[fd]
        resq = self.results[fd]
        while True:
            reply = reader.get_reply()
            if not reply:
                break
            res = resq.popleft()
            res.set_result(reply)


    def run(self):
        while True:
            commands = drain(self.queue)
            self._read_commands(commands)

            events = self.poll.poll(self.timeout)
            self._handle_events(events)

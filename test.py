import redis
import pypredis.client
import multiprocessing
from time import clock
from contextlib import contextmanager

@contextmanager
def time():
    start = clock()
    yield
    end = clock()
    print end - start

PINGBATCH = 100000
SETBATCH = 1000

def test_redis_py_ping(conn):
    pl = conn.pipeline()
    for _ in xrange(PINGBATCH):
        pl.ping()

    return pl.execute()

def test_pypredis_ping(*cons):
    el  = pypredis.client.EventLoop()
    try:
        el.start()
        results = []
        for _ in xrange(PINGBATCH/len(cons)):
            for c in cons:
                results.append(
                    el.send_command(c, "PING")
                )

        return [r.result(10) for r in results]
    finally:
        el.stop()

def test_redis_py_sunion(conn):
    pl = conn.pipeline()
    for _ in xrange(SETBATCH):
        pl.sunion('set1', 'set2')

    return pl.execute()

def test_pypredis_sunion(*cons):
    el  = pypredis.client.EventLoop()
    try:
        el.start()
        results = []
        for _ in xrange(SETBATCH/len(cons)):
            for c in cons:
                results.append(
                    el.send_command(c, "SUNION", "set1", "set2")
                )

        return [r.result(10) for r in results]
    finally:
        el.stop()

rc1 = redis.StrictRedis(unix_socket_path="/tmp/redis.0.sock")
rc2 = redis.StrictRedis(unix_socket_path="/tmp/redis.1.sock")

rc1.sadd('set1', *range(0, 1000))
rc1.sadd('set2', *range(500, 1500))
rc2.sadd('set1', *range(0, 1000))
rc2.sadd('set2', *range(500, 1500))

pc1 = pypredis.client.UnixConnection(path="/tmp/redis.0.sock")
pc2 = pypredis.client.UnixConnection(path="/tmp/redis.1.sock")


if __name__ == '__main__':
    print("pypredis ping")
    with time():
        pypres = test_pypredis_ping(pc1, pc2)
    print("redis-py ping")
    with time():
        rpyres = test_redis_py_ping(rc1)

    assert len(pypres) == PINGBATCH
    assert len(rpyres) == PINGBATCH
    assert all(rpyres)
    assert all(r == 'PONG' for r in pypres)

    print("pypredis sunion")
    with time():
        pypres = test_pypredis_sunion(pc1, pc2)
    print("redis-py sunion")
    with time():
        rpyres = test_redis_py_sunion(rc1)

    assert len(pypres) == SETBATCH
    assert len(rpyres) == SETBATCH
    assert set(pypres[0]) == rpyres[0]

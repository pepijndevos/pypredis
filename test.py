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

BATCH = 100000

def test_redis_py_get(conn):
    pl = conn.pipeline()
    for _ in xrange(BATCH):
        pl.get('a')

    return pl.execute()

def test_pypredis_get(*cons):
    el  = pypredis.client.EventLoop()
    el.start()
    results = []
    for _ in xrange(BATCH):
        for c in cons:
            results.append(
                el.send_command(c, "GET", "a")
            )

    return [r.result(10) for r in results]

def test_redis_py_sinter(conn):
    pl = conn.pipeline()
    for _ in xrange(BATCH):
        pl.sinter('set1', 'set2')

    return pl.execute()

def test_pypredis_sinter(*cons):
    el  = pypredis.client.EventLoop()
    el.start()
    results = []
    for _ in xrange(BATCH/len(cons)):
        for c in cons:
            results.append(
                el.send_command(c, "SINTER", "set1", "set2")
            )

    return [r.result(10) for r in results]

rc1 = redis.StrictRedis(unix_socket_path="/tmp/redis.0.sock")
rc2 = redis.StrictRedis(unix_socket_path="/tmp/redis.1.sock")

rc1.set('a', '1')
rc2.set('a', '2')

rc1.sadd('set1', *range(1, 50))
rc1.sadd('set2', *range(30, 70))
rc2.sadd('set1', *range(1, 50))
rc2.sadd('set2', *range(30, 70))

pc1 = pypredis.client.UnixConnection(path="/tmp/redis.0.sock")
pc2 = pypredis.client.UnixConnection(path="/tmp/redis.1.sock")


if __name__ == '__main__':
    print("pypredis get")
    with time():
        test_pypredis_get(pc1, pc2)
    print("redis-py get")
    with time():
        test_redis_py_get(rc1)

    print("pypredis sinter")
    with time():
        test_pypredis_get(pc1, pc2)
    print("redis-py sinter")
    with time():
        test_redis_py_sinter(rc1)

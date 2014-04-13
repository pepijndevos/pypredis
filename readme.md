A heavy-duty Redis library
==========================

A Redis library that connects to multiple instances over a unix socket and streams commands to them.
The main use-case for this is high-throughput sharded data.

Redis-py has a blocking API with batched pipelines.
All queries will be saved on the client, until you execute the pipeline.
Then it sends all messages and waits for all responses.

    pl1.execute()
    pl2.execute()
    pl3.execute()
    pl4.execute()

This results in CPU spikes on single instances, and prevents efficient multicore utilisation.

txRedis would allow non-blocking pipelining, but due to its single-threaded nature
is not suitable for CPU-intensive work.

PypRedis uses an API based on futures, and works by running your commands on a non-blocking IO thread.
This allows you to pipeline in any way you want, attach callbacks, or block on the result.

PypRedis works on PyPy and uses the Hiredis parser.

Very alpha. No support for TCP/IP or Python 3. Patches welcome.

Usage
-----

Currently quite low-level.

```python
In [1]: import pypredis.client

In [2]: e = pypredis.client.EventLoop()

In [3]: e.start()

In [4]: e.send_command("/tmp/redis.sock", "SET", "A", "foo").result()
Out[4]: 'OK'

In [5]: e.send_command("/tmp/redis.sock", "SET", "B", "bar").result()
Out[5]: 'OK'

In [6]: e.send_command("/tmp/redis.sock", "MGET", "A", "B").result()
Out[6]: ['foo', 'bar']
```

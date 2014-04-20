from cffi import FFI

ffi = FFI()
ffi.cdef("""
    typedef struct redisReply {
        int type; /* REDIS_REPLY_* */
        long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
        int len; /* Length of string */
        char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
        size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
        struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
    } redisReply;

    struct redisReader {
        ...;
    };

    struct redisReader *redisReaderCreate(void);
    void redisReaderFree(struct redisReader *r);
    int redisReaderFeed(struct redisReader *r, const char *buf, size_t len);
    int redisReaderGetReply(struct redisReader *r, redisReply **reply);
    void freeReplyObject(void *reply);
""")

hiredis = ffi.verify("""
    #include "hiredis.h"
    """,
    sources=[
        "hiredis/hiredis.c",
        "hiredis/net.c",
        "hiredis/sds.c",
    ],
    include_dirs=["hiredis"])

class HiredisError(Exception):
    pass

class NoReply(HiredisError):
    pass

class RedisReader(object):
    "Hiredis wrapper"

    REDIS_ERR = -1
    REDIS_OK = 0

    REDIS_REPLY_STRING = 1
    REDIS_REPLY_ARRAY = 2
    REDIS_REPLY_INTEGER = 3
    REDIS_REPLY_NIL = 4
    REDIS_REPLY_STATUS = 5
    REDIS_REPLY_ERROR = 6

    def __init__(self):
        self.hiredis = ffi.gc(
            hiredis.redisReaderCreate(),
            hiredis.redisReaderFree)

    def to_list(self, reply):
        res = []
        for idx in xrange(reply.elements):
            element = reply.element[idx]
            res.append(self.to_py(element))

        return res

    def to_py(self, reply):
        if reply.type == self.REDIS_REPLY_STRING:
            return ffi.buffer(reply.str, reply.len)[:]
        elif reply.type == self.REDIS_REPLY_ARRAY:
            return self.to_list(reply)
        elif reply.type == self.REDIS_REPLY_INTEGER:
            return reply.integer
        elif reply.type == self.REDIS_REPLY_NIL:
            return None
        elif reply.type == self.REDIS_REPLY_STATUS:
            return ffi.buffer(reply.str, reply.len)[:]
        elif reply.type == self.REDIS_REPLY_ERROR:
            raise HiredisError(ffi.buffer(reply.str, reply.len)[:])

    def feed(self, data):
        res = hiredis.redisReaderFeed(self.hiredis, data, len(data))
        if res == self.REDIS_ERR:
            raise HiredisError("Error reading")

    def get_reply(self):
        reply_pointer = ffi.new("redisReply * *")
        res = hiredis.redisReaderGetReply(self.hiredis, reply_pointer)
        if res == self.REDIS_ERR:
            raise HiredisError("Error parsing")

        reply = reply_pointer[0]
        if reply:
            gc_reply = ffi.gc(reply, hiredis.freeReplyObject)
            return self.to_py(gc_reply)
        else:
            raise NoReply()


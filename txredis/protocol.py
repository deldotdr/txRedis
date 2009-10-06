""" 
A client for the Redis daemon.


@file protocol.py
@author Dorian Raymer
@author Ludovico Magnocavallo
@date 9/30/09
@brief Twisted compatible version of redis.py
"""

# TODO: Redis._get_multi_response


import decimal
import errno

from twisted.internet import defer
from twisted.internet import reactor
from twisted.protocols import basic
from twisted.protocols import policies

BUFSIZE = 4096


class RedisError(Exception): pass
class ConnectionError(RedisError): pass
class ResponseError(RedisError): pass
class InvalidResponse(RedisError): pass
class InvalidData(RedisError): pass


class Redis(basic.LineReceiver, policies.TimeoutMixin):
    """The main Redis client.
    """
    
    # States
    READY = 0
    SINGLE_LINE = 1
    WAIT_BULK = 2
    WAIT_MULTI = 3

    state = READY

    def __init__(self, db=None, charset='utf8', errors='strict'):
        self.charset = charset
        self.errors = errors
        self.db = db

        self._next_bulk_reply_len = None
        self._multi_bulk_reply_len = None
        self.resultQueue = defer.DeferredQueue()
        
    def _encode(self, s):
        if isinstance(s, str):
            return s
        if isinstance(s, unicode):
            try:
                return s.encode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData("Error encoding unicode value '%s': %s" % (value.encode(self.charset, 'replace'), e))
        return str(s)
    
    def _push_to_result(self, data):
        """depending on the current reply state, add to the result and
        decide if the deferred callback should be fired (result complete).
        """
        # Redis to Python Type converter
        if data == 'none':
            data = None
        if self.state == self.WAIT_MULTI:
            self.result.append(data)
            if len(self.result) >= self._multi_bulk_reply_len:
                self.finishResult()
        else:
            self.result = data
            self.finishResult()

    def finishResult(self):
        """fire deferred with current result, and rest current result
        """
        self.state = self.READY
        res = self.result
        self.resultQueue.put(res)
        self.result = None

    def lineReceived(self, line):
        """
        Simple state machine. 
        States:
         - Ready to receive 
          o "-" error message
          o "+" single line reply
          o "$" bulk data
          o "*" multi-bulk data
          o ":" integer number
         - Receiving 
          o A single line
          o Multiple lines
        """
        self.resetTimeout()
        c = line[0]
        if c == '-':
            # return error for errback instead of raising...
            self._push_to_result(ResponseError(line[5:] if line[:5] == '-ERR ' else line[1:]))
            return
        if c == '+':
            self.state = self.SINGLE_LINE
            self._push_to_result(line[1:])
            return
        if c == '*':
            try:
                self._multi_bulk_reply_len = int(line[1:])
                if self._multi_bulk_reply_len == -1:
                    self._push_to_result(None)
                    return
                if self._multi_bulk_reply_len == 0:
                    self._push_to_result([])
                    return
                self.state = self.WAIT_MULTI
            except (TypeError, ValueError):
                self._push_to_result(InvalidResponse("Cannot convert multi-response header '%s' to integer" % line))
                return
            self.result = list()
            return
        return self._get_value(line)

    def _get_value(self, data):
        """
        get value of bulk data reply (or one line of multi bulk reply)
        """
        if self._next_bulk_reply_len is not None:
            try:
                self._push_to_result(int(data) if data.find('.') == -1 else decimal.Decimal(data))
            except (ValueError, decimal.InvalidOperation):
                self._push_to_result(data.decode(self.charset))
            self._next_bulk_reply_len = None
            return

        if data == '$-1':
            self._push_to_result(None)
            return
        try:
            c, i = data[0], (int(data[1:]) if data.find('.') == -1 else float(data[1:]))
        except ValueError:
            self._push_to_result(InvalidResponse("Cannot convert data '%s' to integer" % data))
            return
        if c == ':':
            self._push_to_result(i)
            return 
        if c == '$':
            self._next_bulk_reply_len = i
            if i == 0:
                self._push_to_result('')
            return

    def get_response(self):
        """return deferred which will fire with response from server.
        """
        return self.resultQueue.get()

    
    def _write(self, s):
        """
        XXX replace this with sendLine
        """
        self.transport.write(s)
            
    def ping(self):
        """
        >>> r = Redis(db=9)
        >>> r.ping()
        'PONG'
        >>> 
        """
        self.connect()
        self._write('PING\r\n')
        return self.get_response()
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # REDIS COMMANDS
    # 

    # Commands operating on string values
    def set(self, name, value, preserve=False, getset=False):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo')
        'OK'
        >>> r.set('a', u'pippo \u3235')
        'OK'
        >>> r.get('a')
        u'pippo \u3235'
        >>> r.set('b', 105.2)
        'OK'
        >>> r.set('b', 'xxx', preserve=True)
        0
        >>> r.get('b')
        Decimal("105.2")
        >>> 
        """
        # the following will raise an error for unicode values that can't be encoded to ascii
        # we could probably add an 'encoding' arg to init, but then what do we do with get()?
        # convert back to unicode? and what about ints, or pickled values?
        if getset: command = 'GETSET'
        elif preserve: command = 'SETNX'
        else: command = 'SET'
        value = self._encode(value)
        self._write('%s %s %s\r\n%s\r\n' % (
                command, name, len(value), value
            ))
        return self.get_response()
    
    def get(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo'), r.set('b', 15), r.set('c', ' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '), r.set('d', '\\r\\n')
        ('OK', 'OK', 'OK', 'OK')
        >>> r.get('a')
        u'pippo'
        >>> r.get('b')
        15
        >>> r.get('d')
        u'\\r\\n'
        >>> r.get('b')
        15
        >>> r.get('c')
        u' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '
        >>> r.get('c')
        u' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '
        >>> r.get('ajhsd')
        >>> 
        """
        self.connect()
        self._write('GET %s\r\n' % name)
        return self.get_response()
    
    def getset(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo')
        'OK'
        >>> r.getset('a', 2)
        u'pippo'
        >>> 
        """
        return self.set(name, value, getset=True)
        
    def mget(self, *args):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo'), r.set('b', 15), r.set('c', '\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n'), r.set('d', '\\r\\n')
        ('OK', 'OK', 'OK', 'OK')
        >>> r.mget('a', 'b', 'c', 'd')
        [u'pippo', 15, u'\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n', u'\\r\\n']
        >>> 
        """
        self.connect()
        self._write('MGET %s\r\n' % ' '.join(args))
        return self.get_response()
    
    def incr(self, name, amount=1):
        """
        >>> r = Redis(db=9)
        >>> r.delete('a')
        1
        >>> r.incr('a')
        1
        >>> r.incr('a')
        2
        >>> r.incr('a', 2)
        4
        >>>
        """
        self.connect()
        if amount == 1:
            self._write('INCR %s\r\n' % name)
        else:
            self._write('INCRBY %s %s\r\n' % (name, amount))
        return self.get_response()

    def decr(self, name, amount=1):
        """
        >>> r = Redis(db=9)
        >>> if r.get('a'):
        ...     r.delete('a')
        ... else:
        ...     print 1
        1
        >>> r.decr('a')
        -1
        >>> r.decr('a')
        -2
        >>> r.decr('a', 5)
        -7
        >>> 
        """
        self.connect()
        if amount == 1:
            self._write('DECR %s\r\n' % name)
        else:
            self._write('DECRBY %s %s\r\n' % (name, amount))
        return self.get_response()
    
    def exists(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.exists('dsjhfksjdhfkdsjfh')
        0
        >>> r.set('a', 'a')
        'OK'
        >>> r.exists('a')
        1
        >>>
        """
        self.connect()
        self._write('EXISTS %s\r\n' % name)
        return self.get_response()

    def delete(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.delete('dsjhfksjdhfkdsjfh')
        0
        >>> r.set('a', 'a')
        'OK'
        >>> r.delete('a')
        1
        >>> r.exists('a')
        0
        >>> r.delete('a')
        0
        >>> 
        """
        self.connect()
        self._write('DEL %s\r\n' % name)
        return self.get_response()

    def get_type(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 3)
        'OK'
        >>> r.get_type('a')
        'string'
        >>> r.get_type('zzz')
        >>> 
        """
        self.connect()
        self._write('TYPE %s\r\n' % name)
        res = self.get_response()
        # return None if res == 'none' else res
        return res
    
    # Commands operating on the key space
    @defer.inlineCallbacks
    def keys(self, pattern):
        """
        >>> r = Redis(db=9)
        >>> r.flush()
        'OK'
        >>> r.set('a', 'a')
        'OK'
        >>> r.keys('a*')
        [u'a']
        >>> r.set('a2', 'a')
        'OK'
        >>> r.keys('a*')
        [u'a', u'a2']
        >>> r.delete('a2')
        1
        >>> r.keys('sjdfhskjh*')
        []
        >>>
        """
        self.connect()
        self._write('KEYS %s\r\n' % pattern)
        # return self.get_response().split()
        r = yield self.get_response()
        if r is not None:
            res = r.split()
            res.sort()# XXX is sort ok?
        else:
            res = []
        defer.returnValue(res)
    
    def randomkey(self):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'a')
        'OK'
        >>> isinstance(r.randomkey(), str)
        True
        >>> 
        """
        #raise NotImplementedError("Implemented but buggy, do not use.")
        self.connect()
        self._write('RANDOMKEY\r\n')
        return self.get_response()
    
    def rename(self, src, dst, preserve=False):
        """
        >>> r = Redis(db=9)
        >>> try:
        ...     r.rename('a', 'a')
        ... except ResponseError, e:
        ...     print e
        source and destination objects are the same
        >>> r.rename('a', 'b')
        'OK'
        >>> try:
        ...     r.rename('a', 'b')
        ... except ResponseError, e:
        ...     print e
        no such key
        >>> r.set('a', 1)
        'OK'
        >>> r.rename('b', 'a', preserve=True)
        0
        >>> 
        """
        self.connect()
        if preserve:
            self._write('RENAMENX %s %s\r\n' % (src, dst))
            return self.get_response()
        else:
            self._write('RENAME %s %s\r\n' % (src, dst))
            return self.get_response() #.strip()
        
    def dbsize(self):
        """
        >>> r = Redis(db=9)
        >>> type(r.dbsize())
        <type 'int'>
        >>> 
        """
        self.connect()
        self._write('DBSIZE\r\n')
        return self.get_response()
    
    def expire(self, name, time):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 1)
        'OK'
        >>> r.expire('a', 1)
        1
        >>> r.expire('zzzzz', 1)
        0
        >>> 
        """
        self.connect()
        self._write('EXPIRE %s %s\r\n' % (name, time))
        return self.get_response()
    
    def ttl(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.ttl('a')
        -1
        >>> r.expire('a', 10)
        1
        >>> r.ttl('a')
        10
        >>> r.expire('a', 0)
        0
        >>> 
        """
        self.connect()
        self._write('TTL %s\r\n' % name)
        return self.get_response()
    
    # Commands operating on lists
    def push(self, name, value, tail=False):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'a')
        'OK'
        >>> r.set('a', 'a')
        'OK'
        >>> try:
        ...     r.push('a', 'a')
        ... except ResponseError, e:
        ...     print e
        Operation against a key holding the wrong kind of value
        >>> 
        """
        self.connect()
        value = self._encode(value)
        self._write('%s %s %s\r\n%s\r\n' % (
            'LPUSH' if tail else 'RPUSH', name, len(value), value
        ))
        return self.get_response()
    
    def llen(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'a')
        'OK'
        >>> r.llen('l')
        1
        >>> r.push('l', 'a')
        'OK'
        >>> r.llen('l')
        2
        >>> 
        """
        self.connect()
        self._write('LLEN %s\r\n' % name)
        return self.get_response()

    def lrange(self, name, start, end):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.lrange('l', 0, 1)
        []
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lrange('l', 0, 1)
        [u'aaa']
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.lrange('l', 0, 0)
        [u'aaa']
        >>> r.lrange('l', 0, 1)
        [u'aaa', u'bbb']
        >>> r.lrange('l', -1, 0)
        []
        >>> r.lrange('l', -1, -1)
        [u'bbb']
        >>> 
        """
        self.connect()
        self._write('LRANGE %s %s %s\r\n' % (name, start, end))
        return self.get_response()
        
    def ltrim(self, name, start, end):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> try:
        ...     r.ltrim('l', 0, 1)
        ... except ResponseError, e:
        ...     print e
        no such key
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.push('l', 'ccc')
        'OK'
        >>> r.ltrim('l', 0, 1)
        'OK'
        >>> r.llen('l')
        2
        >>> r.ltrim('l', 99, 95)
        'OK'
        >>> r.llen('l')
        0
        >>> 
        """
        self.connect()
        self._write('LTRIM %s %s %s\r\n' % (name, start, end))
        return self.get_response()
    
    def lindex(self, name, index):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('l')
        >>> r.lindex('l', 0)
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lindex('l', 0)
        u'aaa'
        >>> r.lindex('l', 2)
        >>> r.push('l', 'ccc')
        'OK'
        >>> r.lindex('l', 1)
        u'ccc'
        >>> r.lindex('l', -1)
        u'ccc'
        >>> 
        """
        self.connect()
        self._write('LINDEX %s %s\r\n' % (name, index))
        return self.get_response()
        
    def pop(self, name, tail=False):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.pop('l')
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.pop('l')
        u'aaa'
        >>> r.pop('l')
        u'bbb'
        >>> r.pop('l')
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.pop('l', tail=True)
        u'bbb'
        >>> r.pop('l')
        u'aaa'
        >>> r.pop('l')
        >>> 
        """
        self.connect()
        self._write('%s %s\r\n' % ('RPOP' if tail else 'LPOP', name))
        return self.get_response()
    
    def lset(self, name, index, value):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> try:
        ...     r.lset('l', 0, 'a')
        ... except ResponseError, e:
        ...     print e
        no such key
        >>> r.push('l', 'aaa')
        'OK'
        >>> try:
        ...     r.lset('l', 1, 'a')
        ... except ResponseError, e:
        ...     print e
        index out of range
        >>> r.lset('l', 0, 'bbb')
        'OK'
        >>> r.lrange('l', 0, 1)
        [u'bbb']
        >>> 
        """
        self.connect()
        value = self._encode(value)
        self._write('LSET %s %s %s\r\n%s\r\n' % (
            name, index, len(value), value
        ))
        return self.get_response()
    
    def lrem(self, name, value, num=0):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lrem('l', 'aaa')
        2
        >>> r.lrange('l', 0, 10)
        [u'bbb']
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lrem('l', 'aaa', 1)
        1
        >>> r.lrem('l', 'aaa', 1)
        1
        >>> r.lrem('l', 'aaa', 1)
        0
        >>> 
        """
        self.connect()
        value = self._encode(value)
        self._write('LREM %s %s %s\r\n%s\r\n' % (
            name, num, len(value), value
        ))
        return self.get_response()
    
    # Commands operating on sets
    def sadd(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s')
        >>> r.sadd('s', 'a')
        1
        >>> r.sadd('s', 'b')
        1
        >>> 
        """
        self.connect()
        value = self._encode(value)
        self._write('SADD %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        return self.get_response()
        
    def srem(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> r.delete('s')
        1
        >>> r.srem('s', 'aaa')
        0
        >>> r.sadd('s', 'b')
        1
        >>> r.srem('s', 'b')
        1
        >>> r.sismember('s', 'b')
        0
        >>> 
        """
        self.connect()
        value = self._encode(value)
        self._write('SREM %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        return self.get_response()
    
    def sismember(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> r.delete('s')
        1
        >>> r.sismember('s', 'b')
        0
        >>> r.sadd('s', 'a')
        1
        >>> r.sismember('s', 'b')
        0
        >>> r.sismember('s', 'a')
        1
        >>>
        """
        self.connect()
        value = self._encode(value)
        self._write('SISMEMBER %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        return self.get_response()
    
    @defer.inlineCallbacks
    def sinter(self, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> try:
        ...     r.sinter()
        ... except ResponseError, e:
        ...     print e
        wrong number of arguments
        >>> try:
        ...     r.sinter('l')
        ... except ResponseError, e:
        ...     print e
        Operation against a key holding the wrong kind of value
        >>> r.sinter('s1', 's2', 's3')
        set([])
        >>> r.sinter('s1', 's2')
        set([u'a'])
        >>> 
        """
        self.connect()
        self._write('SINTER %s\r\n' % ' '.join(args))
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)
    
    def sinterstore(self, dest, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> r.sinterstore('s_s', 's1', 's2', 's3')
        0
        >>> r.sinterstore('s_s', 's1', 's2')
        1
        >>> r.smembers('s_s')
        set([u'a'])
        >>> 
        """
        self.connect()
        self._write('SINTERSTORE %s %s\r\n' % (dest, ' '.join(args)))
        return self.get_response()

    @defer.inlineCallbacks
    def smembers(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.delete('s')
        1
        >>> r.sadd('s', 'a')
        1
        >>> r.sadd('s', 'b')
        1
        >>> try:
        ...     r.smembers('l')
        ... except ResponseError, e:
        ...     print e
        Operation against a key holding the wrong kind of value
        >>> r.smembers('s')
        set([u'a', u'b'])
        >>> 
        """
        self.connect()
        self._write('SMEMBERS %s\r\n' % name)
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def sunion(self, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> r.sunion('s1', 's2', 's3')
        set([u'a', u'b'])
        >>> r.sadd('s2', 'c')
        1
        >>> r.sunion('s1', 's2', 's3')
        set([u'a', u'c', u'b'])
        >>> 
        """
        self.connect()
        self._write('SUNION %s\r\n' % ' '.join(args))
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)

    def sunionstore(self, dest, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> r.sunionstore('s4', 's1', 's2', 's3')
        2
        >>> r.smembers('s4')
        set([u'a', u'b'])
        >>> 
        """
        self.connect()
        self._write('SUNIONSTORE %s %s\r\n' % (dest, ' '.join(args)))
        return self.get_response()

    # Multiple databases handling commands
    def select(self, db):
        """
        >>> r = Redis(db=9)
        >>> r.delete('a')
        1
        >>> r.select(10)
        'OK'
        >>> r.set('a', 1)
        'OK'
        >>> r.select(9)
        'OK'
        >>> r.get('a')
        >>> 
        """
        self.connect()
        self._write('SELECT %s\r\n' % db)
        return self.get_response()
    
    def move(self, name, db):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'a')
        'OK'
        >>> r.select(10)
        'OK'
        >>> if r.get('a'):
        ...     r.delete('a')
        ... else:
        ...     print 1
        1
        >>> r.select(9)
        'OK'
        >>> r.move('a', 10)
        1
        >>> r.get('a')
        >>> r.select(10)
        'OK'
        >>> r.get('a')
        u'a'
        >>> r.select(9)
        'OK'
        >>> 
        """
        self.connect()
        self._write('MOVE %s %s\r\n' % (name, db))
        return self.get_response()
    
    def flush(self, all_dbs=False):
        """
        >>> r = Redis(db=9)
        >>> r.flush()
        'OK'
        >>> # r.flush(all_dbs=True)
        >>> 
        """
        self.connect()
        self._write('%s\r\n' % ('FLUSHALL' if all_dbs else 'FLUSHDB'))
        return self.get_response()
    
    # Persistence control commands
    def save(self, background=False):
        """
        >>> r = Redis(db=9)
        >>> r.save()
        'OK'
        >>> try:
        ...     resp = r.save(background=True)
        ... except ResponseError, e:
        ...     assert str(e) == 'background save already in progress', str(e)
        ... else:
        ...     assert resp == 'OK'
        >>> 
        """
        self.connect()
        if background:
            self._write('BGSAVE\r\n')
        else:
            self._write('SAVE\r\n')
        return self.get_response()
        
    def lastsave(self):
        """
        >>> import time
        >>> r = Redis(db=9)
        >>> t = int(time.time())
        >>> r.save()
        'OK'
        >>> r.lastsave() >= t
        True
        >>> 
        """
        self.connect()
        self._write('LASTSAVE\r\n')
        return self.get_response()
    
    @defer.inlineCallbacks
    def info(self):
        """
        >>> r = Redis(db=9)
        >>> info = r.info()
        >>> info and isinstance(info, dict)
        True
        >>> isinstance(info.get('connected_clients'), int)
        True
        >>> 
        """
        self.connect()
        self._write('INFO\r\n')
        info = dict()
        res = yield self.get_response()
        for l in res.split('\r\n'):
            if not l:
                continue
            k, v = l.split(':', 1)
            info[k] = int(v) if v.isdigit() else v
        defer.returnValue(info)
    
    def sort(self, name, by=None, get=None, start=None, num=None, desc=False, alpha=False):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'ccc')
        'OK'
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'ddd')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.sort('l', alpha=True)
        [u'aaa', u'bbb', u'ccc', u'ddd']
        >>> r.delete('l')
        1
        >>> for i in range(1, 5):
        ...     res = r.push('l', 1.0 / i)
        >>> r.sort('l')
        [Decimal("0.25"), Decimal("0.333333333333"), Decimal("0.5"), Decimal("1.0")]
        >>> r.sort('l', desc=True)
        [Decimal("1.0"), Decimal("0.5"), Decimal("0.333333333333"), Decimal("0.25")]
        >>> r.sort('l', desc=True, start=2, num=1)
        [Decimal("0.333333333333")]
        >>> r.set('weight_0.5', 10)
        'OK'
        >>> r.sort('l', desc=True, by='weight_*')
        [Decimal("0.5"), Decimal("1.0"), Decimal("0.333333333333"), Decimal("0.25")]
        >>> for i in r.sort('l', desc=True):
        ...     res = r.set('test_%s' % i, 100 - float(i))
        >>> r.sort('l', desc=True, get='test_*')
        [Decimal("99.0"), Decimal("99.5"), Decimal("99.6666666667"), Decimal("99.75")]
        >>> r.sort('l', desc=True, by='weight_*', get='test_*')
        [Decimal("99.5"), Decimal("99.0"), Decimal("99.6666666667"), Decimal("99.75")]
        >>> r.sort('l', desc=True, by='weight_*', get='missing_*')
        [None, None, None, None]
        >>> 
        """
        stmt = ['SORT', name]
        if by:
            stmt.append("BY %s" % by)
        if start and num:
            stmt.append("LIMIT %s %s" % (start, num))
        if get is None:
            pass
        elif isinstance(get, basestring):
            stmt.append("GET %s" % get)
        elif isinstance(get, list) or isinstance(get, tuple):
            for g in get:
                stmt.append("GET %s" % g)
        else:
            raise RedisError("Invalid parameter 'get' for Redis sort")
        if desc:
            stmt.append("DESC")
        if alpha:
            stmt.append("ALPHA")
        self.connect()
        self._write(' '.join(stmt + ["\r\n"]))
        return self.get_response()
    
    def auth(self, passwd):
        self.connect()
        self._write('AUTH %s\r\n' % passwd)
        return self.get_response()
    
    def disconnect(self):
        """Don't need this with twisted.
        """
            
    def connect(self):
        """Don't need this with twisted.
        """
                

    

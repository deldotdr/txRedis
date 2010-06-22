
import time

from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.trial import unittest

from txredis.protocol import Redis
from txredis.protocol import ResponseError

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

class CommandsTestBase(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        clientCreator = protocol.ClientCreator(reactor, Redis)
        self.redis = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)

    def tearDown(self):
        self.redis.transport.loseConnection()

class General(CommandsTestBase):
    """Test commands that operate on any type of redis value.
    """

    @defer.inlineCallbacks
    def test_ping(self):
        a = yield self.redis.ping()
        self.assertEqual(a, 'PONG')

    @defer.inlineCallbacks
    def test_exists(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.exists('dsjhfksjdhfkdsjfh')
        ex = 0
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.exists('a')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_delete(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('dsjhfksjdhfkdsjfh')
        ex = 0
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.delete('a')
        ex = 1
        t(a, ex)
        a = yield r.exists('a')
        ex = 0
        t(a, ex)
        a = yield r.delete('a')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_get_type(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 3)
        ex = 'OK'
        t(a, ex)
        a = yield r.get_type('a')
        ex = 'string'
        t(a, ex)
        a = yield r.get_type('zzz')
        ex = None
        t(a, ex)

    @defer.inlineCallbacks
    def test_keys(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.flush()
        ex = 'OK'
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.keys('a*')
        ex = [u'a']
        t(a, ex)
        a = yield r.set('a2', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.keys('a*')
        ex = [u'a', u'a2']
        t(a, ex)
        a = yield r.delete('a2')
        ex = 1
        t(a, ex)
        a = yield r.keys('sjdfhskjh*')
        ex = []
        t(a, ex)

    @defer.inlineCallbacks
    def test_randomkey(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        random_key = yield r.randomkey()
        a = yield isinstance(random_key, basestring)
        ex = True
        t(a, ex)

    @defer.inlineCallbacks
    def test_rename(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.rename('a', 'a')
        ex = ResponseError('source and destination objects are the same')
        t(str(a), str(ex))
        a = yield r.rename('a', 'b')
        ex = 'OK'
        t(a, ex)
        a = yield r.rename('a', 'b')
        ex = ResponseError('no such key')
        t(str(a), str(ex))
        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.rename('b', 'a', preserve=True)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_dbsize(self):
        r = self.redis
        t = self.assertEqual

        a = type((yield r.dbsize()))
        ex = int
        t(a, ex)

    @defer.inlineCallbacks
    def test_expire(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.expire('a', 1)
        ex = 1
        t(a, ex)
        a = yield r.expire('zzzzz', 1)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_setex(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('q', 1, expire=10)
        ex = 'OK'
        t(a, ex)
        a = yield r.expire('q', 1)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_mset(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.mset({'ma' : 1, 'mb' : 2})
        ex = 'OK'
        t(a, ex)

        a = yield r.mset({'ma' : 1, 'mb' : 2}, preserve=True)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_substr(self):
        r = self.redis
        t = self.assertEqual

        string = 'This is a string'
        r.set('s', string)
        a = yield r.substr('s', 0, 3)
        ex = 'This'
        t(a, ex)


    @defer.inlineCallbacks
    def test_append(self):
        r = self.redis
        t = self.assertEqual

        string = 'some_string'
        a = yield r.set('q', string)
        ex = 'OK'
        t(a, ex)

        addition = 'foo'
        a = yield r.append('q', addition)
        ex = len(string + addition)
        t(a, ex)


    @defer.inlineCallbacks
    def test_ttl(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.ttl('a')
        ex = -1
        t(a, ex)
        a = yield r.expire('a', 10)
        ex = 1
        t(a, ex)
        a = yield r.ttl('a')
        ex = 10
        t(a, ex)
        a = yield r.expire('a', 1)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_select(self):
        r = self.redis
        t = self.assertEqual

        yield r.select(9)
        yield r.delete('a')
        a = yield r.select(10)
        ex = 'OK'
        t(a, ex)
        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.select(9)
        ex = 'OK'
        t(a, ex)
        a = yield r.get('a')
        ex = None
        t(a, ex)

    @defer.inlineCallbacks
    def test_move(self):
        r = self.redis
        t = self.assertEqual

        yield r.select(9)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.select(10)
        ex = 'OK'
        t(a, ex)
        if (yield r.get('a')):
            yield r.delete('a')
        a = yield r.select(9)
        ex = 'OK'
        t(a, ex)
        a = yield r.move('a', 10)
        ex = 1
        t(a, ex)
        yield r.get('a')
        a = yield r.select(10)
        ex = 'OK'
        t(a, ex)
        a = yield r.get('a')
        ex = u'a'
        t(a, ex)
        a = yield r.select(9)
        ex = 'OK'
        t(a, ex)

    @defer.inlineCallbacks
    def test_flush(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.flush()
        ex = 'OK'
        t(a, ex)

    @defer.inlineCallbacks
    def test_save(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.save()
        ex = 'OK'
        t(a, ex)
        resp = yield r.save(background=True)
        """
        ex = ResponseError(
        ...     assert str(e) == 'background save already in progress', str(e)
        ... else:
        ...     assert resp == 'OK'
        """

    @defer.inlineCallbacks
    def test_lastsave(self):
        r = self.redis
        t = self.assertEqual

        tme = int(time.time())
        a = yield r.save()
        ex = 'OK'
        t(a, ex)
        a = (yield r.lastsave()) >= tme
        ex = True
        t(a, ex)

    @defer.inlineCallbacks
    def test_info(self):
        r = self.redis
        t = self.assertEqual

        info = yield r.info()
        a = info and isinstance(info, dict)
        ex = True
        t(a, ex)
        a = isinstance((yield info.get('connected_clients')), int)
        ex = True
        t(a, ex)


class Strings(CommandsTestBase):
    """Test commands that operate on string values.
    """

    @defer.inlineCallbacks
    def test_set(self):
        a = yield self.redis.set('a', 'pippo')
        self.assertEqual(a, 'OK')

        a = yield self.redis.set('a', u'pippo \u3235')
        self.assertEqual(a, 'OK')

        a = yield self.redis.get('a')
        self.assertEqual(a, u'pippo \u3235')

        a = yield self.redis.set('b', 105.2)
        self.assertEqual(a, 'OK')

        a = yield self.redis.set('b', 'xxx', preserve=True)
        self.assertEqual(a, 0)

        a = yield self.redis.get('b')
        self.assertEqual(a, 105.2)

    @defer.inlineCallbacks
    def test_get(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'pippo')
        t(a, 'OK')
        a = yield r.set('b', 15)
        t(a, 'OK')
        a = yield r.set('c', ' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n ')
        t(a, 'OK')
        a = yield r.set('d', '\\r\\n')
        t(a, 'OK')

        a = yield r.get('a')
        t(a, u'pippo')

        a = yield r.get('b')
        ex = 15
        t(a, ex)

        a = yield r.get('d')
        ex = u'\\r\\n'
        t(a, ex)

        a = yield r.get('b')
        ex = 15
        t(a, ex)

        a = yield r.get('c')
        ex = u' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '
        t(a, ex)

        a = yield r.get('ajhsd')
        ex = None
        t(a, ex)


    @defer.inlineCallbacks
    def test_getset(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'pippo')
        ex = 'OK'
        t(a, ex)

        a = yield r.getset('a', 2)
        ex = u'pippo'
        t(a, ex)

    @defer.inlineCallbacks
    def test_mget(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'pippo')
        ex = 'OK'
        t(a, ex)
        a = yield r.set('b', 15)
        ex = 'OK'
        t(a, ex)
        a = yield r.set('c', '\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n')
        ex = 'OK'
        t(a, ex)
        a = yield r.set('d', '\\r\\n')
        ex = 'OK'
        t(a, ex)
        a = yield r.mget('a', 'b', 'c', 'd')
        ex = [u'pippo', 15, u'\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n', u'\\r\\n']
        t(a, ex)

    @defer.inlineCallbacks
    def test_incr(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('a')
        ex = 1
        t(a, ex)
        a = yield r.incr('a')
        ex = 1
        t(a, ex)
        a = yield r.incr('a')
        ex = 2
        t(a, ex)
        a = yield r.incr('a', 2)
        ex = 4
        t(a, ex)


    @defer.inlineCallbacks
    def test_decr(self):
        r = self.redis
        t = self.assertEqual


        a = yield r.get('a')
        if a:
            yield r.delete('a')

        a = yield r.decr('a')
        ex = -1
        t(a, ex)
        a = yield r.decr('a')
        ex = -2
        t(a, ex)
        a = yield r.decr('a', 5)
        ex = -7
        t(a, ex)


class Lists(CommandsTestBase):
    """Test commands that operate on lists.
    """

    @defer.inlineCallbacks
    def test_push(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.push('l', 'a')
        ex = 1
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.push('a', 'a')
        ex = ResponseError('Operation against a key holding the wrong kind of value')
        t(str(a), str(ex))

    @defer.inlineCallbacks
    def test_llen(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.push('l', 'a')
        ex = 1
        t(a, ex)
        a = yield r.llen('l')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'a')
        ex = 2
        t(a, ex)
        a = yield r.llen('l')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_lrange(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.lrange('l', 0, 1)
        ex = []
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'aaa']
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.lrange('l', 0, 0)
        ex = [u'aaa']
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'aaa', u'bbb']
        t(a, ex)
        a = yield r.lrange('l', -1, 0)
        ex = []
        t(a, ex)
        a = yield r.lrange('l', -1, -1)
        ex = [u'bbb']
        t(a, ex)

    @defer.inlineCallbacks
    def test_ltrim(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.ltrim('l', 0, 1)
        ex = ResponseError('OK')
        t(str(a), str(ex))
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'ccc')
        ex = 3
        t(a, ex)
        a = yield r.ltrim('l', 0, 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.llen('l')
        ex = 2
        t(a, ex)
        a = yield r.ltrim('l', 99, 95)
        ex = 'OK'
        t(a, ex)
        a = yield r.llen('l')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_lindex(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        yield r.lindex('l', 0)
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.lindex('l', 0)
        ex = u'aaa'
        t(a, ex)
        yield r.lindex('l', 2)
        a = yield r.push('l', 'ccc')
        ex = 2
        t(a, ex)
        a = yield r.lindex('l', 1)
        ex = u'ccc'
        t(a, ex)
        a = yield r.lindex('l', -1)
        ex = u'ccc'
        t(a, ex)

    @defer.inlineCallbacks
    def test_pop(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        yield r.pop('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.pop('l')
        ex = u'aaa'
        t(a, ex)
        a = yield r.pop('l')
        ex = u'bbb'
        t(a, ex)
        yield r.pop('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.pop('l', tail=True)
        ex = u'bbb'
        t(a, ex)
        a = yield r.pop('l')
        ex = u'aaa'
        t(a, ex)
        a = yield r.pop('l')
        ex = None
        t(a, ex)

    @defer.inlineCallbacks
    def test_lset(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        a = yield r.lset('l', 0, 'a')
        ex = ResponseError('no such key')
        t(str(a), str(ex))
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.lset('l', 1, 'a')
        ex = ResponseError('index out of range')
        t(str(a), str(ex))
        a = yield r.lset('l', 0, 'bbb')
        ex = 'OK'
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'bbb']
        t(a, ex)

    @defer.inlineCallbacks
    def test_lrem(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 3
        t(a, ex)
        a = yield r.lrem('l', 'aaa')
        ex = 2
        t(a, ex)
        a = yield r.lrange('l', 0, 10)
        ex = [u'bbb']
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 3
        t(a, ex)
        a = yield r.lrem('l', 'aaa', 1)
        ex = 1
        t(a, ex)
        a = yield r.lrem('l', 'aaa', 1)
        ex = 1
        t(a, ex)
        a = yield r.lrem('l', 'aaa', 1)
        ex = 0
        t(a, ex)

class Sets(CommandsTestBase):
    """Test commands that operate on sets.
    """

    @defer.inlineCallbacks
    def test_sadd(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s', 'b')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_srem(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.srem('s', 'aaa')
        ex = 0
        t(a, ex)
        a = yield r.sadd('s', 'b')
        ex = 1
        t(a, ex)
        a = yield r.srem('s', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sismember('s', 'b')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_spop(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('s')

        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)

        a = yield r.spop('s')
        ex = u'a'
        t(a, ex)

    @defer.inlineCallbacks
    def test_scard(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('s')

        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)

        a = yield r.scard('s')
        ex = 1
        t(a, ex)


    @defer.inlineCallbacks
    def test_sismember(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sismember('s', 'b')
        ex = 0
        t(a, ex)
        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sismember('s', 'b')
        ex = 0
        t(a, ex)
        a = yield r.sismember('s', 'a')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_sinter(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sinter()
        ex = ResponseError("wrong number of arguments for 'sinter' command")
        t(str(a), str(ex))
        a = yield r.sinter('l')
        ex = ResponseError('Operation against a key holding the wrong kind of value')
        t(str(a), str(ex))
        a = yield r.sinter('s1', 's2', 's3')
        ex = set([])
        t(a, ex)
        a = yield r.sinter('s1', 's2')
        ex = set([u'a'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sinterstore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sinterstore('s_s', 's1', 's2', 's3')
        ex = 0
        t(a, ex)
        a = yield r.sinterstore('s_s', 's1', 's2')
        ex = 1
        t(a, ex)
        a = yield r.smembers('s_s')
        ex = set([u'a'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_smembers(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('s')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s', 'b')
        ex = 1
        t(a, ex)
        a = yield r.smembers('l')
        ex = ResponseError('Operation against a key holding the wrong kind of value')
        t(str(a), str(ex))
        a = yield r.smembers('s')
        ex = set([u'a', u'b'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sunion(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sunion('s1', 's2', 's3')
        ex = set([u'a', u'b'])
        t(a, ex)
        a = yield r.sadd('s2', 'c')
        ex = 1
        t(a, ex)
        a = yield r.sunion('s1', 's2', 's3')
        ex = set([u'a', u'c', u'b'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sunionstore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sunionstore('s4', 's1', 's2', 's3')
        ex = 2
        t(a, ex)
        a = yield r.smembers('s4')
        ex = set([u'a', u'b'])
        t(a, ex)


    @defer.inlineCallbacks
    def test_sort(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        a = yield r.push('l', 'ccc')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'ddd')
        ex = 3
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 4
        t(a, ex)
        a = yield r.sort('l', alpha=True)
        ex = [u'aaa', u'bbb', u'ccc', u'ddd']
        t(a, ex)
        a = yield r.delete('l')
        ex = 1
        t(a, ex)
        for i in range(1, 5):
            yield r.push('l', 1.0 / i)
        a = yield r.sort('l')
        ex = [0.25, 0.333333333333, 0.5, 1.0]
        t(a, ex)
        a = yield r.sort('l', desc=True)
        ex = [1.0, 0.5, 0.333333333333, 0.25]
        t(a, ex)
        a = yield r.sort('l', desc=True, start=2, num=1)
        ex = [0.333333333333]
        t(a, ex)
        a = yield r.set('weight_0.5', 10)
        ex = 'OK'
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*')
        ex = [0.5, 1.0, 0.333333333333, 0.25]
        t(a, ex)
        for i in (yield r.sort('l', desc=True)):
            yield r.set('test_%s' % i, 100 - float(i))
        a = yield r.sort('l', desc=True, get='test_*')
        ex = [99.0, 99.5, 99.6666666667, 99.75]
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*', get='test_*')
        ex = [99.5, 99.0, 99.6666666667, 99.75]
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*', get='missing_*')
        ex = [None, None, None, None]
        t(a, ex)

    @defer.inlineCallbacks
    def test_large_values(self):
        import uuid
        import random
        r = self.redis
        t = self.assertEqual

        for i in range(5):
            key = str(uuid.uuid4())
            value = random.randrange(10**40000, 11**40000)
            a = yield r.set(key, value)
            t('OK', a)
            rval = yield r.get(key)
            t(value, rval)


class Hash(CommandsTestBase):
    """Test commands that operate on hashes.
    """
    @defer.inlineCallbacks
    def test_basic(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')

        a = yield r.hexists('d', 'k')
        ex = 0
        t(a, ex)

        yield r.hset('d', 'k', 'v')

        a = yield r.hexists('d', 'k')
        ex = 1
        t(a, ex)

        a = yield r.hget('d', 'k')
        ex = {'k' : 'v' }
        t(a, ex)
        a = yield r.hset('d', 'new', 'b', preserve=True)
        ex = 1
        t(a, ex)
        a = yield r.hset('d', 'new', 'b', preserve=True)
        ex = 0
        t(a, ex)
        yield r.hdelete('d', 'new')

        yield r.hset('d', 'f', 's')
        a = yield r.hgetall('d')
        ex = dict(k='v', f='s')
        t(a, ex)

        a = yield r.hgetall('foo')
        ex = {}
        t(a, ex)

        a = yield r.hget('d', 'notexist')
        ex = None
        t(a, ex)

        a = yield r.hlen('d')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_hincr(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        yield r.hset('d', 'k', 0)
        a = yield r.hincr('d', 'k')
        ex = 1
        t(a, ex)

        a = yield r.hincr('d', 'k')
        ex = 2
        t(a, ex)


    @defer.inlineCallbacks
    def test_hmget(self):
        r = self.redis
        t = self.assertEqual

        yield r.hdelete('d', 'k')
        yield r.hdelete('d', 'j')
        yield r.hset('d', 'k', 'v')
        yield r.hset('d', 'j', 'p')
        a = yield r.hget('d', ['k', 'j'])
        ex = {'k' : 'v', 'j' : 'p'}
        t(a, ex)


    @defer.inlineCallbacks
    def test_hmset(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        in_dict = dict(k='v', j='p')
        a = yield r.hmset('d', in_dict)
        ex = 'OK'
        t(a, ex)

        a = yield r.hgetall('d')
        ex = in_dict
        t(a, ex)

    @defer.inlineCallbacks
    def test_hkeys(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        in_dict = dict(k='v', j='p')
        yield r.hmset('d', in_dict)

        a = yield r.hkeys('d')
        ex = ['k', 'j']
        t(a, ex)

    @defer.inlineCallbacks
    def test_hvals(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        in_dict = dict(k='v', j='p')
        yield r.hmset('d', in_dict)

        a = yield r.hvals('d')
        ex = ['v', 'p']
        t(a, ex)


class LargeMultiBulk(CommandsTestBase):
    @defer.inlineCallbacks
    def test_large_multibulk(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        data = set(range(1, 10000))
        for i in data:
            r.sadd('s', i)
        res = yield r.smembers('s')
        t(res, data)

class SortedSet(CommandsTestBase):
    """Test commands that operate on sorted sets.
    """
    @defer.inlineCallbacks
    def test_basic(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        a = yield r.zadd('z', 'a', 1)
        ex = 1
        t(a, ex)
        yield r.zadd('z', 'b', 2.142)

        a = yield r.zrank('z', 'a')
        ex = 0
        t(a, ex)

        a = yield r.zrank('z', 'a', reverse=True)
        ex = 1
        t(a, ex)

        a = yield r.zcard('z')
        ex = 2
        t(a, ex)

        a = yield r.zscore('z', 'b')
        ex = 2.142
        t(a, ex)

        a = yield r.zrange('z', 0, -1, withscores=True)
        ex = [('a', 1), ('b', 2.142)]
        t(a, ex)

        a = yield r.zrem('z', 'a')
        ex = 1
        t(a, ex)


    @defer.inlineCallbacks
    def test_zrangebyscore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1.014)
        yield r.zadd('z', 'b', 4.252)
        yield r.zadd('z', 'c', 0.232)
        yield r.zadd('z', 'd', 10.425)
        a = yield r.zrangebyscore('z')
        ex = ['c', 'a', 'b', 'd']
        t(a, ex)

        a = yield r.zrangebyscore('z', offset=1, count=2)
        ex = ['a', 'b']
        t(a, ex)

        a = yield r.zrangebyscore('z', offset=1, count=2, withscores=True)
        ex = [('a', 1.014), ('b', 4.252)]
        t(a, ex)

        a = yield r.zrangebyscore('z', min=1, offset=1, count=2, withscores=True)
        ex = [('b', 4.252), ('d', 10.425)]
        t(a, ex)


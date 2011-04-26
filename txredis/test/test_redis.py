
import time

from twisted.internet import error
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.task import Clock
from twisted.test.proto_helpers import StringTransportWithDisconnection
from twisted.trial import unittest

from txredis.protocol import Redis, RedisSubscriber, RedisClientFactory
from txredis.protocol import ResponseError

REDIS_HOST = 'localhost'
REDIS_PORT = 6381


class CommandsTestBase(unittest.TestCase):
    protocol = Redis
    def setUp(self):
        clientCreator = protocol.ClientCreator(reactor, self.protocol)
        d = clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)
        def got_conn(redis):
            self.redis = redis
        d.addCallback(got_conn)
        def cannot_conn(res):
            msg = '\n' * 3 + '*' * 80 + '\n' * 2
            msg += "NOTE: Redis server not running on port %s. Please start a local instance of Redis " \
                  "on this port to run unit tests against.\n\n" % REDIS_PORT
            msg += '*' * 80 + '\n' * 4
            raise unittest.SkipTest(msg)
        d.addErrback(cannot_conn)
        return d

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
    def test_config(self):
        t = self.assertEqual
        a = yield self.redis.get_config('*')
        self.assertTrue(isinstance(a, dict))
        self.assertTrue('dbfilename' in a)

        a = yield self.redis.set_config('dbfilename', 'dump.rdb.tmp')
        ex = 'OK'
        t(a, ex)

        a = yield self.redis.get_config('dbfilename')
        self.assertTrue(isinstance(a, dict))
        t(a['dbfilename'], 'dump.rdb.tmp')

    """
    @defer.inlineCallbacks
    def test_auth(self):
        r = self.redis
        t = self.assertEqual

        # set a password
        password = 'foobar'
        a = yield self.redis.set_config('requirepass', password)
        ex = 'OK'
        t(a, ex)

        # auth with it
        a = yield self.redis.auth(password)
        ex = 'OK'
        t(a, ex)

        # turn password off
        a = yield self.redis.set_config('requirepass', '')
        ex = 'OK'
        t(a, ex)
    """

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
        self.assertTrue(a == None or a == 'none')

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
        a = yield isinstance((yield r.randomkey()), str)
        ex = True
        t(a, ex)

    def test_rename_same_src_dest(self):
        r = self.redis
        t = self.assertEqual
        d = r.rename('a', 'a')
        self.failUnlessFailure(d, ResponseError)
        def test_err(a):
            ex = ResponseError('ERR source and destination objects are the same')
            t(str(a), str(ex))
        d.addCallback(test_err)
        return d

    @defer.inlineCallbacks
    def test_rename(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.rename('a', 'b')
        ex = 'OK'
        t(a, ex)
        a = yield r.get('a')
        t(a, None)
        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.rename('b', 'a', preserve=True)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_dbsize(self):
        r = self.redis
        t = self.assertTrue
        a = yield r.dbsize()
        t(isinstance(a, int) or isinstance(a, long))

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
        # the following checks the expected response of an EXPIRE on a key with
        # an existing TTL. unfortunately the behaviour of redis changed in
        # v2.1.3 so we have to determine which behaviour to expect...
        info = yield r.info()
        redis_vern = tuple(map(int, info['redis_version'].split('.')))
        if redis_vern < (2, 1, 3):
            ex = 0
        else:
            ex = 1
        a = yield r.expire('q', 1)
        t(a, ex)

    @defer.inlineCallbacks
    def test_mset(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.mset({'ma': 1, 'mb': 2})
        ex = 'OK'
        t(a, ex)

        a = yield r.mset({'ma': 1, 'mb': 2}, preserve=True)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_substr(self):
        r = self.redis
        t = self.assertEqual

        string = 'This is a string'
        r.set('s', string)
        a = yield r.substr('s', 0, 3) # old name
        ex = 'This'
        t(a, ex)
        a = yield r.getrange('s', 0, 3) # new name
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
        a = yield r.expire('a', 0)
        ex = 1
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

    def test_lastsave(self):
        r = self.redis
        t = self.assertEqual

        tme = int(time.time())
        d = r.save()
        def done_save(a):
            ex = 'OK'
            t(a, ex)
            d = r.lastsave()
            def got_lastsave(a):
                a = a >= tme
                ex = True
                t(a, ex)
            d.addCallback(got_lastsave)
            return d

        def save_err(res):
            if 'Background save already in progress' in str(res):
                return True
            else:
                raise res
        d.addCallbacks(done_save, save_err)
        return d

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

    @defer.inlineCallbacks
    def test_multi(self):
        r = yield self.redis.multi()
        self.assertEqual(r, 'OK')

    @defer.inlineCallbacks
    def test_execute(self):
        # multi with two sets
        yield self.redis.multi()
        r = yield self.redis.set('foo', 'bar')
        self.assertEqual(r, 'QUEUED')
        r = yield self.redis.set('foo', 'barbar')
        self.assertEqual(r, 'QUEUED')
        r = yield self.redis.execute()
        self.assertEqual(r, ['OK', 'OK'])
        r = yield self.redis.get('foo')
        self.assertEqual(r, 'barbar')

    def test_discard(self):
        d = self.redis.execute()
        # discard without multi will return ResponseError
        d = self.failUnlessFailure(d, ResponseError)

        # multi with two sets
        def step1(_res):
            d = self.redis.set('foo', 'bar1')

            def step2(_res):
                d = self.redis.multi()
                def in_multi(_res):
                    d = self.redis.set('foo', 'bar2')
                    def step3(_res):
                        d = self.redis.discard()
                        def step4(r):
                            self.assertEqual(r, 'OK')
                            d = self.redis.get('foo')
                            def got_it(res):
                                self.assertEqual(res, 'bar1')
                            d.addCallback(got_it)
                            return d
                        d.addCallback(step4)
                        return d
                    d.addCallback(step3)
                    return d
                d.addCallback(in_multi)
                return d
            d.addCallback(step2)
            return d

        d.addCallback(step1)
        return d

class Strings(CommandsTestBase):
    """Test commands that operate on string values.
    """

    @defer.inlineCallbacks
    def test_blank(self):
        yield self.redis.set('a', "")

        r = yield self.redis.get('a')
        self.assertEquals("", r)

    @defer.inlineCallbacks
    def test_set(self):
        a = yield self.redis.set('a', 'pippo')
        self.assertEqual(a, 'OK')

        unicode_str = u'pippo \u3235'
        a = yield self.redis.set('a', unicode_str)
        self.assertEqual(a, 'OK')

        a = yield self.redis.get('a')
        self.assertEqual(a, unicode_str.encode('utf8'))

        a = yield self.redis.set('b', 105.2)
        self.assertEqual(a, 'OK')

        a = yield self.redis.set('b', 'xxx', preserve=True)
        self.assertEqual(a, 0)

        a = yield self.redis.get('b')
        self.assertEqual(a, '105.2')

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
        ex = '15'
        t(a, ex)

        a = yield r.get('d')
        ex = u'\\r\\n'
        t(a, ex)

        a = yield r.get('b')
        ex = '15'
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
        ex = [u'pippo', '15',
              u'\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n', u'\\r\\n']
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
    def test_blank_item(self):
        key = 'test:list'
        yield self.redis.delete(key)

        chars = ["a", "", "c"]
        for char in chars:
            yield self.redis.push(key, char)

        r = yield self.redis.lrange(key, 0, len(chars))
        self.assertEquals(["c", "", "a"], r)

    @defer.inlineCallbacks
    def test_concurrent(self):
        """Test ability to handle many large responses at the same time"""
        num_lists = 100
        items_per_list = 50

        # 1. Generate and fill lists
        lists = []
        for l in range(0, num_lists):
            key = 'list-%d' % l
            yield self.redis.delete(key)
            for i in range(0, items_per_list):
                yield self.redis.push(key, 'item-%d' % i)
            lists.append(key)

        # 2. Make requests to get all lists
        ds = []
        for key in lists:
            d = self.redis.lrange(key, 0, items_per_list)
            ds.append(d)

        # 3. Wait on all responses and make sure we got them all
        r = yield defer.DeferredList(ds)
        self.assertEquals(len(r), num_lists)

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
        ex = [u'bbb']
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'bbb', u'aaa']
        t(a, ex)
        a = yield r.lrange('l', -1, 0)
        ex = []
        t(a, ex)
        a = yield r.lrange('l', -1, -1)
        ex = [u'aaa']
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
        ex = u'aaa'
        t(a, ex)
        a = yield r.lindex('l', -1)
        ex = u'aaa'
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
        ex = u'bbb'
        t(a, ex)
        a = yield r.pop('l')
        ex = u'aaa'
        t(a, ex)
        yield r.pop('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.pop('l', tail=True)
        ex = u'aaa'
        t(a, ex)
        a = yield r.pop('l')
        ex = u'bbb'
        t(a, ex)
        a = yield r.pop('l')
        ex = None
        t(a, ex)

    def test_lset_on_nonexistant_key(self):
        r = self.redis
        t = self.assertEqual

        d = r.delete('l')
        def bad_lset(_res):
            d = r.lset('l', 0, 'a')
            self.failUnlessFailure(d, ResponseError)
            def match_err(a):
                ex = ResponseError('ERR no such key')
                t(str(a), str(ex))
            d.addCallback(match_err)
            return d
        d.addCallback(bad_lset)
        return d

    def test_lset_bad_range(self):
        r = self.redis
        t = self.assertEqual

        d = r.delete('l')
        def proceed(_res):
            d = r.push('l', 'aaa')
            def done_push(a):
                ex = 1
                t(a, ex)
                d = r.lset('l', 1, 'a')
                self.failUnlessFailure(d, ResponseError)
                def check(a):
                    ex = ResponseError('ERR index out of range')
                    t(str(a), str(ex))
                d.addCallback(check)
                return d
            d.addCallback(done_push)
            return d
        d.addCallback(proceed)
        return d

    @defer.inlineCallbacks
    def test_lset(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
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
    def test_blank(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sadd('s', "")
        ex = 1
        t(a, ex)
        a = yield r.smembers('s')
        ex = set([""])
        t(a, ex)

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
    def test_sdiff(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        yield r.delete('t')
        yield r.sadd('s', 'a')
        yield r.sadd('s', 'b')
        yield r.sadd('t', 'a')
        a = yield r.sdiff('s', 't')
        ex = ['b']
        t(a, ex)

        a = yield r.sdiffstore('c', 's', 't')
        ex = 1
        t(a, ex)

        a = yield r.scard('c')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_srandmember(self):
        r = self.redis

        yield r.delete('s')
        yield r.sadd('s', 'a')
        yield r.sadd('s', 'b')
        yield r.sadd('s', 'c')
        a = yield r.srandmember('s')
        self.assertTrue(a in set(['a', 'b', 'c']))

    @defer.inlineCallbacks
    def test_smove(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        yield r.delete('t')
        yield r.sadd('s', 'a')
        yield r.sadd('t', 'b')
        a = yield r.smove('s', 't', 'a')
        ex = 1
        t(a, ex)
        a = yield r.scard('s')
        ex = 0
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
    def test_sort_style(self):
        # considering, given that redis only stores strings, whether the sort it
        # provides is a numeric or a lexicographical sort; turns out that it's
        # numeric; i.e. redis is doing implicit type coercion for the sort of
        # numeric values.  This test serves to document that, and to a lesser
        # extent check for regression in the implicit str() marshalling of txredis
        r = self.redis
        t = self.assertEqual
        yield r.delete('l')
        items = [ 007, 10, -5, 0.1, 100, -3, 20, 0.02, -3.141 ]
        for i in items:
            yield r.push('l', i, tail=True)
        a = yield r.sort('l')
        ex = map(str, sorted(items))
        t(a, ex)

    @defer.inlineCallbacks
    def test_sort(self):
        r = self.redis
        t = self.assertEqual
        s = lambda l: map(str, l)

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
            yield r.push('l', 1.0 / i, tail=True)
        a = yield r.sort('l')
        ex = s([0.25, 0.333333333333, 0.5, 1.0])
        t(a, ex)
        a = yield r.sort('l', desc=True)
        ex = s([1.0, 0.5, 0.333333333333, 0.25])
        t(a, ex)
        a = yield r.sort('l', desc=True, start=2, num=1)
        ex = s([0.333333333333])
        t(a, ex)
        a = yield r.set('weight_0.5', 10)
        ex = 'OK'
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*')
        ex = s([0.5, 1.0, 0.333333333333, 0.25])
        t(a, ex)
        for i in (yield r.sort('l', desc=True)):
            yield r.set('test_%s' % i, 100 - float(i))
        a = yield r.sort('l', desc=True, get='test_*')
        ex = s([99.0, 99.5, 99.6666666667, 99.75])
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*', get='test_*')
        ex = s([99.5, 99.0, 99.6666666667, 99.75])
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
            t(rval, str(value))


class Hash(CommandsTestBase):
    """Test commands that operate on hashes.
    """

    @defer.inlineCallbacks
    def test_blank(self):
        yield self.redis.delete('h')
        yield self.redis.hset('h', 'blank', "")
        a = yield self.redis.hget('h', 'blank')
        self.assertEquals(a, '')
        a = yield self.redis.hgetall('h')
        self.assertEquals(a, {'blank': ''})

    @defer.inlineCallbacks
    def test_cas(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('h')
        ex = 1
        t(a, ex)

        a = yield r.hsetnx('h', 'f', 'v')
        ex = 1
        t(a, ex)

        a = yield r.hsetnx('h', 'f', 'v')
        ex = 0
        t(a, ex)


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
        ex = {'k': 'v'}
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
    def test_hget(self):
        r = self.redis
        t = self.assertEqual

        yield r.hdelete('key', 'field')
        yield r.hset('key', 'field', 'value1')
        a = yield r.hget('key', 'field')
        ex = {'field': 'value1'}
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
        ex = {'k': 'v', 'j': 'p'}
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
        data = set(xrange(1, 100000))
        for i in data:
            r.sadd('s', i)
        res = yield r.smembers('s')
        t(res, set(map(str, data)))


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
    def test_zcount(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1)
        yield r.zadd('z', 'b', 2)
        yield r.zadd('z', 'c', 3)
        yield r.zadd('z', 'd', 4)
        a = yield r.zcount('z', 1, 3)
        ex = 3
        t(a, ex)


    @defer.inlineCallbacks
    def test_zremrange(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1.0)
        yield r.zadd('z', 'b', 2.0)
        yield r.zadd('z', 'c', 3.0)
        yield r.zadd('z', 'd', 4.0)

        a = yield r.zremrangebyscore('z', 1.0, 3.0)
        ex = 3
        t(a, ex)

        yield r.zadd('z', 'a', 1.0)
        yield r.zadd('z', 'b', 2.0)
        yield r.zadd('z', 'c', 3.0)
        a = yield r.zremrangebyrank('z', 0, 2)
        ex = 3
        t(a, ex)

    @defer.inlineCallbacks
    def test_zrangebyscore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        a = yield r.zrangebyscore('z', -1, -1, withscores=True)
        ex = []
        t(a, ex)

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

    @defer.inlineCallbacks
    def test_zscore_and_zrange_nonexistant(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('a')
        a = yield r.zscore('a', 'somekey')
        t(a, None)

        yield r.delete('a')
        a = yield r.zrange('a', 0, -1, withscores=True)
        t(a, [])


    @defer.inlineCallbacks
    def test_zaggregatestore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('a')
        yield r.delete('b')
        yield r.delete('t')

        yield r.zadd('a', 'a', 1.0)
        yield r.zadd('a', 'b', 2.0)
        yield r.zadd('a', 'c', 3.0)
        yield r.zadd('b', 'a', 1.0)
        yield r.zadd('b', 'b', 2.0)
        yield r.zadd('b', 'c', 3.0)

        a = yield r.zunionstore('t', ['a', 'b'])
        ex = 3
        t(a, ex)

        a = yield r.zscore('t', 'a')
        ex = 2
        t(a, ex)

        yield r.delete('t')
        a = yield r.zunionstore('t', {'a' : 2.0, 'b' : 2.0})
        ex = 3
        t(a, ex)

        a = yield r.zscore('t', 'a')
        ex = 4
        t(a, ex)

        yield r.delete('t')
        a = yield r.zunionstore('t', {'a' : 2.0, 'b' : 2.0}, aggregate='MAX')
        ex = 3
        t(a, ex)

        a = yield r.zscore('t', 'a')
        ex = 2
        t(a, ex)

        yield r.delete('t')
        a = yield r.zinterstore('t', {'a' : 2.0, 'b' : 2.0}, aggregate='MAX')
        ex = 3
        t(a, ex)


class BlockingListOperartions(CommandsTestBase):
    """@todo test timeout
    @todo robustly test async/blocking redis commands
    """

    @defer.inlineCallbacks
    def test_bpop_noblock(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('test.list.a')
        yield r.delete('test.list.b')
        yield r.push('test.list.a', 'stuff')
        yield r.push('test.list.a', 'things')
        yield r.push('test.list.b', 'spam')
        yield r.push('test.list.b', 'bee')
        yield r.push('test.list.b', 'honey')

        a = yield r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.a', 'things']
        t(a, ex)
        a = yield r.bpop(['test.list.b', 'test.list.a'])
        ex = ['test.list.b', 'honey']
        t(a, ex)
        a = yield r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.a', 'stuff']
        t(a, ex)
        a = yield r.bpop(['test.list.b', 'test.list.a'])
        ex = ['test.list.b', 'bee']
        t(a, ex)
        a = yield r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.b', 'spam']
        t(a, ex)

    @defer.inlineCallbacks
    def test_bpop_block(self):
        r = self.redis
        t = self.assertEqual

        clientCreator = protocol.ClientCreator(reactor, Redis)
        r2 = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)

        def _cb(reply, ex):
            t(reply, ex)

        yield r.delete('test.list.a')
        yield r.delete('test.list.b')

        d = r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.a', 'stuff']
        d.addCallback(_cb, ex)

        yield r2.push('test.list.a', 'stuff')

        yield d
        r2.transport.loseConnection()


# if hiredis and its python wrappers are installed, test them too
try:
    import hiredis
    from txredis.protocol import HiRedisProtocol
    class HiRedisGeneral(General):
        protcol = HiRedisProtocol
    class HiRedisStrings(Strings):
        protocol = HiRedisProtocol
    class HiRedisLists(Lists):
        protocol = HiRedisProtocol
    class HiRedisHash(Hash):
        protocol = HiRedisProtocol
    class HiRedisSortedSet(SortedSet):
        protocol = HiRedisProtocol
    class HiRedisSets(Sets):
        protocol = HiRedisProtocol
    _hush_pyflakes = hiredis
    del _hush_pyflakes
except ImportError:
    pass

class Network(unittest.TestCase):

    def setUp(self):
        self.proto = Redis()
        self.clock = Clock()
        self.proto.callLater = self.clock.callLater
        self.transport = StringTransportWithDisconnection()
        self.transport.protocol = self.proto
        self.proto.makeConnection(self.transport)

    def test_request_while_disconnected(self):
        # fake disconnect
        self.proto._disconnected = True

        d = self.proto.get('foo')
        self.assertFailure(d, RuntimeError)

        def checkMessage(error):
            self.assertEquals(str(error), 'Not connected')

        return d.addCallback(checkMessage)

    def test_disconnect_during_request(self):
        d1 = self.proto.get("foo")
        d2 = self.proto.get("bar")
        self.assertEquals(len(self.proto._request_queue), 2)

        self.transport.loseConnection()
        done = defer.DeferredList([d1, d2], consumeErrors=True)

        def checkFailures(results):
            self.assertEquals(len(self.proto._request_queue), 0)
            for success, result in results:
                self.assertFalse(success)
                result.trap(error.ConnectionDone)

        return done.addCallback(checkFailures)



class Protocol(unittest.TestCase):

    def setUp(self):
        self.proto = Redis()
        self.transport = StringTransportWithDisconnection()
        self.transport.protocol = self.proto
        self.proto.makeConnection(self.transport)

    def sendResponse(self, data):
        self.proto.dataReceived(data)

    def test_error_response(self):
        # pretending 'foo' is a set, so get is incorrect
        d = self.proto.get("foo")
        self.assertEquals(self.transport.value(), '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
        msg = "Operation against a key holding the wrong kind of value"
        self.sendResponse("-%s\r\n" % msg)
        self.failUnlessFailure(d, ResponseError)
        def check_err(r):
            self.assertEquals(str(r), msg)
        return d

    @defer.inlineCallbacks
    def test_singleline_response(self):
        d = self.proto.ping()
        self.assertEquals(self.transport.value(), '*1\r\n$4\r\nPING\r\n')
        self.sendResponse("+PONG\r\n")
        r = yield d
        self.assertEquals(r, 'PONG')

    @defer.inlineCallbacks
    def test_bulk_response(self):
        d = self.proto.get("foo")
        self.assertEquals(self.transport.value(), '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
        self.sendResponse("$3\r\nbar\r\n")
        r = yield d
        self.assertEquals(r, 'bar')

    @defer.inlineCallbacks
    def test_multibulk_response(self):
        d = self.proto.lrange("foo", 0, 1)
        expected = '*4\r\n$6\r\nLRANGE\r\n$3\r\nfoo\r\n$1\r\n0\r\n$1\r\n1\r\n'
        self.assertEquals(self.transport.value(), expected)
        self.sendResponse("*2\r\n$3\r\nbar\r\n$6\r\nlolwut\r\n")
        r = yield d
        self.assertEquals(r, ['bar', 'lolwut'])

    @defer.inlineCallbacks
    def test_integer_response(self):
        d = self.proto.dbsize()
        self.assertEquals(self.transport.value(), '*1\r\n$6\r\nDBSIZE\r\n')
        self.sendResponse(":1234\r\n")
        r = yield d
        self.assertEquals(r, 1234)

class TestFactory(CommandsTestBase):
    def setUp(self):
        d = CommandsTestBase.setUp(self)
        def do_setup(_res):
            self.factory = RedisClientFactory()
            reactor.connectTCP(REDIS_HOST, REDIS_PORT, self.factory)
            d = self.factory.deferred
            def cannot_connect(_res):
                raise unittest.SkipTest('Cannot connect to Redis.')
            d.addErrback(cannot_connect)
            return d
        d.addCallback(do_setup)
        return d

    def tearDown(self):
        CommandsTestBase.tearDown(self)
        self.factory.continueTrying = 0
        self.factory.stopTrying()
        if self.factory.client:
            self.factory.client.setTimeout(None)
            self.factory.client.transport.loseConnection()

    @defer.inlineCallbacks
    def test_reconnect(self):
        a = yield self.factory.client.info()
        self.assertTrue('uptime_in_days' in a)

        # teardown the connection
        self.factory.client.transport.loseConnection()

        # wait until reconnected
        a = yield self.factory.deferred

        a = yield self.factory.client.info()
        self.assertTrue('uptime_in_days' in a)
    timeout = 4


class ProtocolBuffering(Protocol):

    def sendResponse(self, data):
        """Send a response one character at a time to test buffering"""
        for char in data:
            self.proto.dataReceived(char)


class PubSub(CommandsTestBase):

    @defer.inlineCallbacks
    def setUp(self):
        yield CommandsTestBase.setUp(self)

        class TestSubscriber(RedisSubscriber):

            def __init__(self, *args, **kwargs):
                RedisSubscriber.__init__(self, *args, **kwargs)
                self.msg_channel = None
                self.msg_message = None
                self.msg_received = defer.Deferred()
                self.channel_subscribed = defer.Deferred()

            def messageReceived(self, channel, message):
                self.msg_channel = channel
                self.msg_message = message
                self.msg_received.callback(None)
                self.msg_received = defer.Deferred()

            def channelSubscribed(self, channel, numSubscriptions):
                self.channel_subscribed.callback(None)
                self.channel_subscribed = defer.Deferred()
            channelUnsubscribed = channelSubscribed
            channelPatternSubscribed = channelSubscribed
            channelPatternUnsubscribed = channelSubscribed

        clientCreator = protocol.ClientCreator(reactor, TestSubscriber)
        self.subscriber = yield clientCreator.connectTCP(REDIS_HOST,
                                                         REDIS_PORT)

    def tearDown(self):
        CommandsTestBase.tearDown(self)
        self.subscriber.transport.loseConnection()

    @defer.inlineCallbacks
    def test_subscribe(self):
        s = self.subscriber
        t = self.assertEqual

        cb = s.channel_subscribed
        yield s.subscribe("channelA")
        yield cb

        cb = s.msg_received
        a = yield self.redis.publish("channelA", "dataB")
        ex = 1
        t(a, ex)
        yield cb
        a = s.msg_channel
        ex = "channelA"
        t(a, ex)
        a = s.msg_message
        ex = "dataB"
        t(a, ex)

    @defer.inlineCallbacks
    def test_unsubscribe(self):
        s = self.subscriber

        cb = s.channel_subscribed
        yield s.subscribe("channelA", "channelB", "channelC")
        yield cb

        cb = s.channel_subscribed
        yield s.unsubscribe("channelA", "channelC")
        yield cb

        yield s.unsubscribe()

    @defer.inlineCallbacks
    def test_psubscribe(self):
        s = self.subscriber
        t = self.assertEqual

        cb = s.channel_subscribed
        yield s.psubscribe("channel*", "magic*")
        yield cb

        cb = s.msg_received
        a = yield self.redis.publish("channelX", "dataC")
        ex = 1
        t(a, ex)
        yield cb
        a = s.msg_channel
        ex = "channelX"
        t(a, ex)
        a = s.msg_message
        ex = "dataC"
        t(a, ex)

    @defer.inlineCallbacks
    def test_punsubscribe(self):
        s = self.subscriber

        cb = s.channel_subscribed
        yield s.psubscribe("channel*", "magic*", "woot*")
        yield cb

        cb = s.channel_subscribed
        yield s.punsubscribe("channel*", "woot*")
        yield cb
        yield s.punsubscribe()


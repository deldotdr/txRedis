
import decimal

from twisted.internet import protocol 
from twisted.internet import reactor
from twisted.internet import defer
from twisted.trial import unittest

from txredis.protocol import Redis

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

class RedisTestBase(unittest.TestCase):


    @defer.inlineCallbacks
    def setUp(self):
        clientCreator = protocol.ClientCreator(reactor, Redis)
        self.redis = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)

    def tearDown(self):
        self.redis.transport.loseConnection()

    @defer.inlineCallbacks
    def test_ping(self):
        a = yield self.redis.ping()
        self.assertEqual(a, 'PONG')

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
        self.assertEqual(a, decimal.Decimal("105.2"))


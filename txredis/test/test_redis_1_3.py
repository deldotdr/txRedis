
from twisted.internet import protocol 
from twisted.internet import reactor
from twisted.internet import defer

from txredis.protocol import Redis, RedisSubscriber
from txredis.protocol import ResponseError

from test_redis import CommandsTestBase

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

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
        ex = ['test.list.a', 'stuff'] 
        t(a, ex)

        a = yield r.bpop(['test.list.a', 'test.list.b'], tail=True)
        ex = ['test.list.a', 'things'] 
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
        # d.addErrback(_eb)

        info = yield r2.info()

        yield r2.push('test.list.a', 'stuff')

        r2.transport.loseConnection()


class SubscribePublish(CommandsTestBase):

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
        self.subscriber = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)

    def tearDown(self):
        CommandsTestBase.tearDown(self)
        self.subscriber.transport.loseConnection()

    @defer.inlineCallbacks
    def test_subscribe(self):
        r = self.redis
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
        r = self.redis
        s = self.subscriber
        t = self.assertEqual

        cb = s.channel_subscribed
        yield s.subscribe("channelA", "channelB", "channelC")
        yield cb

        cb = s.channel_subscribed
        yield s.unsubscribe("channelA", "channelC")
        yield cb

        yield s.unsubscribe()

    @defer.inlineCallbacks
    def test_psubscribe(self):
        r = self.redis
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
        r = self.redis
        s = self.subscriber
        t = self.assertEqual

        cb = s.channel_subscribed
        yield s.psubscribe("channel*", "magic*", "woot*")
        yield cb

        cb = s.channel_subscribed
        yield s.punsubscribe("channel*", "woot*")
        yield cb

        yield s.punsubscribe()


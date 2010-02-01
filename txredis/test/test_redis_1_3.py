
from twisted.internet import protocol 
from twisted.internet import reactor
from twisted.internet import defer

from txredis.protocol import Redis
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





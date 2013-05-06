from twisted.internet import reactor, protocol, defer
from twisted.python import log

from txredis.protocol import Redis, RedisSubscriber

import sys


REDIS_HOST = 'localhost'
REDIS_PORT = 6379


class Subscriber(RedisSubscriber):

    num_channels = 0

    def channelSubscribed(self, channel, numSubscribed):
        self.num_channels += 1

    def channelUnsubscribed(self, channel, numSubscribed):
        self.num_channels -= 1

    def messageReceived(self, channel, message):
        log.msg("got message %r on channel %s" % (message, channel))


@defer.inlineCallbacks
def getRedisSubscriber():
    clientCreator = protocol.ClientCreator(reactor, Subscriber)
    redis = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)
    defer.returnValue(redis)


@defer.inlineCallbacks
def getRedis():
    clientCreator = protocol.ClientCreator(reactor, Redis)
    redis = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)
    defer.returnValue(redis)


@defer.inlineCallbacks
def runTest():
    redis1 = yield getRedisSubscriber()
    redis2 = yield getRedis()

    log.msg("redis1: SUBSCRIBE w00t")
    response = yield redis1.subscribe("w00t")
    log.msg("subscribed to w00t, response = %r" % response)

    while redis1.num_channels == 0:
        d = defer.Deferred()
        reactor.callLater(0.1, d.callback, True)
        yield d

    log.msg("redis2: PUBLISH w00t 'Hello, world!'")
    response = yield redis2.publish("w00t", "Hello, world!")
    log.msg("published to w00t, response = %r" % response)


def main():
    log.startLogging(sys.stdout)
    reactor.callLater(0, runTest)
    reactor.run()


if __name__ == "__main__":
    main()
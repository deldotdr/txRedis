from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import defer

from txredis.client import HiRedisClient

# Hostname and Port number of a redis server
HOST = 'localhost'
PORT = 6379


@defer.inlineCallbacks
def main():
    clientCreator = protocol.ClientCreator(reactor, HiRedisClient)
    redis = yield clientCreator.connectTCP(HOST, PORT)

    res = yield redis.ping()
    print res

    info = yield redis.info()
    print info

    res = yield redis.set('test', 42)
    print res

    test = yield redis.get('test')
    print test

    reactor.stop()

if __name__ == "__main__":
    main()
    reactor.run()

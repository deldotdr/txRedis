from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import defer

from txredis.protocol import Redis

# Hostname and Port number of a redis server
HOST = 'localhost'
PORT = 6379

@defer.inlineCallbacks
def main():
    clientCreator = protocol.ClientCreator(reactor, Redis)
    redis = yield clientCreator.connectTCP(HOST, PORT)
    
    res = yield redis.ping()
    print res

    info = yield redis.info()
    print info

    res = yield redis.set('test', 42)
    print res
    
    test = yield redis.get('test')
    print test

if __name__ == "__main__":
    main()
    reactor.run()

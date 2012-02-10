"""
@file testing.py

This module provides the basic needs to run txRedis unit tests.
"""
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.trial import unittest

from txredis.client import Redis


REDIS_HOST = 'localhost'
REDIS_PORT = 6381


class CommandsBaseTestCase(unittest.TestCase):

    protocol = Redis

    def setUp(self):

        def got_conn(redis):
            self.redis = redis

        def cannot_conn(res):
            msg = '\n' * 3 + '*' * 80 + '\n' * 2
            msg += ("NOTE: Redis server not running on port %s. Please start "
                    "a local instance of Redis on this port to run unit tests "
                    "against.\n\n") % REDIS_PORT
            msg += '*' * 80 + '\n' * 4
            raise unittest.SkipTest(msg)

        clientCreator = protocol.ClientCreator(reactor, self.protocol)
        d = clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)
        d.addCallback(got_conn)
        d.addErrback(cannot_conn)
        return d

    def tearDown(self):
        self.redis.transport.loseConnection()

# if hiredis and its python wrappers are installed, test them too
try:
    import hiredis

    from txredis.protocol import HiRedisProtocol
    from txredis.tests import test_client

    isHiRedis = True

except ImportError:
    isHiRedis = False


if isHiRedis:

    class HiRedisGeneral(test_client.GeneralCommandTestCase):

        protcol = HiRedisProtocol


    class HiRedisStrings(test_client.StringsCommandTestCase):

        protocol = HiRedisProtocol


    class HiRedisLists(test_client.ListsCommandsTestCase):

        protocol = HiRedisProtocol


    class HiRedisHash(test_client.HashCommandsTestCase):

        protocol = HiRedisProtocol


    class HiRedisSortedSet(test_client.SortedSetCommandsTestCase):

        protocol = HiRedisProtocol


    class HiRedisSets(test_client.SetsCommandsTestCase):

        protocol = HiRedisProtocol


    _hush_pyflakes = hiredis
    del _hush_pyflakes

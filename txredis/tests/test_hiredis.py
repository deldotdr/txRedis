# if hiredis and its python wrappers are installed, test them too
try:
    import hiredis
    isHiRedis = True

except ImportError:
    isHiRedis = False

from txredis.client import HiRedisClient
from txredis.tests import test_client


if isHiRedis:

    class HiRedisGeneral(test_client.GeneralCommandTestCase):
        protcol = HiRedisClient

    class HiRedisStrings(test_client.StringsCommandTestCase):
        protocol = HiRedisClient

    class HiRedisLists(test_client.ListsCommandsTestCase):
        protocol = HiRedisClient

    class HiRedisHash(test_client.HashCommandsTestCase):
        protocol = HiRedisClient

    class HiRedisSortedSet(test_client.SortedSetCommandsTestCase):
        protocol = HiRedisClient

    class HiRedisSets(test_client.SetsCommandsTestCase):
        protocol = HiRedisClient

    _hush_pyflakes = hiredis
    del _hush_pyflakes

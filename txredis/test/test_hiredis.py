from txredis.test import test_redis

# if hiredis and its python wrappers are installed, test them too
try:
    import hiredis
    from txredis.protocol import HiRedisProtocol
    class HiRedisGeneral(test_redis.General):
        protcol = HiRedisProtocol
    class HiRedisStrings(test_redis.Strings):
        protocol = HiRedisProtocol
    class HiRedisLists(test_redis.Lists):
        protocol = HiRedisProtocol
    class HiRedisHash(test_redis.Hash):
        protocol = HiRedisProtocol
    class HiRedisSortedSet(test_redis.SortedSet):
        protocol = HiRedisProtocol
    class HiRedisSets(test_redis.Sets):
        protocol = HiRedisProtocol
    _hush_pyflakes = hiredis
    del _hush_pyflakes
except ImportError:
    pass

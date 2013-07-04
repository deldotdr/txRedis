# txRedis
### Asynchronous [Redis](http://redis.io) client for [Twisted Python](http://www.twistedmatrix.com).


## Install

Install via [pip](https://pypi.python.org/pypi/pip). Usage examples can be found in the examples/ directory of this repository.

> pip install txredis

Included are two protocol implementations, one using a custom Twisted
based protocol parser, and another using the [hiredis](https://github.com/pietern/hiredis-py) protocol parser. If you would like to use hiredis, simply install it via pip and use the provided `HiRedisClient` protocol:

> pip install -U hiredis


## Bugs
File bug reports and any other feedback with the issue tracker at
http://github.com/deldotdr/txRedis/issues/


## Contributing
Please open a pull request at http://github.com/deldotdr/txRedis and be sure to include tests.


## Contact
There is no a txRedis list but questions can be raised in the [issues area](https://github.com/deldotdr/txRedis/issues) or in the [Redis community](http://redis.io/community).

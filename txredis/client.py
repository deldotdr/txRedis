"""
@file client.py
"""
import itertools

from twisted.internet import defer
from twisted.internet.protocol import ReconnectingClientFactory

try:
    import hiredis
except ImportError:
    pass

from txredis import exceptions
from txredis.protocol import RedisBase, HiRedisBase


class RedisClient(RedisBase):
    """The main Redis client."""

    def __init__(self, *args, **kwargs):
        RedisBase.__init__(self, *args, **kwargs)

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # REDIS COMMANDS
    #
    def ping(self):
        """
        Test command. Expect PONG as a reply.
        """
        self._send('PING')
        return self.getResponse()

    def shutdown(self):
        """
        Synchronously save the dataset to disk and then shut down the server
        """
        self._send('SHUTDOWN')
        return self.getResponse()

    def slaveof(self, host, port):
        """
        Make the server a slave of another instance, or promote it as master

        The SLAVEOF command can change the replication settings of a slave on
        the fly. If a Redis server is arleady acting as slave, the command
        SLAVEOF NO ONE will turn off the replicaiton turning the Redis server
        into a MASTER. In the proper form SLAVEOF hostname port will make the
        server a slave of the specific server listening at the specified
        hostname and port.

        If a server is already a slave of some master, SLAVEOF hostname port
        will stop the replication against the old server and start the
        synchrnonization against the new one discarding the old dataset.

        The form SLAVEOF no one will stop replication turning the server into a
        MASTER but will not discard the replication. So if the old master stop
        working it is possible to turn the slave into a master and set the
        application to use the new master in read/write. Later when the other
        Redis server will be fixed it can be configured in order to work as
        slave.
        """
        self._send('SLAVEOF', host, port)
        return self.getResponse()

    def get_config(self, pattern):
        """
        Get configuration for Redis at runtime.
        """
        self._send('CONFIG', 'GET', pattern)

        def post_process(values):
            # transform into dict
            res = {}
            if not values:
                return res
            for i in xrange(0, len(values) - 1, 2):
                res[values[i]] = values[i + 1]
            return res
        return self.getResponse().addCallback(post_process)

    def set_config(self, parameter, value):
        """
        Set configuration at runtime.
        """
        self._send('CONFIG', 'SET', parameter, value)
        return self.getResponse()

    # Commands operating on string values
    def set(self, key, value, preserve=False, getset=False, expire=None):
        """
        Set the string value of a key
        """
        # The following will raise an error for unicode values that can't be
        # encoded to ascii. We could probably add an 'encoding' arg to init,
        # but then what do we do with get()? Convert back to unicode? And what
        # about ints, or pickled values?
        if getset:
            command = 'GETSET'
        elif preserve:
            return self.setnx(key, value)
        else:
            command = 'SET'

        if expire:
            self._send('SETEX', key, expire, value)
        else:
            self._send(command, key, value)
        return self.getResponse()

    def setnx(self, key, value):
        """
        Set key to hold string value if key does not exist. In that case, it is
        equal to SET. When key already holds a value, no operation is
        performed. SETNX is short for "SET if Not eXists".
        """
        self._send('SETNX', key, value)
        return self.getResponse()

    def msetnx(self, mapping):
        """
        Sets the given keys to their respective values. MSETNX will not perform
        any operation at all even if just a single key already exists.

        Because of this semantic MSETNX can be used in order to set different
        keys representing different fields of an unique logic object in a way
        that ensures that either all the fields or none at all are set.

        MSETNX is atomic, so all given keys are set at once. It is not possible
        for clients to see that some of the keys were updated while others are
        unchanged.
        """

        self._send('msetnx', *list(itertools.chain(*mapping.iteritems())))
        return self.getResponse()

    def mset(self, mapping, preserve=False):
        """
        Set multiple keys to multiple values
        """
        if preserve:
            command = 'MSETNX'
        else:
            command = 'MSET'
        self._send(command, *list(itertools.chain(*mapping.iteritems())))
        return self.getResponse()

    def append(self, key, value):
        """
        Append a value to a key
        """
        self._send('APPEND', key, value)
        return self.getResponse()

    def getrange(self, key, start, end):
        """
        Get a substring of the string stored at a key
        """
        self._send('GETRANGE', key, start, end)
        return self.getResponse()
    substr = getrange

    def get(self, key):
        """
        Get the value of a key
        """
        self._send('GET', key)
        return self.getResponse()

    def getset(self, key, value):
        """
        Set the string value of a key and return its old value
        """
        return self.set(key, value, getset=True)

    def mget(self, *args):
        """
        Get the values of all the given keys
        """
        self._send('MGET', *args)
        return self.getResponse()

    def incr(self, key, amount=1):
        """
        Increment the integer value of a key by the given amount (default 1)
        """
        if amount == 1:
            self._send('INCR', key)
        else:
            self._send('INCRBY', key, amount)
        return self.getResponse()

    def decr(self, key, amount=1):
        """
        Decrement the integer value of a key by the given amount (default 1)
        """
        if amount == 1:
            self._send('DECR', key)
        else:
            self._send('DECRBY', key, amount)
        return self.getResponse()

    def exists(self, key):
        """
        Determine if a key exists
        """
        self._send('EXISTS', key)
        return self.getResponse()

    def delete(self, key, *keys):
        """
        Delete one or more keys
        """
        self._send('DEL', key, *keys)
        return self.getResponse()

    def get_type(self, key):
        """
        Determine the type stored at key
        """
        self._send('TYPE', key)
        return self.getResponse()

    def get_object(self, key, refcount=False, encoding=False, idletime=False):
        """
        Inspect the internals of Redis objects.
        @param key : The Redis key you want to inspect
        @param refcount: Returns the number of refereces of the value
                         associated with the specified key.
        @param encoding: Returns the kind of internal representation for
                         value.
        @param idletime: Returns the number of seconds since the object stored
                         at the specified key is idle. (Currently the actual
                         resolution is 10 seconds.)
        """
        subcommand = ''
        if idletime:
            subcommand = 'IDLETIME'
        elif encoding:
            subcommand = 'ENCODING'
        elif refcount:
            subcommand = 'REFCOUNT'
        if not subcommand:
            raise exceptions.InvalidCommand('Need a subcommand')
        self._send('OBJECT', subcommand, key)
        return self.getResponse()

    # Bit operations
    def getbit(self, key, offset):
        """
        Returns the bit value at offset in the string value stored at key.

        @param key: The Redis key to get bit from.
        @param offset: The offset to get bit from.
        """
        self._send('GETBIT', key, offset)
        return self.getResponse()

    def setbit(self, key, offset, value):
        """
        Sets the bit value at offset in the string value stored at key.

        @param key: The Redis key to set bit on.
        @param offset: The offset for the bit to set.
        @param value: The bit value (0 or 1)
        """
        self._send('SETBIT', key, offset, value)
        return self.getResponse()

    def bitcount(self, key, start=None, end=None):
        """
        Count the number of set bits (population counting) in a string.

        @param key: The Redis key to get bit count from.
        @param start: Optional starting index for bytes to scan (inclusive)
        @param end: Optional ending index for bytes to scan (inclusive).
                    End index is required when start is given.
        """
        start_end = []
        if start is not None:
            start_end.append(start)
            start_end.append(end)
        self._send('BITCOUNT', key, *start_end)
        return self.getResponse()

    # Commands operating on the key space
    def keys(self, pattern):
        """
        Find all keys matching the given pattern
        """
        self._send('KEYS', pattern)

        def post_process(res):
            if res is not None:
                # XXX is sort ok?
                res.sort()
            else:
                res = []
            return res

        return self.getResponse().addCallback(post_process)

    def randomkey(self):
        """
        Return a random key from the keyspace
        """
        #raise NotImplementedError("Implemented but buggy, do not use.")
        self._send('RANDOMKEY')
        return self.getResponse()

    def rename(self, src, dst, preserve=False):
        """
        Rename a key
        """
        self._send('RENAMENX' if preserve else 'RENAME', src, dst)
        return self.getResponse()

    def dbsize(self):
        """
        Return the number of keys in the selected database
        """
        self._send('DBSIZE')
        return self.getResponse()

    def expire(self, key, time):
        """
        Set a key's time to live in seconds
        """
        self._send('EXPIRE', key, time)
        return self.getResponse()

    def expireat(self, key, time):
        """
        Set the expiration for a key as a UNIX timestamp
        """
        self._send('EXPIREAT', key, time)
        return self.getResponse()

    def ttl(self, key):
        """
        Get the time to live for a key
        """
        self._send('TTL', key)
        return self.getResponse()

    # transaction commands:
    def multi(self):
        """
        Mark the start of a transaction block
        """
        self._send('MULTI')
        return self.getResponse()

    def execute(self):
        """
        Sends the EXEC command

        Called execute because exec is a reserved word in Python.
        """
        self._send('EXEC')
        return self.getResponse()

    def discard(self):
        """
        Discard all commands issued after MULTI
        """
        self._send('DISCARD')
        return self.getResponse()

    def watch(self, *keys):
        """
        Watch the given keys to determine execution of the MULTI/EXEC block
        """
        self._send('WATCH', *keys)
        return self.getResponse()

    def unwatch(self):
        """
        Forget about all watched keys
        """
        self._send('UNWATCH')
        return self.getResponse()

    # # # # # # # # #
    # List Commands:
    # RPUSH
    # LPUSH
    # RPUSHX
    # LPUSHX
    # LLEN
    # LRANGE
    # LTRIM
    # LINDEX
    # LSET
    # LREM
    # LPOP
    # RPOP
    # BLPOP
    # BRPOP
    # RPOPLPUSH
    # SORT
    def push(self, key, value, tail=False, no_create=False):
        """
        @param key Redis key
        @param value String element of list

        Add the string value to the head (LPUSH/LPUSHX) or tail
        (RPUSH/RPUSHX) of the list stored at key key. If the key does
        not exist and no_create is False (the default) an empty list
        is created just before the append operation. If the key exists
        but is not a List an error is returned.

        @note Time complexity: O(1)
        """
        if tail:
            if no_create:
                return self.rpushx(key, value)
            else:
                return self.rpush(key, value)
        else:
            if no_create:
                return self.lpushx(key, value)
            else:
                return self.lpush(key, value)

    def lpush(self, key, *values, **kwargs):
        """
        Add string to head of list.
        @param key : List key
        @param values : Sequence of values to push
        @param value : For backwards compatibility, a single value.
        """
        if not kwargs:
            self._send('LPUSH', key, *values)
        elif 'value' in kwargs:
            self._send('LPUSH', key, kwargs['value'])
        else:
            raise exceptions.InvalidCommand('Need arguments for LPUSH')
        return self.getResponse()

    def rpush(self, key, *values, **kwargs):
        """
        Add string to end of list.
        @param key : List key
        @param values : Sequence of values to push
        @param value : For backwards compatibility, a single value.
        """
        if not kwargs:
            self._send('RPUSH', key, *values)
        elif 'value' in kwargs:
            self._send('RPUSH', key, kwargs['value'])
        else:
            raise exceptions.InvalidCommand('Need arguments for RPUSH')
        return self.getResponse()

    def lpushx(self, key, value):
        self._send('LPUSHX', key, value)
        return self.getResponse()

    def rpushx(self, key, value):
        self._send('RPUSHX', key, value)
        return self.getResponse()

    def llen(self, key):
        """
        @param key Redis key

        Return the length of the list stored at the key key. If the
        key does not exist zero is returned (the same behavior as for
        empty lists). If the value stored at key is not a list an error is
        returned.

        @note Time complexity: O(1)
        """
        self._send('LLEN', key)
        return self.getResponse()

    def lrange(self, key, start, end):
        """
        @param key Redis key
        @param start first element
        @param end last element

        Return the specified elements of the list stored at the key key.
        Start and end are zero-based indexes. 0 is the first element
        of the list (the list head), 1 the next element and so on.
        For example LRANGE foobar 0 2 will return the first three elements
        of the list.
        start and end can also be negative numbers indicating offsets from
        the end of the list. For example -1 is the last element of the
        list, -2 the penultimate element and so on.
        Indexes out of range will not produce an error: if start is over
        the end of the list, or start > end, an empty list is returned. If
        end is over the end of the list Redis will threat it just like the
        last element of the list.

        @note Time complexity: O(n) (with n being the length of the range)
        """
        self._send('LRANGE', key, start, end)
        return self.getResponse()

    def ltrim(self, key, start, end):
        """
        @param key Redis key
        @param start first element
        @param end last element

        Trim an existing list so that it will contain only the specified
        range of elements specified. Start and end are zero-based indexes.
        0 is the first element of the list (the list head), 1 the next
        element and so on.
        For example LTRIM foobar 0 2 will modify the list stored at foobar
        key so that only the first three elements of the list will remain.
        start and end can also be negative numbers indicating offsets from
        the end of the list. For example -1 is the last element of the
        list, -2 the penultimate element and so on.
        Indexes out of range will not produce an error: if start is over
        the end of the list, or start > end, an empty list is left as
        value. If end over the end of the list Redis will threat it just
        like the last element of the list.

        @note Time complexity: O(n) (with n being len of list - len of range)
        """
        self._send('LTRIM', key, start, end)
        return self.getResponse()

    def lindex(self, key, index):
        """
        @param key Redis key
        @param index index of element

        Return the specified element of the list stored at the specified
        key. 0 is the first element, 1 the second and so on. Negative
        indexes are supported, for example -1 is the last element, -2 the
        penultimate and so on.
        If the value stored at key is not of list type an error is
        returned. If the index is out of range an empty string is returned.

        @note Time complexity: O(n) (with n being the length of the list)
        Note that even if the average time complexity is O(n) asking for
        the first or the last element of the list is O(1).
        """
        self._send('LINDEX', key, index)
        return self.getResponse()

    def rpop(self, key):
        self._send('RPOP', key)
        return self.getResponse()

    def lpop(self, key):
        self._send('LPOP', key)
        return self.getResponse()

    def pop(self, key, tail=False):
        """
        @param key Redis key
        @param tail pop element from tail instead of head

        Atomically return and remove the first (LPOP) or last (RPOP)
        element of the list. For example if the list contains the elements
        "a","b","c" LPOP will return "a" and the list will become "b","c".
        If the key does not exist or the list is already empty the special
        value 'nil' is returned.
        """
        return self.rpop(key) if tail else self.lpop(key)

    def brpop(self, keys, timeout=30):
        """
        Issue a BRPOP - blockling list pop from the right.
        @param keys is a list of one or more Redis keys
        @param timeout max number of seconds to block for
        """
        self._send('BRPOP', *(list(keys) + [str(timeout)]))
        return self.getResponse()

    def brpoplpush(self, source, destination, timeout=30):
        """
        Blocking variant of RPOPLPUSH.
        @param source - Source list.
        @param destination - Destination list
        @param timeout - max number of seconds to block for (a
                        timeout of 0 will block indefinitely)
        """
        self._send('BRPOPLPUSH', source, destination, str(timeout))
        return self.getResponse()

    def bpop(self, keys, tail=False, timeout=30):
        """
        @param keys a list of one or more Redis keys of non-empty list(s)
        @param tail pop element from tail instead of head
        @param timeout max number of seconds block for (0 is forever)

        BLPOP (and BRPOP) is a blocking list pop primitive. You can see
        this commands as blocking versions of LPOP and RPOP able to block
        if the specified keys don't exist or contain empty lists.
        The following is a description of the exact semantic. We
        describe BLPOP but the two commands are identical, the only
        difference is that BLPOP pops the element from the left (head)
        of the list, and BRPOP pops from the right (tail).

        Non blocking behavior
        When BLPOP is called, if at least one of the specified keys
        contain a non empty list, an element is popped from the head of
        the list and returned to the caller together with the name of
        the key (BLPOP returns a two elements array, the first element
        is the key, the second the popped value).
        Keys are scanned from left to right, so for instance if you
        issue BLPOP list1 list2 list3 0 against a dataset where list1
        does not exist but list2 and list3 contain non empty lists,
        BLPOP guarantees to return an element from the list stored at
        list2 (since it is the first non empty list starting from the
        left).

        Blocking behavior
        If none of the specified keys exist or contain non empty lists,
        BLPOP blocks until some other client performs a LPUSH or an
        RPUSH operation against one of the lists.
        Once new data is present on one of the lists, the client
        finally returns with the name of the key unblocking it and the
        popped value.
        When blocking, if a non-zero timeout is specified, the client
        will unblock returning a nil special value if the specified
        amount of seconds passed without a push operation against at
        least one of the specified keys.
        A timeout of zero means instead to block forever.

        Multiple clients blocking for the same keys
        Multiple clients can block for the same key. They are put into
        a queue, so the first to be served will be the one that started
        to wait earlier, in a first-blpopping first-served fashion.

        Return value
        BLPOP returns a two-elements array via a multi bulk reply in
        order to return both the unblocking key and the popped value.
        When a non-zero timeout is specified, and the BLPOP operation
        timed out, the return value is a nil multi bulk reply. Most
        client values will return false or nil accordingly to the
        programming language used.
        """
        cmd = 'BRPOP' if tail else 'BLPOP'
        self._send(cmd, *(list(keys) + [str(timeout)]))
        return self.getResponse()

    def rpoplpush(self, srckey, dstkey):
        """
        @param srckey key of list to pop tail element of
        @param dstkey key of list to push to

        Atomically return and remove the last (tail) element of the srckey
        list, and push the element as the first (head) element of the
        dstkey list. For example if the source list contains the elements
        "a","b","c" and the destination list contains the elements
        "foo","bar" after an RPOPLPUSH command the content of the two lists
        will be "a","b" and "c","foo","bar".
        If the key does not exist or the list is already empty the special
        value 'nil' is returned. If the srckey and dstkey are the same the
        operation is equivalent to removing the last element from the list
        and pusing it as first element of the list, so it's a "list
        rotation" command.

        Programming patterns: safe queues
        Redis lists are often used as queues in order to exchange messages
        between different programs. A program can add a message performing
        an LPUSH operation against a Redis list (we call this program a
        Producer), while another program (that we call Consumer)
        can process the messages performing an RPOP command in
        order to start reading the messages from the oldest.
        Unfortunately if a Consumer crashes just after an RPOP
        operation the message gets lost. RPOPLPUSH solves this
        problem since the returned message is added to another
        "backup" list. The Consumer can later remove the message
        from the backup list using the LREM command when the
        message was correctly processed.
        Another process, called Helper, can monitor the "backup"
        list to check for timed out entries to repush against the
        main queue.

        Programming patterns: server-side O(N) list traversal
        Using RPOPPUSH with the same source and destination key a
        process can visit all the elements of an N-elements List in
        O(N) without to transfer the full list from the server to
        the client in a single LRANGE operation. Note that a
        process can traverse the list even while other processes
        are actively RPUSHing against the list, and still no
        element will be skipped.
        Return value

        Bulk reply
        """
        self._send('RPOPLPUSH', srckey, dstkey)
        return self.getResponse()

    def lset(self, key, index, value):
        """
        @param key Redis key
        @param index index of element
        @param value new value of element at index

        Set the list element at index (see LINDEX for information about the
        index argument) with the new value. Out of range indexes will
        generate an error. Note that setting the first or last elements of
        the list is O(1).
        Similarly to other list commands accepting indexes, the index can
        be negative to access elements starting from the end of the list.
        So -1 is the last element, -2 is the penultimate, and so forth.

        @note Time complexity: O(N) (with N being the length of the list)
        """
        self._send('LSET', key, index, value)
        return self.getResponse()

    def lrem(self, key, value, count=0):
        """
        @param key Redis key
        @param value value to match
        @param count number of occurrences of value
        Remove the first count occurrences of the value element from the
        list. If count is zero all the elements are removed. If count is
        negative elements are removed from tail to head, instead to go from
        head to tail that is the normal behavior. So for example LREM with
        count -2 and hello as value to remove against the list
        (a,b,c,hello,x,hello,hello) will lave the list (a,b,c,hello,x). The
        number of removed elements is returned as an integer, see below for
        more information about the returned value. Note that non existing
        keys are considered like empty lists by LREM, so LREM against non
        existing keys will always return 0.

        @retval deferred that returns the number of removed elements
        (int) if the operation succeeded

        @note Time complexity: O(N) (with N being the length of the list)
        """
        self._send('LREM', key, count, value)
        return self.getResponse()

    # Commands operating on sets
    def _list_to_set(self, res):
        if type(res) is list:
            return set(res)
        return res

    def sadd(self, key, *values, **kwargs):
        """
        Add a member to a set
        @param key : SET key to add values to.
        @param values : sequence of values to add to set
        @param value : For backwards compatibility, add one value.
        """
        if not kwargs:
            self._send('SADD', key, *values)
        elif 'value' in kwargs:
            self._send('SADD', key, kwargs['value'])
        else:
            raise exceptions.InvalidCommand('Need arguments for SADD')
        return self.getResponse()

    def srem(self, key, *values, **kwargs):
        """
        Remove a member from a set
        @param key : Set key
        @param values : Sequence of values to remove
        @param value : For backwards compatibility, single value to remove.
        """
        if not kwargs:
            self._send('SREM', key, *values)
        elif 'value' in kwargs:
            self._send('SREM', key, kwargs['value'])
        else:
            raise exceptions.InvalidCommand('Need arguments for SREM')
        return self.getResponse()

    def spop(self, key):
        """
        Remove and return a random member from a set
        """
        self._send('SPOP', key)
        return self.getResponse()

    def scard(self, key):
        """
        Get the number of members in a set
        """
        self._send('SCARD', key)
        return self.getResponse()

    def sismember(self, key, value):
        """
        Determine if a given value is a member of a set
        """
        self._send('SISMEMBER', key, value)
        return self.getResponse()

    def sdiff(self, *args):
        """
        Subtract multiple sets
        """
        self._send('SDIFF', *args)
        return self.getResponse()

    def sdiffstore(self, dstkey, *args):
        """
        Subtract multiple sets and store the resulting set in dstkey
        """
        self._send('SDIFFSTORE', dstkey, *args)
        return self.getResponse()

    def srandmember(self, key):
        """
        Get a random member from a set
        """
        self._send('SRANDMEMBER', key)
        return self.getResponse()

    def sinter(self, *args):
        """
        Intersect multiple sets
        """
        self._send('SINTER', *args)
        return self.getResponse().addCallback(self._list_to_set)

    def sinterstore(self, dest, *args):
        """
        Intersect multiple sets and store the resulting set in dest
        """
        self._send('SINTERSTORE', dest, *args)
        return self.getResponse()

    def smembers(self, key):
        """
        Get all the members in a set
        """
        self._send('SMEMBERS', key)
        return self.getResponse().addCallback(self._list_to_set)

    def smove(self, srckey, dstkey, member):
        """Move member from the set at srckey to the set at dstkey."""
        self._send('SMOVE', srckey, dstkey, member)
        return self.getResponse()

    def sunion(self, *args):
        """
        Add multiple sets
        """
        self._send('SUNION', *args)
        return self.getResponse().addCallback(self._list_to_set)

    def sunionstore(self, dest, *args):
        """
        Add multiple sets and store the resulting set in dest
        """
        self._send('SUNIONSTORE', dest, *args)
        return self.getResponse()

    # Multiple databases handling commands
    def select(self, db):
        """
        Select the DB with having the specified zero-based numeric index. New
        connections always use DB 0.
        """
        self._send('SELECT', db)
        return self.getResponse()

    def move(self, key, db):
        """
        Move a key to another database
        """
        self._send('MOVE', key, db)
        return self.getResponse()

    def flush(self, all_dbs=False):
        """
        Remove all keys from the current database or, if all_dbs is True,
        all databases.
        """
        if all_dbs:
            return self.flushall()
        else:
            return self.flushdb()

    def flushall(self):
        """
        Remove all keys from all databases
        """
        self._send('FLUSHALL')
        return self.getResponse()

    def flushdb(self):
        """
        Remove all keys from the current database
        """
        self._send('FLUSHDB')
        return self.getResponse()

    # Persistence control commands
    def bgrewriteaof(self):
        """
        Rewrites the append-only file to reflect the current dataset in memory.
        If BGREWRITEAOF fails, no data gets lost as the old AOF will be
        untouched.
        """
        self._send('BGREWRITEAOF')
        return self.getResponse()

    def bgsave(self):
        """
        Save the DB in background. The OK code is immediately returned. Redis
        forks, the parent continues to server the clients, the child saves the
        DB on disk then exit. A client my be able to check if the operation
        succeeded using the LASTSAVE command.
        """
        self._send('BGSAVE')
        return self.getResponse()

    def save(self, background=False):
        """
        Synchronously save the dataset to disk.
        """
        if background:
            return self.bgsave()
        else:
            self._send('SAVE')
        return self.getResponse()

    def lastsave(self):
        """
        Return the UNIX TIME of the last DB save executed with success. A
        client may check if a BGSAVE command succeeded reading the LASTSAVE
        value, then issuing a BGSAVE command and checking at regular intervals
        every N seconds if LASTSAVE changed.
        """
        self._send('LASTSAVE')
        return self.getResponse()

    def info(self):
        """
        The info command returns different information and statistics about the
        server in an format that's simple to parse by computers and easy to red
        by huamns.
        """
        self._send('INFO')

        def post_process(res):
            info = dict()
            res = res.split('\r\n')
            for l in res:
                if not l or l[0] == '#':
                    continue
                k, v = l.split(':')
                info[k] = int(v) if v.isdigit() else v
            return info

        return self.getResponse().addCallback(post_process)

    def sort(self, key, by=None, get=None, start=None, num=None, desc=False,
             alpha=False):
        """
        Sort the elements in a list, set or sorted set
        """
        stmt = ['SORT', key]
        if by:
            stmt.extend(['BY', by])
        if start and num:
            stmt.extend(['LIMIT', start, num])
        if get is None:
            pass
        elif isinstance(get, basestring):
            stmt.extend(['GET', get])
        elif isinstance(get, list) or isinstance(get, tuple):
            for g in get:
                stmt.extend(['GET', get])
        else:
            raise exceptions.RedisError(
                "Invalid parameter 'get' for Redis sort")
        if desc:
            stmt.append("DESC")
        if alpha:
            stmt.append("ALPHA")
        self._send(*stmt)
        return self.getResponse()

    def auth(self, passwd):
        """
        Request for authentication in a password protected Redis server. Redis
        can be instructed to require a password before allowing clients to
        execute commands. This is done using the requirepass directive in the
        configuration file.  If password matches the password in the
        configuration file, the server replies with the OK status code and
        starts accepting commands. Otherwise, an error is returned and the
        clients needs to try a new password.

        Note: because of the high performance nature of Redis, it is possible
        to try a lot of passwords in parallel in very short time, so make sure
        to generate a strong and very long password so that this attack is
        infeasible.
        """
        self._send('AUTH', passwd)
        return self.getResponse()

    def quit(self):
        """
        Ask the server to close the connection. The connection is closed as
        soon as all pending replies have been written to the client.
        """
        self._send('QUIT')
        return self.getResponse()

    def echo(self, msg):
        """
        Returns message.
        """
        self._send('ECHO', msg)
        return self.getResponse()

    # # # # # # # # #
    # Hash Commands:
    # HSET
    # HGET
    # HMSET
    # HINCRBY
    # HEXISTS
    # HDEL
    # HLEN
    # HKEYS
    # HVALS
    # HGETALL
    def hmset(self, key, in_dict):
        """
        Sets the specified fields to their respective values in the hash stored
        at key. This command overwrites any existing fields in the hash. If key
        does not exist, a new key holding a hash is created.
        """
        fields = list(itertools.chain(*in_dict.iteritems()))
        self._send('HMSET', key, *fields)
        return self.getResponse()

    def hset(self, key, field, value, preserve=False):
        """
        Sets field in the hash stored at key to value. If key does not exist, a
        new key holding a hash is created. If field already exists in the hash,
        it is overwritten.
        """
        if preserve:
            return self.hsetnx(key, field, value)
        else:
            self._send('HSET', key, field, value)
            return self.getResponse()

    def hsetnx(self, key, field, value):
        """
        Sets field in the hash stored at key to value, only if field does not
        yet exist. If key does not exist, a new key holding a hash is created.
        If field already exists, this operation has no effect.
        """
        self._send('HSETNX', key, field, value)
        return self.getResponse()

    def hget(self, key, field):
        """
        Returns the value associated with field in the hash stored at key.
        """
        if isinstance(field, basestring):
            self._send('HGET', key, field)
        else:
            self._send('HMGET', *([key] + field))

        def post_process(values):
            if not values:
                return values
            if isinstance(field, basestring):
                return {field: values}
            return dict(itertools.izip(field, values))

        return self.getResponse().addCallback(post_process)
    hmget = hget

    def hget_value(self, key, field):
        """
        Get the value of a hash field
        """
        assert isinstance(field, basestring)
        self._send('HGET', key, field)
        return self.getResponse()

    def hkeys(self, key):
        """
        Get all the fields in a hash
        """
        self._send('HKEYS', key)
        return self.getResponse()

    def hvals(self, key):
        """
        Get all the values in a hash
        """
        self._send('HVALS', key)
        return self.getResponse()

    def hincr(self, key, field, amount=1):
        """
        Increments the number stored at field in the hash stored at key by
        increment. If key does not exist, a new key holding a hash is created.
        If field does not exist or holds a string that cannot be interpreted as
        integer, the value is set to 0 before the operation is performed.  The
        range of values supported by HINCRBY is limited to 64 bit signed
        integers.
        """
        self._send('HINCRBY', key, field, amount)
        return self.getResponse()
    hincrby = hincr

    def hexists(self, key, field):
        """
        Returns if field is an existing field in the hash stored at key.
        """
        self._send('HEXISTS', key, field)
        return self.getResponse()

    def hdel(self, key, *fields):
        """
        Removes field from the hash stored at key.
        @param key : Hash key
        @param fields : Sequence of fields to remvoe
        """
        if fields:
            self._send('HDEL', key, *fields)
        else:
            raise exceptions.InvalidCommand('Need arguments for HDEL')
        return self.getResponse()
    hdelete = hdel  # backwards compat for older txredis

    def hlen(self, key):
        """
        Returns the number of fields contained in the hash stored at key.
        """
        self._send('HLEN', key)
        return self.getResponse()

    def hgetall(self, key):
        """
        Returns all fields and values of the hash stored at key. In the
        returned value, every field name is followed by its value, so the
        length of the reply is twice the size of the hash.
        """
        self._send('HGETALL', key)

        def post_process(key_vals):
            res = {}
            i = 0
            while i < len(key_vals) - 1:
                res[key_vals[i]] = key_vals[i + 1]
                i += 2
            return res

        return self.getResponse().addCallback(post_process)

    def publish(self, channel, message):
        """
        Publishes a message to all subscribers of a specified channel.
        """
        self._send('PUBLISH', channel, message)
        return self.getResponse()

    # # # # # # # # #
    # Sorted Set Commands:
    # ZADD
    # ZREM
    # ZINCRBY
    # ZRANK
    # ZREVRANK
    # ZRANGE
    # ZREVRANGE
    # ZRANGEBYSCORE
    # ZREVRANGEBYSCORE
    # ZCARD
    # ZSCORE
    # ZREMRANGEBYRANK
    # ZREMRANGEBYSCORE
    # ZUNIONSTORE / ZINTERSTORE
    def zadd(self, key, *item_tuples, **kwargs):
        """
        Add members to a sorted set, or update its score if it already exists
        @param key : Sorted set key
        @param item_tuples : Sequence of score, value pairs.
                            e.g. zadd(key, score1, value1, score2, value2)
        @param member : For backwards compatibility, member name.
        @param score : For backwards compatibility, score.

        NOTE: If there are only two arguments, the order is interpreted
              as (value, score) for backwards compatibility reasons.
        """
        if not kwargs and len(item_tuples) == 2 and \
           isinstance(item_tuples[0], basestring):
            self._send('ZADD', key, item_tuples[1], item_tuples[0])
        elif not kwargs:
            self._send('ZADD', key, *item_tuples)
        elif 'member' in kwargs and 'score' in kwargs:
            score, member = item_tuples
            self._send('ZADD', key, kwargs['score'], kwargs['member'])
        else:
            raise exceptions.InvalidCommand('Need arguments for ZADD')
        return self.getResponse()

    def zrem(self, key, *members, **kwargs):
        """
        Remove members from a sorted set
        @param key : Sorted set key
        @param members : Sequeunce of members to remove
        @param member : For backwards compatibility - if specified remove
                        one member.
        """
        if not kwargs:
            self._send('ZREM', key, *members)
        elif 'member' in kwargs:
            self._send('ZREM', key, kwargs['member'])
        else:
            raise exceptions.InvalidCommand('Need arguments for ZREM')
        return self.getResponse()

    def zremrangebyrank(self, key, start, end):
        """
        Remove all members in a sorted set within the given indexes
        """
        self._send('ZREMRANGEBYRANK', key, start, end)
        return self.getResponse()

    def zremrangebyscore(self, key, min, max):
        """
        Remove all members in a sorted set within the given scores
        """
        self._send('ZREMRANGEBYSCORE', key, min, max)
        return self.getResponse()

    def _zopstore(self, op, dstkey, keys, aggregate=None):
        """ Creates a union or intersection of N sorted sets given by keys k1
        through kN, and stores it at dstkey. It is mandatory to provide the
        number of input keys N, before passing the input keys and the other
        (optional) arguments.
        """
        # basic arguments
        args = [op, dstkey, len(keys)]
        # add in key names, and optionally weights
        if isinstance(keys, dict):
            args.extend(list(keys.iterkeys()))
            args.append('WEIGHTS')
            args.extend(list(keys.itervalues()))
        else:
            args.extend(keys)
        if aggregate:
            args.append('AGGREGATE')
            args.append(aggregate)
        self._send(*args)
        return self.getResponse()

    def zunionstore(self, dstkey, keys, aggregate=None):
        """ Creates a union of N sorted sets at dstkey. keys can be a list
        of keys or dict of keys mapping to weights. aggregate can be
        one of SUM, MIN or MAX.
        """
        return self._zopstore('ZUNIONSTORE', dstkey, keys, aggregate)

    def zinterstore(self, dstkey, keys, aggregate=None):
        """Creates an intersection of N sorted sets at dstkey.

        Keys can be a list of keys or dict of keys mapping to weights.
        Aggregate can be one of SUM, MIN or MAX.

        """
        return self._zopstore('ZINTERSTORE', dstkey, keys, aggregate)

    def zincr(self, key, member, incr=1):
        """
        Increment the score of a member in a sorted set by the given amount
        (default 1)
        """
        self._send('ZINCRBY', key, incr, member)
        return self.getResponse()

    def zrank(self, key, member, reverse=False):
        """
        Determine the index of a member in a sorted set. If reverse
        is True, the scores are orderd from high to low.
        """
        cmd = 'ZREVRANK' if reverse else 'ZRANK'
        self._send(cmd, key, member)
        return self.getResponse()

    def zcount(self, key, min, max):
        """
        Count the members in a sorted set with scores within the given values
        """
        self._send('ZCOUNT', key, min, max)
        return self.getResponse()

    def zrange(self, key, start, end, withscores=False, reverse=False):
        """
        Return a range of members in a sorted set, by index.
        If withscores is True, the score is returned as well.
        If reverse is True, the elements are considered to be
        sorted from high to low.
        """
        cmd = 'ZREVRANGE' if reverse else 'ZRANGE'
        args = [cmd, key, start, end]
        if withscores:
            args.append('WITHSCORES')
        self._send(*args)
        dfr = self.getResponse()

        def post_process(vals_and_scores):
            # return list of (val, score) tuples
            res = []
            bins = len(vals_and_scores) - 1
            i = 0
            while i < bins:
                res.append((vals_and_scores[i], float(vals_and_scores[i + 1])))
                i += 2
            return res

        if withscores:
            dfr.addCallback(post_process)
        return dfr

    def zrevrange(self, key, start, end, withscores=False):
        """
        Return a range of members in a sorted set, by index, with scores
        ordered from high to low
        """
        return self.zrange(key, start, end, withscores, reverse=True)

    def zrevrank(self, key, member):
        """
        Determine the index of a member in a sorted set, with scores ordered
        from high to low
        """
        self._send('ZREVRANK', key, member)
        return self.getResponse()

    def zcard(self, key):
        """
        Get the number of members in a sorted set
        """
        self._send('ZCARD', key)
        return self.getResponse()

    def zscore(self, key, element):
        """
        Get the score associated with the given member in a sorted set
        """
        self._send('ZSCORE', key, element)

        def post_process(res):
            if res is not None:
                return float(res)
            else:
                return res
        return self.getResponse().addCallback(post_process)

    def zrangebyscore(self, key, min='-inf', max='+inf', offset=0,
                      count=None, withscores=False, reverse=False):
        """
        Return a range of members in a sorted set, by score.
        """
        if reverse:
            args = ['ZREVRANGEBYSCORE', key, max, min]
        else:
            args = ['ZRANGEBYSCORE', key, min, max]
        if count is not None:
            args.extend(['LIMIT', offset, count])
        elif offset:
            raise ValueError("Can't have offset without count")
        if withscores:
            args.append('WITHSCORES')
        self._send(*args)
        dfr = self.getResponse()

        def post_process(vals_and_scores):
            # return list of (val, score) tuples
            res = []
            bins = len(vals_and_scores) - 1
            i = 0
            while i < bins:
                res.append((vals_and_scores[i], float(vals_and_scores[i + 1])))
                i += 2
            return res

        if withscores:
            dfr.addCallback(post_process)
        return dfr

    def zrevrangebyscore(self, key, min='-inf', max='+inf', offset=0,
            count=None, withscores=False):
        return self.zrangebyscore(key, min, max, offset, count, withscores,
                reverse=True)

    # # # # # # # # #
    # Scripting Commands:
    # EVAL
    # EVALSHA
    # SCRIPT LOAD
    # SCRIPT EXISTS
    # SCRIPT FLUSH
    # SCRIPT KILL

    def eval(self, source, keys=(), args=()):
        """
        Evaluate Lua script source with keys and arguments.
        """
        keycount = len(keys)
        args = ['EVAL', source, keycount] + list(keys) + list(args)
        self._send(*args)
        return self.getResponse()

    def evalsha(self, sha1, keys=(), args=()):
        """
        Evaluate Lua script loaded in script cache under given sha1 with keys
        and arguments.
        """
        keycount = len(keys)
        args = ['EVALSHA', sha1, keycount] + list(keys) + list(args)
        self._send(*args)
        return self.getResponse()

    def script_load(self, source):
        """
        Load Lua script source into cache. This returns the SHA1 of the loaded
        script on success.
        """
        args = ['SCRIPT', 'LOAD', source]
        self._send(*args)
        return self.getResponse()

    def script_exists(self, *sha1s):
        """
        Check whether of no scripts for given sha1 exists in cache. Returns
        list of booleans.
        """
        args = ['SCRIPT', 'EXISTS'] + list(sha1s)
        self._send(*args)
        return self.getResponse()

    def script_flush(self):
        """
        Flush the script cache.
        """
        args = ['SCRIPT', 'FLUSH']
        self._send(*args)
        return self.getResponse()

    def script_kill(self):
        """
        Kill the currently executing script.
        """
        args = ['SCRIPT', 'KILL']
        self._send(*args)
        return self.getResponse()


class HiRedisClient(HiRedisBase, RedisClient):
    """A subclass of the Redis protocol that uses the hiredis library for
    parsing.
    """
    def __init__(self, db=None, password=None, charset='utf8',
                 errors='strict'):
        super(HiRedisClient, self).__init__(db, password, charset, errors)
        self._reader = hiredis.Reader(protocolError=exceptions.InvalidData,
                                      replyError=exceptions.ResponseError)


class RedisSubscriber(RedisBase):
    """
    Redis client for subscribing & listening for published events.  Redis
    connections listening to events are expected to not issue commands other
    than subscribe & unsubscribe, and therefore no other commands are available
    on a RedisSubscriber instance.
    """

    def __init__(self, *args, **kwargs):
        RedisBase.__init__(self, *args, **kwargs)
        self.setTimeout(None)

    def handleCompleteMultiBulkData(self, reply):
        """
        Overrides RedisBase.handleCompleteMultiBulkData to intercept published
        message events.
        """
        if reply[0] == u"message":
            channel, message = reply[1:]
            self.messageReceived(channel, message)
        elif reply[0] == u"pmessage":
            pattern, channel, message = reply[1:]
            self.messageReceived(channel, message)
        elif reply[0] == u"subscribe":
            channel, numSubscribed = reply[1:]
            self.channelSubscribed(channel, numSubscribed)
        elif reply[0] == u"unsubscribe":
            channel, numSubscribed = reply[1:]
            self.channelUnsubscribed(channel, numSubscribed)
        elif reply[0] == u"psubscribe":
            channelPattern, numSubscribed = reply[1:]
            self.channelPatternSubscribed(channelPattern, numSubscribed)
        elif reply[0] == u"punsubscribe":
            channelPattern, numSubscribed = reply[1:]
            self.channelPatternUnsubscribed(channelPattern, numSubscribed)
        else:
            RedisBase.handleCompleteMultiBulkData(self, reply)

    def messageReceived(self, channel, message):
        """
        Called when this connection is subscribed to a channel that
        has received a message published on it.
        """
        pass

    def channelSubscribed(self, channel, numSubscriptions):
        """
        Called when a channel is subscribed to.
        """
        pass

    def channelUnsubscribed(self, channel, numSubscriptions):
        """
        Called when a channel is unsubscribed from.
        """
        pass

    def channelPatternSubscribed(self, channel, numSubscriptions):
        """
        Called when a channel patern is subscribed to.
        """
        pass

    def channelPatternUnsubscribed(self, channel, numSubscriptions):
        """
        Called when a channel pattern is unsubscribed from.
        """
        pass

    def subscribe(self, *channels):
        """
        Begin listening for PUBLISH messages on one or more channels.  When a
        message is published on one, the messageReceived method will be
        invoked.  Does not return any value, although the method
        channelSubscribed will be invoked on confirmation from the server of
        every subscribed channel.  If a channel is already subscribed to by
        this connection, then channelSubscribed will not be invoked but the
        channel will continue to be subscribed to.
        """
        self._send('SUBSCRIBE', *channels)

    def unsubscribe(self, *channels):
        """
        Terminate listening for PUBLISH messages on one or more channels.  If
        no channels are passed in, all channels are unsubscribed from.i Does
        not return any value, but the method channelUnsubscribed will be
        invokved for each channel that is unsubscribed from.  If a channel is
        provided that is not subscribed to by this connection, then
        channelUnsubscribed will not be invoked.
        """
        self._send('UNSUBSCRIBE', *channels)

    def psubscribe(self, *patterns):
        """
        Begin listening for PUBLISH messages on one or more channel patterns.
        When a message is published on a matching channel, the messageReceived
        method will be invoked.  Does not return any value, but the method
        channelPatternSubscribed will be invoked for each channel pattern that
        is subscribed to.
        """
        self._send('PSUBSCRIBE', *patterns)

    def punsubscribe(self, *patterns):
        """
        Terminate listening for PUBLISH messages on one or more channel
        patterns.  If no channel patterns are passed in, all channel patterns
        are unsubscribed from.  Does not return any value, but the method
        channelPatternUnsubscribed will be invoked for eeach channel pattern
        that is unsubscribed from.
        """
        self._send('PUNSUBSCRIBE', *patterns)


class RedisClientFactory(ReconnectingClientFactory):

    protocol = RedisClient

    def __init__(self, *args, **kwargs):
        self.noisy = True
        self._args = args
        self._kwargs = kwargs
        self.client = None
        self.deferred = defer.Deferred()

    def buildProtocol(self, addr):
        from twisted.internet import reactor

        def fire(res):
            self.deferred.callback(self.client)
            self.deferred = defer.Deferred()
        self.client = self.protocol(*self._args, **self._kwargs)
        self.client.factory = self
        reactor.callLater(0, fire, self.client)
        self.resetDelay()
        return self.client


class RedisSubscriberFactory(RedisClientFactory):
    protocol = RedisSubscriber


# backwards compatibility
Redis = RedisClient
HiRedisProtocol = HiRedisClient

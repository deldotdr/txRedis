"""
@file protocol.py

@author Reza Lotun (rlotun@gmail.com)
@date 06/22/10
Added multi-bulk command sending support.
Added support for hash commands.
Added support for sorted set.
Added support for new basic commands APPEND and SUBSTR.
Removed forcing of float data to be decimal.
Removed inlineCallbacks within protocol code.
Added setuptools support to setup.py

@author Garret Heaton (powdahound@gmail.com)
@date 06/15/10
Added read buffering for bulk data.
Removed use of LineReceiver to avoid Twisted recursion bug.
Added support for multi, exec, and discard

@author Dorian Raymer
@date 02/01/10
Added BLPOP/BRPOP and RPOPLPUSH to list commands.
Added doc strings to list commands (copied from the Redis google code
project page).

@author Dorian Raymer
@author Ludovico Magnocavallo
@date 9/30/09
@brief Twisted compatible version of redis.py

@mainpage

txRedis is an asynchronous, Twisted, version of redis.py (included in the
redis server source).

The official Redis Command Reference:
http://code.google.com/p/redis/wiki/CommandReference

@section An example demonstrating how to use the client in your code:
@code
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import defer

from txredis.protocol import Redis

@defer.inlineCallbacks
def main():
    clientCreator = protocol.ClientCreator(reactor, Redis)
    redis = yield clientCreator.connectTCP(HOST, PORT)

    res = yield redis.ping()
    print res

    res = yield redis.set('test', 42)
    print res

    test = yield redis.get('test')
    print res

@endcode

Redis google code project: http://code.google.com/p/redis/
Command doc strings taken from the CommandReference wiki page.

"""


from collections import deque
from itertools import chain, izip

from twisted.internet import defer, protocol
from twisted.protocols import policies

try:
    import hiredis
except ImportError:
    pass

class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class ResponseError(RedisError):
    pass


class InvalidResponse(RedisError):
    pass


class InvalidData(RedisError):
    pass


class RedisBase(protocol.Protocol, policies.TimeoutMixin, object):
    """The main Redis client."""

    ERROR = "-"
    SINGLE_LINE = "+"
    INTEGER = ":"
    BULK = "$"
    MULTI_BULK = "*"

    def __init__(self, db=None, password=None, charset='utf8', errors='strict'):
        self.charset = charset
        self.db = db if db is not None else 0
        self.password = password
        self.errors = errors
        self._buffer = ''
        self._bulk_length = None
        self._disconnected = False
        self._multi_bulk_length = None
        self._multi_bulk_reply = []
        self._request_queue = deque()

    def dataReceived(self, data):
        """Receive data.

        Spec: http://redis.io/topics/protocol
        """
        self.resetTimeout()
        self._buffer = self._buffer + data

        while self._buffer:

            # if we're expecting bulk data, read that many bytes
            if self._bulk_length is not None:
                # wait until there's enough data in the buffer
                if len(self._buffer) < self._bulk_length + 2: # /r/n
                    return
                data = self._buffer[:self._bulk_length]
                self._buffer = self._buffer[self._bulk_length+2:] # 2 for /r/n
                self.bulkDataReceived(data)
                continue

            # wait until we have a line
            if '\r\n' not in self._buffer:
                return

            # grab a line
            line, self._buffer = self._buffer.split('\r\n', 1)
            if len(line) == 0:
                continue

            # first byte indicates reply type
            reply_type = line[0]
            reply_data = line[1:]

            # Error message (-)
            if reply_type == self.ERROR:
                self.errorReceived(reply_data)
            # Integer number (:)
            elif reply_type == self.INTEGER:
                self.integerReceived(reply_data)
            # Single line (+)
            elif reply_type == self.SINGLE_LINE:
                self.singleLineReceived(reply_data)
            # Bulk data (&)
            elif reply_type == self.BULK:
                try:
                    self._bulk_length = int(reply_data)
                except ValueError:
                    r = InvalidResponse("Cannot convert data '%s' to integer"
                                        % reply_data)
                    self.responseReceived(r)
                    return
                # requested value may not exist
                if self._bulk_length == -1:
                    self.bulkDataReceived(None)
            # Multi-bulk data (*)
            elif reply_type == self.MULTI_BULK:
                # reply_data will contain the # of bulks we're about to get
                try:
                    self._multi_bulk_length = int(reply_data)
                except ValueError:
                    r = InvalidResponse("Cannot convert data '%s' to integer"
                                        % reply_data)
                    self.responseReceived(r)
                    return
                if self._multi_bulk_length == -1:
                    self._multi_bulk_reply = None
                    self.multiBulkDataReceived()
                    return
                elif self._multi_bulk_length == 0:
                    self.multiBulkDataReceived()

    def failRequests(self, reason):
        while self._request_queue:
            d = self._request_queue.popleft()
            d.errback(reason)

    def connectionMade(self):
        """ Called when incoming connections is made to the server. """
        d = defer.succeed(True)

        # if we have a password set, make sure we auth
        if self.password:
            d.addCallback(lambda _res : self.auth(self.password))

        # select the db passsed in
        if self.db:
            d.addCallback(lambda _res : self.select(self.db))

        def done_connecting(_res):
            # set our state as soon as we're properly connected
            self._disconnected = False
        d.addCallback(done_connecting)

        return d

    def connectionLost(self, reason):
        """Called when the connection is lost.

        Will fail all pending requests.

        """
        self._disconnected = True
        self.failRequests(reason)

    def timeoutConnection(self):
        """Called when the connection times out.

        Will fail all pending requests with a TimeoutError.

        """
        self.failRequests(defer.TimeoutError("Connection timeout"))
        self.transport.loseConnection()

    def errorReceived(self, data):
        """Error response received."""
        reply = ResponseError(data if data[:4] == 'ERR ' else data)
        if self._request_queue:
            # properly errback this reply
            self._request_queue.popleft().errback(reply)
        else:
            # we should have a request queue - if not, just raise this exception
            raise reply

    def singleLineReceived(self, data):
        """Single line response received."""
        if data == 'none':
            reply = None # should this happen here in the client?
        else:
            reply = data

        self.responseReceived(reply)

    def handleMultiBulkElement(self, element):
        self._multi_bulk_reply.append(element)
        self._multi_bulk_length = self._multi_bulk_length - 1
        if self._multi_bulk_length == 0:
            self.multiBulkDataReceived()

    def integerReceived(self, data):
        """Integer response received."""
        try:
            reply = int(data)
        except ValueError:
            reply = InvalidResponse("Cannot convert data '%s' to integer"
                                    % data)
        if self._multi_bulk_length > 0:
            self.handleMultiBulkElement(reply)
            return

        self.responseReceived(reply)

    def bulkDataReceived(self, data):
        """Bulk data response received."""
        self._bulk_length = None
        self.responseReceived(data)

    def multiBulkDataReceived(self):
        """Multi bulk response received.

        The bulks making up this response have been collected in
        self._multi_bulk_reply.

        """
        reply = self._multi_bulk_reply
        self._multi_bulk_reply = []
        self._multi_bulk_length = None
        self.handleCompleteMultiBulkData(reply)

    def handleCompleteMultiBulkData(self, reply):
        self.responseReceived(reply)

    def responseReceived(self, reply):
        """Handle a server response.

        If we're waiting for multibulk elements, store this reply. Otherwise
        provide the reply to the waiting request.

        """
        if self._multi_bulk_length > 0:
            self.handleMultiBulkElement(reply)
        elif self._request_queue:
            self._request_queue.popleft().callback(reply)

    def getResponse(self):
        """
        @retval a deferred which will fire with response from server.
        """
        if self._disconnected:
            return defer.fail(RuntimeError("Not connected"))

        d = defer.Deferred()
        self._request_queue.append(d)
        return d

    def _encode(self, s):
        """Encode a value for sending to the server."""
        if isinstance(s, str):
            return s
        if isinstance(s, unicode):
            try:
                return s.encode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData("Error encoding unicode value '%s': %s"
                                  % (s.encode(self.charset, 'replace'), e))
        return str(s)

    def _send(self, *args):
        """Encode and send a request using the 'unified request protocol' (aka multi-bulk)"""
        cmds = []
        for i in args:
            v = self._encode(i)
            cmds.append('$%s\r\n%s\r\n' % (len(v), v))
        cmd = '*%s\r\n' % len(args) + ''.join(cmds)
        self.transport.write(cmd)

    def send(self, command, *args):
        self._send(command, *args)
        return self.getResponse()


class Redis(RedisBase):
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
        """
        # The following will raise an error for unicode values that can't be
        # encoded to ascii. We could probably add an 'encoding' arg to init,
        # but then what do we do with get()? Convert back to unicode? And what
        # about ints, or pickled values?
        if getset:
            command = 'GETSET'
        elif preserve:
            command = 'SETNX'
        else:
            command = 'SET'

        if expire:
            self._send('SETEX', key, expire, value)
        else:
            self._send(command, key, value)
        return self.getResponse()

    def mset(self, mapping, preserve=False):
        if preserve:
            command = 'MSETNX'
        else:
            command = 'MSET'
        self._send(command, *list(chain(*mapping.iteritems())))
        return self.getResponse()

    def append(self, key, value):
        self._send('APPEND', key, value)
        return self.getResponse()

    def getrange(self, key, start, end):
        self._send('GETRANGE', key, start, end)
        return self.getResponse()
    substr = getrange

    def get(self, key):
        """
        """
        self._send('GET', key)
        return self.getResponse()

    def getset(self, key, value):
        """
        """
        return self.set(key, value, getset=True)

    def mget(self, *args):
        """
        """
        self._send('MGET', *args)
        return self.getResponse()

    def incr(self, key, amount=1):
        """
        """
        if amount == 1:
            self._send('INCR', key)
        else:
            self._send('INCRBY', key, amount)
        return self.getResponse()

    def decr(self, key, amount=1):
        """
        """
        if amount == 1:
            self._send('DECR', key)
        else:
            self._send('DECRBY', key, amount)
        return self.getResponse()

    def exists(self, key):
        """
        """
        self._send('EXISTS', key)
        return self.getResponse()

    def delete(self, key):
        """
        """
        self._send('DEL', key)
        return self.getResponse()

    def get_type(self, key):
        """
        """
        self._send('TYPE', key)
        res = self.getResponse()
        # return None if res == 'none' else res
        return res

    # Commands operating on the key space
    def keys(self, pattern):
        """
        """
        self._send('KEYS', pattern)

        def post_process(res):
            if res is not None:
                res.sort()# XXX is sort ok?
            else:
                res = []
            return res

        return self.getResponse().addCallback(post_process)

    def randomkey(self):
        """
        """
        #raise NotImplementedError("Implemented but buggy, do not use.")
        self._send('RANDOMKEY')
        return self.getResponse()

    def rename(self, src, dst, preserve=False):
        """
        """
        self._send('RENAMENX' if preserve else 'RENAME', src, dst)
        return self.getResponse() #.strip()

    def dbsize(self):
        """
        Return the number of keys in the selected database
        """
        self._send('DBSIZE')
        return self.getResponse()

    def expire(self, key, time):
        """
        """
        self._send('EXPIRE', key, time)
        return self.getResponse()

    def ttl(self, key):
        """
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

    def unwatch(self, *keys):
        """
        Forget about all watched keys
        """
        self._send('UNWATCH', *keys)
        return self.getResponse()


    # # # # # # # # #
    # List Commands:
    # RPUSH
    # LPUSH
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
    def push(self, key, value, tail=False):
        """
        @param key Redis key
        @param value String element of list

        Add the string value to the head (LPUSH) or tail (RPUSH) of the
        list stored at key key. If the key does not exist an empty list is
        created just before the append operation. If the key exists but is
        not a List an error is returned.

        @note Time complexity: O(1)
        """
        if tail:
            return self.rpush(key, value)
        else:
            return self.lpush(key, value)

    def lpush(self, key, value):
        self._send('LPUSH', key, value)
        return self.getResponse()

    def rpush(self, key, value):
        self._send('RPUSH', key, value)
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
        self._send('RPOP' if tail else 'LPOP', key)
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
        self._send('BRPOP' if tail else 'BLPOP', *(list(keys) + [str(timeout)]))
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

    def sadd(self, key, value):
        """
        """
        self._send('SADD', key, value)
        return self.getResponse()

    def srem(self, key, value):
        """
        """
        self._send('SREM', key, value)
        return self.getResponse()

    def spop(self, key):
        self._send('SPOP', key)
        return self.getResponse()

    def scard(self, key):
        self._send('SCARD', key)
        return self.getResponse()

    def sismember(self, key, value):
        """
        """
        self._send('SISMEMBER', key, value)
        return self.getResponse()

    def sdiff(self, *args):
        self._send('SDIFF', *args)
        return self.getResponse()

    def sdiffstore(self, dstkey, *args):
        self._send('SDIFFSTORE', dstkey, *args)
        return self.getResponse()

    def srandmember(self, key):
        self._send('SRANDMEMBER', key)
        return self.getResponse()

    def sinter(self, *args):
        """
        """
        self._send('SINTER', *args)
        return self.getResponse().addCallback(self._list_to_set)

    def sinterstore(self, dest, *args):
        """
        """
        self._send('SINTERSTORE', dest, *args)
        return self.getResponse()

    def smembers(self, key):
        """
        """
        self._send('SMEMBERS', key)
        return self.getResponse().addCallback(self._list_to_set)

    def smove(self, srckey, dstkey, member):
        """ Move the specifided member from the set at srckey to the set at dstkey. """
        self._send('SMOVE', srckey, dstkey, member)
        return self.getResponse()

    def sunion(self, *args):
        """
        """
        self._send('SUNION', *args)
        return self.getResponse().addCallback(self._list_to_set)

    def sunionstore(self, dest, *args):
        """
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
        """
        self._send('MOVE', key, db)
        return self.getResponse()

    def flush(self, all_dbs=False):
        """
        """
        if all_dbs:
            return self.flushall()
        else:
            return self.flushdb()

    def flushall(self):
        self._send('FLUSHALL')
        return self.getResponse()

    def flushdb(self):
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
                if not l:
                    continue
                k, v = l.split(':')
                info[k] = int(v) if v.isdigit() else v
            return info

        return self.getResponse().addCallback(post_process)

    def sort(self, key, by=None, get=None, start=None, num=None, desc=False,
             alpha=False):
        """
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
            raise RedisError("Invalid parameter 'get' for Redis sort")
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
        fields = list(chain(*in_dict.iteritems()))
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
            return dict(izip(field, values))

        return self.getResponse().addCallback(post_process)
    hmget = hget

    def hget_value(self, key, field):
        assert isinstance(field, basestring)
        self._send('HGET', key, field)
        return self.getResponse()

    def hkeys(self, key):
        self._send('HKEYS', key)
        return self.getResponse()

    def hvals(self, key):
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

    def hdel(self, key, field):
        """
        Removes field from the hash stored at key.
        """
        self._send('HDEL', key, field)
        return self.getResponse()
    hdelete = hdel # backwards compat for older txredis

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
    # ZCARD
    # ZSCORE
    # ZREMRANGEBYRANK
    # ZREMRANGEBYSCORE
    # ZUNIONSTORE / ZINTERSTORE
    def zadd(self, key, member, score):
        self._send('ZADD', key, score, member)
        return self.getResponse()

    def zrem(self, key, member):
        self._send('ZREM', key, member)
        return self.getResponse()

    def zremrangebyrank(self, key, start, end):
        self._send('ZREMRANGEBYRANK', key, start, end)
        return self.getResponse()

    def zremrangebyscore(self, key, min, max):
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
        """ Creates an intersection of N sorted sets at dstkey. keys can be a list
        of keys or dict of keys mapping to weights. aggregate can be
        one of SUM, MIN or MAX.
        """
        return self._zopstore('ZINTERSTORE', dstkey, keys, aggregate)

    def zincr(self, key, member, incr=1):
        self._send('ZINCRBY', key, incr, member)
        return self.getResponse()

    def zrank(self, key, member, reverse=False):
        cmd = 'ZREVRANK' if reverse else 'ZRANK'
        self._send(cmd, key, member)
        return self.getResponse()

    def zcount(self, key, min, max):
        self._send('ZCOUNT', key, min, max)
        return self.getResponse()

    def zrange(self, key, start, end, withscores=False, reverse=False):
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
                res.append((vals_and_scores[i], float(vals_and_scores[i+1])))
                i += 2
            return res

        if withscores:
            dfr.addCallback(post_process)
        return dfr

    def zrevrange(self, key, start, end, withscores=False):
        return self.zrange(key, start, end, withscores, reverse=True)

    def zrevrank(self, key, member):
        self._send('ZREVRANK', key, member)
        return self.getResponse()

    def zcard(self, key):
        self._send('ZCARD', key)
        return self.getResponse()

    def zscore(self, key, element):
        self._send('ZSCORE', key, element)
        def post_process(res):
            if res is not None:
                return float(res)
            else:
                return res
        return self.getResponse().addCallback(post_process)

    def zrangebyscore(self, key, min='-inf', max='+inf', offset=None,
                      count=None, withscores=False):
        args = ['ZRANGEBYSCORE', key, min, max]
        if offset and count:
            args.extend(['LIMIT', offset, count])
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
                res.append((vals_and_scores[i], float(vals_and_scores[i+1])))
                i += 2
            return res

        if withscores:
            dfr.addCallback(post_process)
        return dfr

class HiRedisProtocol(Redis):
    """ A subclass of the Redis protocol that uses the hiredis library for parsing. """
    def __init__(self, db=None, password=None, charset='utf8', errors='strict'):
        Redis.__init__(self, db, password, charset, errors)
        self._reader = hiredis.Reader(protocolError=InvalidData,
                                      replyError=ResponseError)

    def dataReceived(self, data):
        """Receive data.
        """
        self.resetTimeout()
        if data:
            self._reader.feed(data)
        res = self._reader.gets()
        while res is not False:
            if isinstance(res, ResponseError):
                self._request_queue.popleft().errback(res)
            else:
                if isinstance(res, basestring) and res == 'none':
                    res = None
                self._request_queue.popleft().callback(res)
            res = self._reader.gets()


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


class RedisClientFactory(protocol.ReconnectingClientFactory):
    protocol = Redis

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


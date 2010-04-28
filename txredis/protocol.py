""" 
@file protocol.py

@author Dorian Raymer
@data 02/01/10
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


import decimal

from twisted.internet import defer
from twisted.protocols import basic
from twisted.protocols import policies


class RedisError(Exception): pass
class ConnectionError(RedisError): pass
class ResponseError(RedisError): pass
class InvalidResponse(RedisError): pass
class InvalidData(RedisError): pass


class RedisReplyQueue(defer.DeferredQueue):

    def failAll(self, reason):
        """Trigger errback on all waiting deferreds"""
        while self.waiting:
            self.waiting.pop(0).errback(reason)


class Redis(basic.LineReceiver, policies.TimeoutMixin):
    """The main Redis client.
    """

    ERROR = "-"
    STATUS = "+"
    INTEGER = ":"
    BULK = "$"
    MULTI_BULK = "*"

    def __init__(self, db=None, charset='utf8', errors='strict'):
        self.charset = charset
        self.errors = errors
        self.db = db

        self.bulk_length = 0
        self.multi_bulk_length = 0
        self.multi_bulk_reply = []
        self.replyQueue = RedisReplyQueue()

        self._disconnected = False

    def _cancelCommands(self, reason):
        self.replyQueue.failAll(reason)

    def connectionLost(self, reason):
        self._disconnected = True
        self._cancelCommands(reason)
        basic.LineReceiver.connectionLost(self, reason)
        
    def lineReceived(self, line):
        """
        Reply types:
          "-" error message
          "+" single line status reply
          ":" integer number (protocol level only?)

          "$" bulk data
          "*" multi-bulk data
        """
        self.resetTimeout()
        if len(line) == 0:
            return
        token = line[0] # first byte indicates reply type
        data = line[1:]
        if token == self.ERROR:
            self.errorReceived(data)
        elif token == self.STATUS:
            self.statusReceived(data)
        elif token == self.INTEGER:
            self.integerReceived(data)
        elif token == self.BULK:
            try:
                self.bulk_length = int(data)
            except ValueError:
                self.replyReceived(InvalidResponse("Cannot convert data '%s' to integer" % data))
                return 
            if self.bulk_length == -1:
                self.bulkDataReceived(None)
                return
            else:
                self.setRawMode()
        elif token == self.MULTI_BULK:
            try:
                self.multi_bulk_length = int(data)
            except (TypeError, ValueError):
                self.replyReceived(InvalidResponse("Cannot convert multi-response header '%s' to integer" % data))
                self.multi_bulk_length = 0
                return
            if self.multi_bulk_length == -1:
                self.multi_bulk_reply = None
                self.multiBulkDataReceived()
                return
            elif self.multi_bulk_length == 0:
                self.multiBulkDataReceived()
 

    def rawDataReceived(self, data):
        """
        Process and dispatch to bulkDataReceived.
        @todo buffer raw data in case a bulk piece comes in more than one
        part
        """
        reply_len = self.bulk_length 
        bulk_data = data[:reply_len]
        rest_data = data[reply_len + 2:]
        self.bulkDataReceived(bulk_data)
        self.setLineMode(extra=rest_data)

    def timeoutConnection(self):
        self._disconnected = True
        self._cancelCommands(defer.TimeoutError("Connection timeout"))
        basic.LineReceiver.timeoutConnection(self, reason)

    def errorReceived(self, data):
        """
        Error from server.
        """
        reply = ResponseError(data[4:] if data[:4] == 'ERR ' else data)
        self.replyReceived(reply)

    def statusReceived(self, data):
        """
        Single line status should always be a string.
        """
        if data == 'none':
            reply = None # should this happen here in the client?
        else:
            reply = data 
        self.replyReceived(reply)

    def integerReceived(self, data):
        """
        For handling integer replies.
        """
        try:
            reply = int(data) 
        except ValueError:
            reply = InvalidResponse("Cannot convert data '%s' to integer" % data)
        self.replyReceived(reply)


    def bulkDataReceived(self, data):
        """
        Receipt of a bulk data element.
        """
        self.bulk_length = 0
        if data is None:
            element = data
        else:
            try:
                element = int(data) if data.find('.') == -1 else decimal.Decimal(data)
            except (ValueError, decimal.InvalidOperation):
                element = data.decode(self.charset)

        if self.multi_bulk_length > 0:
            self.handleMultiBulkElement(element)
            return
        else:
            self.replyReceived(element)

    def handleMultiBulkElement(self, element):
        self.multi_bulk_reply.append(element)
        self.multi_bulk_length = self.multi_bulk_length - 1
        if self.multi_bulk_length == 0:
            self.multiBulkDataReceived()


    def multiBulkDataReceived(self):
        """
        Receipt of list or set of bulk data elements.
        """
        reply = self.multi_bulk_reply
        self.multi_bulk_reply = []
        self.multi_bulk_length = 0
        self.replyReceived(reply)
        

    def replyReceived(self, reply):
        """
        Complete reply received and ready to be pushed to the requesting
        function.
        """
        self.replyQueue.put(reply)

    def get_response(self):
        """
        @retval a deferred which will fire with response from server.
        """
        if self._disconnected:
            return defer.fail(RuntimeError("Not connected"))
        return self.replyQueue.get()

    def _encode(self, s):
        if isinstance(s, str):
            return s
        if isinstance(s, unicode):
            try:
                return s.encode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData("Error encoding unicode value '%s': %s" % (s.encode(self.charset, 'replace'), e))
        return str(s)
    
    def _write(self, s):
        """
        """
        self.transport.write(s)
            
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # REDIS COMMANDS
    #

    def ping(self):
        """
        Test command. Expect PONG as a reply.
        """
        self._write('PING\r\n')
        return self.get_response()

    # Commands operating on string values
    def set(self, key, value, preserve=False, getset=False):
        """
        """
        # the following will raise an error for unicode values that can't be encoded to ascii
        # we could probably add an 'encoding' arg to init, but then what do we do with get()?
        # convert back to unicode? and what about ints, or pickled values?
        if getset: command = 'GETSET'
        elif preserve: command = 'SETNX'
        else: command = 'SET'
        value = self._encode(value)
        self._write('%s %s %s\r\n%s\r\n' % (
                command, key, len(value), value
            ))
        return self.get_response()
    
    def get(self, key):
        """
        """
        self._write('GET %s\r\n' % key)
        return self.get_response()
    
    def getset(self, key, value):
        """
        """
        return self.set(key, value, getset=True)
        
    def mget(self, *args):
        """
        """
        self._write('MGET %s\r\n' % ' '.join(args))
        return self.get_response()
    
    def incr(self, key, amount=1):
        """
        """
        if amount == 1:
            self._write('INCR %s\r\n' % key)
        else:
            self._write('INCRBY %s %s\r\n' % (key, amount))
        return self.get_response()

    def decr(self, key, amount=1):
        """
        """
        if amount == 1:
            self._write('DECR %s\r\n' % key)
        else:
            self._write('DECRBY %s %s\r\n' % (key, amount))
        return self.get_response()
    
    def exists(self, key):
        """
        """
        self._write('EXISTS %s\r\n' % key)
        return self.get_response()

    def delete(self, key):
        """
        """
        self._write('DEL %s\r\n' % key)
        return self.get_response()

    def get_type(self, key):
        """
        """
        self._write('TYPE %s\r\n' % key)
        res = self.get_response()
        # return None if res == 'none' else res
        return res
    
    # Commands operating on the key space
    @defer.inlineCallbacks
    def keys(self, pattern):
        """
        """
        self._write('KEYS %s\r\n' % pattern)
        # return self.get_response().split()
        r = yield self.get_response()
        if r is not None:
            res = r.split()
            res.sort()# XXX is sort ok?
        else:
            res = []
        defer.returnValue(res)
    
    def randomkey(self):
        """
        """
        #raise NotImplementedError("Implemented but buggy, do not use.")
        self._write('RANDOMKEY\r\n')
        return self.get_response()
    
    def rename(self, src, dst, preserve=False):
        """
        """
        if preserve:
            self._write('RENAMENX %s %s\r\n' % (src, dst))
            return self.get_response()
        else:
            self._write('RENAME %s %s\r\n' % (src, dst))
            return self.get_response() #.strip()
        
    def dbsize(self):
        """
        """
        self._write('DBSIZE\r\n')
        return self.get_response()
    
    def expire(self, key, time):
        """
        """
        self._write('EXPIRE %s %s\r\n' % (key, time))
        return self.get_response()
    
    def ttl(self, key):
        """
        """
        self._write('TTL %s\r\n' % key)
        return self.get_response()
    
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
        
        Add the string value to the head (RPUSH) or tail (LPUSH) of the
        list stored at key key. If the key does not exist an empty list is
        created just before the append operation. If the key exists but is
        not a List an error is returned.

        @note Time complexity: O(1)
        """
        value = self._encode(value)
        self._write('%s %s %s\r\n%s\r\n' % (
            'LPUSH' if tail else 'RPUSH', key, len(value), value
        ))
        return self.get_response()
    
    def llen(self, key):
        """
        @param key Redis key

        Return the length of the list stored at the key key. If the
        key does not exist zero is returned (the same behavior as for
        empty lists). If the value stored at key is not a list an error is
        returned.

        @note Time complexity: O(1)
        """
        self._write('LLEN %s\r\n' % key)
        return self.get_response()

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
        self._write('LRANGE %s %s %s\r\n' % (key, start, end))
        return self.get_response()
        
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
        self._write('LTRIM %s %s %s\r\n' % (key, start, end))
        return self.get_response()
    
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
        self._write('LINDEX %s %s\r\n' % (key, index))
        return self.get_response()
        
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
        self._write('%s %s\r\n' % ('RPOP' if tail else 'LPOP', key))
        return self.get_response()

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
        cmd = '%s ' % ('BRPOP' if tail else 'BLPOP',)
        for key in keys:
            cmd += '%s ' % key
        cmd += '%s\r\n' % str(timeout)
        self._write(cmd) 
        return self.get_response()

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
        self._write('%s %s %s\r\n' % ('RPOPLPUSH', srckey, dstkey,))
        return self.get_response()

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
        value = self._encode(value)
        self._write('LSET %s %s %s\r\n%s\r\n' % (
            key, index, len(value), value
        ))
        return self.get_response()
    
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
        value = self._encode(value)
        self._write('LREM %s %s %s\r\n%s\r\n' % (
            key, count, len(value), value
        ))
        return self.get_response()
    
    # Commands operating on sets
    def sadd(self, key, value):
        """
        """
        value = self._encode(value)
        self._write('SADD %s %s\r\n%s\r\n' % (
            key, len(value), value
        ))
        return self.get_response()
        
    def srem(self, key, value):
        """
        """
        value = self._encode(value)
        self._write('SREM %s %s\r\n%s\r\n' % (
            key, len(value), value
        ))
        return self.get_response()

    def spop(self, key):
        self._write('SPOP %s\r\n' % key)
        return self.get_response()

    def scard(self, key):
        self._write('SCARD %s\r\n' % key)
        return self.get_response()
    
    def sismember(self, key, value):
        """
        """
        value = self._encode(value)
        self._write('SISMEMBER %s %s\r\n%s\r\n' % (
            key, len(value), value
        ))
        return self.get_response()
    
    @defer.inlineCallbacks
    def sinter(self, *args):
        """
        """
        self._write('SINTER %s\r\n' % ' '.join(args))
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)
    
    def sinterstore(self, dest, *args):
        """
        """
        self._write('SINTERSTORE %s %s\r\n' % (dest, ' '.join(args)))
        return self.get_response()

    @defer.inlineCallbacks
    def smembers(self, key):
        """
        """
        self._write('SMEMBERS %s\r\n' % key)
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def sunion(self, *args):
        """
        """
        self._write('SUNION %s\r\n' % ' '.join(args))
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)

    def sunionstore(self, dest, *args):
        """
        """
        self._write('SUNIONSTORE %s %s\r\n' % (dest, ' '.join(args)))
        return self.get_response()

    # Multiple databases handling commands
    def select(self, db):
        """
        """
        self._write('SELECT %s\r\n' % db)
        return self.get_response()
    
    def move(self, key, db):
        """
        """
        self._write('MOVE %s %s\r\n' % (key, db))
        return self.get_response()
    
    def flush(self, all_dbs=False):
        """
        """
        self._write('%s\r\n' % ('FLUSHALL' if all_dbs else 'FLUSHDB'))
        return self.get_response()
    
    # Persistence control commands
    def save(self, background=False):
        """
        """
        if background:
            self._write('BGSAVE\r\n')
        else:
            self._write('SAVE\r\n')
        return self.get_response()
        
    def lastsave(self):
        """
        """
        self._write('LASTSAVE\r\n')
        return self.get_response()
    
    @defer.inlineCallbacks
    def info(self):
        """
        """
        self._write('INFO\r\n')
        info = dict()
        res = yield self.get_response()
        res = res.split('\r\n')
        for l in res:
            if not l:
                continue
            k, v = l.split(':')
            info[k] = int(v) if v.isdigit() else v
        defer.returnValue(info)
    
    def sort(self, key, by=None, get=None, start=None, num=None, desc=False, alpha=False):
        """
        """
        stmt = ['SORT', key]
        if by:
            stmt.append("BY %s" % by)
        if start and num:
            stmt.append("LIMIT %s %s" % (start, num))
        if get is None:
            pass
        elif isinstance(get, basestring):
            stmt.append("GET %s" % get)
        elif isinstance(get, list) or isinstance(get, tuple):
            for g in get:
                stmt.append("GET %s" % g)
        else:
            raise RedisError("Invalid parameter 'get' for Redis sort")
        if desc:
            stmt.append("DESC")
        if alpha:
            stmt.append("ALPHA")
        self._write(' '.join(stmt + ["\r\n"]))
        return self.get_response()
    
    def auth(self, passwd):
        self._write('AUTH %s\r\n' % passwd)
        return self.get_response()
    
    

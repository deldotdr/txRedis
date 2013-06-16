"""
@file protocol.py

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

from twisted.internet import defer, protocol
from twisted.protocols import policies

from txredis import exceptions


class RedisBase(protocol.Protocol, policies.TimeoutMixin, object):
    """The main Redis client."""

    ERROR = "-"
    SINGLE_LINE = "+"
    INTEGER = ":"
    BULK = "$"
    MULTI_BULK = "*"

    def __init__(self, db=None, password=None, charset='utf8',
                 errors='strict'):
        self.charset = charset
        self.db = db if db is not None else 0
        self.password = password
        self.errors = errors
        self._buffer = ''
        self._bulk_length = None
        self._disconnected = False
        # Format of _multi_bulk_stack elements is:
        # [[length-remaining, [replies] | None]]
        self._multi_bulk_stack = deque()
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
                # we add 2 to _bulk_length to account for \r\n
                if len(self._buffer) < self._bulk_length + 2:
                    return
                data = self._buffer[:self._bulk_length]
                self._buffer = self._buffer[self._bulk_length + 2:]
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
                    r = exceptions.InvalidResponse(
                        "Cannot convert data '%s' to integer" % reply_data)
                    self.responseReceived(r)
                    return
                # requested value may not exist
                if self._bulk_length == -1:
                    self.bulkDataReceived(None)
            # Multi-bulk data (*)
            elif reply_type == self.MULTI_BULK:
                # reply_data will contain the # of bulks we're about to get
                try:
                    multi_bulk_length = int(reply_data)
                except ValueError:
                    r = exceptions.InvalidResponse(
                        "Cannot convert data '%s' to integer" % reply_data)
                    self.responseReceived(r)
                    return
                if multi_bulk_length == -1:
                    self._multi_bulk_stack.append([-1, None])
                    self.multiBulkDataReceived()
                    return
                else:
                    self._multi_bulk_stack.append([multi_bulk_length, []])
                    if multi_bulk_length == 0:
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
            d.addCallback(lambda _res: self.auth(self.password))

        # select the db passsed in
        if self.db:
            d.addCallback(lambda _res: self.select(self.db))

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
        if data[:4] == 'ERR ':
            reply = exceptions.ResponseError(data[4:])
        elif data[:9] == 'NOSCRIPT ':
            reply = exceptions.NoScript(data[9:])
        elif data[:8] == 'NOTBUSY ':
            reply = exceptions.NotBusy(data[8:])
        else:
            reply = exceptions.ResponseError(data)

        if self._request_queue:
            # properly errback this reply
            self._request_queue.popleft().errback(reply)
        else:
            # we should have a request queue. if not, just raise this exception
            raise reply

    def singleLineReceived(self, data):
        """Single line response received."""
        if data == 'none':
            # should this happen here in the client?
            reply = None
        else:
            reply = data

        self.responseReceived(reply)

    def handleMultiBulkElement(self, element):
        top = self._multi_bulk_stack[-1]
        top[1].append(element)
        top[0] -= 1
        if top[0] == 0:
            self.multiBulkDataReceived()

    def integerReceived(self, data):
        """Integer response received."""
        try:
            reply = int(data)
        except ValueError:
            reply = exceptions.InvalidResponse(
                "Cannot convert data '%s' to integer" % data)
        if self._multi_bulk_stack:
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
        the last entry in self._multi_bulk_stack.

        """
        reply = self._multi_bulk_stack.pop()[1]
        if self._multi_bulk_stack:
            self.handleMultiBulkElement(reply)
        else:
            self.handleCompleteMultiBulkData(reply)

    def handleCompleteMultiBulkData(self, reply):
        self.responseReceived(reply)

    def responseReceived(self, reply):
        """Handle a server response.

        If we're waiting for multibulk elements, store this reply. Otherwise
        provide the reply to the waiting request.

        """
        if self._multi_bulk_stack:
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
                raise exceptions.InvalidData(
                    "Error encoding unicode value '%s': %s" % (
                        s.encode(self.charset, 'replace'), e))
        return str(s)

    def _send(self, *args):
        """Encode and send a request

        Uses the 'unified request protocol' (aka multi-bulk)

        """
        cmds = []
        for i in args:
            v = self._encode(i)
            cmds.append('$%s\r\n%s\r\n' % (len(v), v))
        cmd = '*%s\r\n' % len(args) + ''.join(cmds)
        self.transport.write(cmd)

    def send(self, command, *args):
        self._send(command, *args)
        return self.getResponse()


class HiRedisBase(RedisBase):
    """A subclass of the RedisBase protocol that uses the hiredis library for
    parsing.
    """

    def dataReceived(self, data):
        """Receive data.
        """
        self.resetTimeout()
        if data:
            self._reader.feed(data)
        res = self._reader.gets()
        while res is not False:
            if isinstance(res, exceptions.ResponseError):
                self._request_queue.popleft().errback(res)
            else:
                if isinstance(res, basestring) and res == 'none':
                    res = None
                self._request_queue.popleft().callback(res)
            res = self._reader.gets()

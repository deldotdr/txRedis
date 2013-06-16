"""
@file exceptions.py
"""

class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class ResponseError(RedisError):
    pass


class NoScript(RedisError):
    pass


class NotBusy(RedisError):
    pass


class InvalidResponse(RedisError):
    pass


class InvalidData(RedisError):
    pass


class InvalidCommand(RedisError):
    pass

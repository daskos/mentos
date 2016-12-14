#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Wegweiser entrypoint
Main service entry point for Wegweiser service.
Example:
    Entrypoint is::
        $ python driver.py
It expects a lot of enviromental variables for configruation. See help for more details.
Todo:
    *
"""
from collections import deque
from typing import TypeVar
from aioreactive.core import AsyncSink, AsyncSource, AsyncSingleStream
from aioreactive.core import chain

T = TypeVar('T')


class RecordIO(AsyncSource):
    """Operator to parse byte stream into Mesos RecordIO format."""

    def __init__(self, source: AsyncSource) -> None:
        """Creates an operator to parse byte stream.
        Keyword arguments:
        :source types.AsyncSource source: Source stream.
        """
        self._source = source
        self.bytes_buffer = deque()

    async def __astart__(self, sink: AsyncSink) -> AsyncSingleStream:
        _sink = await chain(RecordIO.Sink(self), sink)
        return await chain(self._source, _sink)

    class Sink(AsyncSingleStream):
        """Sink for the created source stream."""

        def __init__(self, source: "RecordIO") -> None:
            super().__init__()
            self._bytes_buffer = source.bytes_buffer

        async def asend(self, value: bytes) -> None:
            """Parse incoming stream of bytes.
            Keeps buffer of incoming bytes.
            """
            self._bytes_buffer.append(value)
            length = self._bytes_buffer[0].split(b'\n', 1)[0]
            number = -len(length)
            length = int(length)
            i = 0
            while i < len(self._bytes_buffer) and number < length:
                number += len(self._bytes_buffer[i])
                i += 1

            if number < length:
                return

            msgs = [self._bytes_buffer.popleft().split(b'\n', 1)[1]]
            number = len(msgs[0])

            while number < length:
                msg = self._bytes_buffer.popleft()
                number += len(msg)
                msgs.append(msg)

            if number > length:
                msg = msgs[-1]
                length, message = msg[
                    (length - number):], msg[:(length - number)]
                msgs[-1] = message
                self._bytes_buffer.appendleft(length)

            msg = b''.join(msgs)
            await self._sink.asend(msg)
            # if not self._bytes_buffer:
            #     await self._sink.aclose()


def recordio(source: AsyncSource):
    """Parse source stream as a byte stream of RecordIO messages.
    xs = recordio
    Returns an observable sequence containing binary messages.
    """
    return RecordIO(source)

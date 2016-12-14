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
        self.first = True
        self.count_bytes = 0

    async def __astart__(self, sink: AsyncSink) -> AsyncSingleStream:
        _sink = await chain(RecordIO.Sink(self), sink)
        return await chain(self._source, _sink)

    class Sink(AsyncSingleStream):
        """Sink for the created source stream."""

        def __init__(self, source: "RecordIO") -> None:
            super().__init__()
            self._first = source.first
            self._count_bytes = source.count_bytes

        async def asend(self, value: bytes) -> None:
            """Parse incoming stream of bytes.
            Keeps buffer of incoming bytes.
            """
            if not value:
                return 
            if self._first:
                self._count_bytes = int(value)
                self._first = False
                return
            try:
                msg = value[:self._count_bytes]
                self._count_bytes = int(value[self._count_bytes:])
            except ValueError:
                self._count_bytes = 0
                self._first = True
                # This happens when the connection is closed, there are no more records
                # we just ignore and return.
                return
            await self._sink.asend(msg)

            # if not self._bytes_buffer:
            #     await self._sink.aclose()


def recordio(source: AsyncSource):
    """Parse source stream as a byte stream of RecordIO messages.
    xs = recordio
    Returns an observable sequence containing binary messages.
    """
    return RecordIO(source)

from rx import Observable, AnonymousObservable
from rx.internal import ArgumentOutOfRangeException
from rx.internal import extensionmethod
from collections import deque


@extensionmethod(Observable)
def recordiorx(self):
    """Returns a specified number of contiguous elements from the start of
    an observable sequence, using the specified scheduler for the edge case
    of take(0).
    1 - source.take(5)
    2 - source.take(0, rx.Scheduler.timeout)
    Keyword arguments:
    count -- The number of elements to return.
    scheduler -- [Optional] Scheduler used to produce an OnCompleted
        message in case count is set to 0.
    Returns an observable sequence that contains the specified number of
    elements from the start of the input sequence.
    """

    observable = self

    def subscribe(observer):
        bytes_buffer = deque()

        def on_next(value):
            bytes_buffer.append(value)
            length = bytes_buffer[0].split(b'\n', 1)[0]
            number = -len(length)
            length = int(length)
            i = 0
            while i < len(bytes_buffer) and number < length:
                number += len(bytes_buffer[i])
                i += 1

            if number < length:
                return

            msgs = [bytes_buffer.popleft().split(b'\n', 1)[1]]
            number = len(msgs[0])

            while number < length:
                msg = bytes_buffer.popleft()
                number += len(msg)
                msgs.append(msg)

            if number > length:
                msg = msgs[-1]
                length, message = msg[
                    (length - number):], msg[:(length - number)]
                msgs[-1] = message
                bytes_buffer.appendleft(length)

            msg = b''.join(msgs)
            observer.on_next(msg)

        return observable.subscribe(on_next, observer.on_error, observer.on_completed)
    return AnonymousObservable(subscribe)

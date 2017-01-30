
from __future__ import unicode_literals
import socket

try:
    ConnectionRefusedError = ConnectionRefusedError
except NameError:# pragma: no cover
	ConnectionRefusedError = socket.error


class MesosError(Exception):
    pass


class FailedRetry(MesosError):
    pass


class ConnectError(MesosError):

    def __init__(self, endpoint):
        self.endpoint = endpoint


class MasterRedirect(MesosError):

    def __init__(self, location):
        self.location = location


class NoLeadingMaster(MesosError):
    pass


class BadSubscription(MesosError):
    pass


class ConnectionLost(MesosError):
    pass


class BadMessage(MesosError):
    pass


class ExecutorException(MesosError):
    pass

class NoRedirectException(MesosError):
    pass

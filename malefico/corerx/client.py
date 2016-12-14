HTTP_STATUS_CODES_TO_RETRY = [500, 502, 503, 504]


class FailedRequest(Exception):
    """
    A wrapper of all possible exception during a HTTP request
    """
    code = 0
    message = ''
    url = ''
    raised = ''

    def __init__(self, *, raised='', message='', code='', url=''):
        self.raised = raised
        self.message = message
        self.code = code
        self.url = url

        super().__init__("code:{c} url={u} message={m} raised={r}".format(
            c=self.code, u=self.url, m=self.message, r=self.raised))


async def send_http(session, method, url, *,
                    retries=1,
                    interval=0.9,
                    backoff=3,
                    read_timeout=15.9,
                    http_status_codes_to_retry=HTTP_STATUS_CODES_TO_RETRY,
                    **kwargs):
    """
    Sends a HTTP request and implements a retry logic.

    Arguments:
        session (obj): A client aiohttp session object
        method (str): Method to use
        url (str): URL for the request
        retries (int): Number of times to retry in case of failure
        interval (float): Time to wait before retries
        backoff (int): Multiply interval by this factor after each failure
        read_timeout (float): Time to wait for a response
    """
    backoff_interval = interval
    raised_exc = None
    attempt = 0

    if method not in ['get', 'patch', 'post']:
        raise ValueError

    if retries == -1:  # -1 means retry indefinitely
        attempt = -1
    elif retries == 0:  # Zero means don't retry
        attempt = 1
    else:  # any other value means retry N times
        attempt = retries + 1

    while attempt != 0:
        if raised_exc:
            log.error('caught "%s" url:%s method:%s, remaining tries %s, '
                      'sleeping %.2fsecs', raised_exc, method.upper(), url,
                      attempt, backoff_interval)
            await asyncio.sleep(backoff_interval)
            # bump interval for the next possible attempt
            backoff_interval = backoff_interval * backoff
        log.info('sending %s %s with %s', method.upper(), url, kwargs)
        try:
            with aiohttp.Timeout(timeout=read_timeout):
                async with getattr(session, method)(url, **kwargs) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                        except json.decoder.JSONDecodeError as exc:
                            log.error(
                                'failed to decode response code:%s url:%s '
                                'method:%s error:%s response:%s',
                                response.status, url, method.upper(), exc,
                                response.reason
                            )
                            raise aiohttp.errors.HttpProcessingError(
                                code=response.status, message=exc.msg)
                        else:
                            log.info('code:%s url:%s method:%s response:%s',
                                     response.status, url, method.upper(),
                                     response.reason)
                            raised_exc = None
                            return data
                    elif response.status in http_status_codes_to_retry:
                        log.error(
                            'received invalid response code:%s url:%s error:%s'
                            ' response:%s', response.status, url, '',
                            response.reason
                        )
                        raise aiohttp.errors.HttpProcessingError(
                            code=response.status, message=response.reason)
                    else:
                        try:
                            data = await response.json()
                        except json.decoder.JSONDecodeError as exc:
                            log.error(
                                'failed to decode response code:%s url:%s '
                                'error:%s response:%s', response.status, url,
                                exc, response.reason
                            )
                            raise FailedRequest(
                                code=response.status, message=exc,
                                raised=exc.__class__.__name__, url=url)
                        else:
                            log.warning('received %s for %s', data, url)
                            print(data['errors'][0]['detail'])
                            raised_exc = None
        except (aiohttp.errors.ClientResponseError,
                aiohttp.errors.ClientRequestError,
                aiohttp.errors.ClientOSError,
                aiohttp.errors.ClientDisconnectedError,
                aiohttp.errors.ClientTimeoutError,
                asyncio.TimeoutError,
                aiohttp.errors.HttpProcessingError) as exc:
            try:
                code = exc.code
            except AttributeError:
                code = ''
            raised_exc = FailedRequest(code=code, message=exc, url=url,
                                       raised=exc.__class__.__name__)
        else:
            raised_exc = None
            break

        attempt -= 1

    if raised_exc:
        raise raised_exc

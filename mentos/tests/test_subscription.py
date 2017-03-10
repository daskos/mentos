import time
from subprocess import PIPE, Popen

import pytest
from mentos.states import States
from mentos.subscription import Event, Subscription
from mentos.utils import encode_data
from tornado import gen


@pytest.mark.gen_test(run_sync=False, timeout=600)
def test_subscription(loop, mocker):
    # TODO: split this case to smaller ones
    framework_info = {'user': 'Test',
                      'name': 'test',
                      'capabilities': [],
                      'failover_timeout': 100000000,
                      'hostname': 'localhost'}

    handler = mocker.Mock()
    handlers = {Event.SUBSCRIBED: handler,
                Event.HEARTBEAT: handler,
                Event.OFFERS: handler,
                Event.SHUTDOWN: handler}

    sub = Subscription(framework_info, 'zk://localhost:2181',
                       '/api/v1/scheduler', handlers, timeout=1, loop=loop)
    assert sub.state.current_state == States.CLOSED
    yield sub.start()

    yield sub.ensure_safe([States.SUBSCRIBING, States.SUBSCRIBED])
    assert sub.state.current_state in [States.SUBSCRIBING, States.SUBSCRIBED]

    assert 'framework_id' not in sub.framework
    assert sub.connection is not None

    # Have to wait for some time for to this to happen
    yield sub.ensure_safe([States.SUBSCRIBED])

    assert sub.state.current_state == States.SUBSCRIBED
    assert handler.call_count >= 2
    assert 'id' in sub.framework

    assert handler.call_args_list[0][0][0][
        'framework_id'] == sub.framework['id']
    # assert handler.call_args_list[1][0][0]['type'] == 'HEARTBEAT'

    first_id = sub.framework['id']
    first_mesos_id = sub.mesos_stream_id

    assert sub.mesos_stream_id is not None
    yield sub.send({})
    # with pytest.raises(exc.BadMessage):

    resp = yield sub.send({
        'type': 'MESSAGE',
        'message': {
            'agent_id': {
                'value': 'aa'
            },
            'executor_id': {
                'value': ''
            },
            'data': encode_data(b's')
        }
    })

    assert resp.code == 202
    assert resp.effective_url == sub.connection.endpoint + sub.api_path

    if sub.master_info.info['port'] == 5050:  # pragma: no cover
        active = 'mesos_master_0'
    elif sub.master_info.info['port'] == 6060:  # pragma: no cover
        active = 'mesos_master_1'
    else:  # pragma: no cover
        active = 'mesos_master_2'

    # TODO: factor out to test utils
    p = Popen(['docker-compose restart %s' % active], shell=True,
              stdout=PIPE, stderr=PIPE)
    p.wait()

    time.sleep(20)
    yield gen.sleep(5)
    yield sub.ensure_safe([States.SUBSCRIBED])

    assert sub.state.current_state == States.SUBSCRIBED
    assert first_id == sub.framework['id']
    assert first_mesos_id != sub.mesos_stream_id

    resp = yield sub.send({
        'type': 'MESSAGE',
        'message': {
            'agent_id': {
                'value': 'aa'
            },
            'executor_id': {
                'value': ''
            },
            'data': encode_data(b's')
        }
    })

    assert resp.code == 202
    assert resp.effective_url == sub.connection.endpoint + sub.api_path

    sub.close()
    assert sub.closing is True


@pytest.mark.gen_test(run_sync=True, timeout=600)
def test_bad_subscription(loop, mocker):
    framework_info = {}

    handler = mocker.Mock()
    handlers = {Event.SUBSCRIBED: handler,
                Event.HEARTBEAT: handler,
                Event.OFFERS: handler,
                Event.SHUTDOWN: handler}

    sub = Subscription(framework_info, 'zk://localhost:2181',
                       '/api/v1/scheduler', handlers, timeout=1, loop=loop)

    assert sub.state.current_state == States.CLOSED
    yield sub.start()
    yield gen.sleep(5)
    assert sub.state.current_state == States.CLOSED

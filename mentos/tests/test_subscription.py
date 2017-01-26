import pytest
from tornado import gen
from mock import call
from mentos import states
from mentos import exceptions as exc
from mentos import subscription
from mentos.utils import encode_data
from subprocess import Popen,PIPE
import time
import random
@pytest.mark.gen_test(run_sync=False, timeout=600)
def test_subscription(io_loop, mocker):
    subm = {
        "user": "Test",
        "name": "test",
        "capabilities": [],
        "failover_timeout": 100000000,
        "hostname": "localhost"
    }

    handler = mocker.Mock()

    sub = subscription.Subscription(subm, "zk://localhost:2181", "/api/v1/scheduler", {subscription.Event.SUBSCRIBED: handler,
                                                                                       subscription.Event.HEARTBEAT: handler,
                                                                                       subscription.Event.OFFERS:handler,
                                                                                       subscription.Event.SHUTDOWN: handler},
                                    timeout=1, loop=io_loop)

    assert sub.state.current_state == states.States.CLOSED
    yield sub.start()

    yield sub.ensure_safe([states.States.SUBSCRIBING])


    assert sub.state.current_state == states.States.SUBSCRIBING

    assert "framework_id" not in sub.framework
    assert sub.connection != None

    #Have to wait for some time for to this to happen
    yield sub.ensure_safe([states.States.SUBSCRIBED])

    assert sub.state.current_state == states.States.SUBSCRIBED

    assert handler.call_count >=2
    assert "id" in sub.framework

    assert handler.call_args_list[0][0][0]["framework_id"] == sub.framework["id"]
    assert handler.call_args_list[1][0][0]["type"] == "HEARTBEAT"

    first_id = sub.framework["id"]
    first_mesos_id = sub.mesos_stream_id

    assert sub.mesos_stream_id != None

    with pytest.raises(exc.BadMessage):
         yield sub.send({})

    resp= yield sub.send({

        "type": "MESSAGE",
        "message": {
            "agent_id": {
                "value": "aa"
            },
            "executor_id": {
                "value": ""
            },
            "data": encode_data(b"s")
        }
    })

    assert resp.code == 202
    assert resp.effective_url == sub.connection.endpoint+sub.api_path

    time.sleep(random.randint(1, 20))

    if sub.master_info.info["port"] == 5050:  # pragma: no cover
        active = "mesos_master_0"
    elif sub.master_info.info["port"] == 6060:  # pragma: no cover
        active = "mesos_master_1"
    else:
        active = "mesos_master_2"

    p = Popen(["docker-compose restart %s" % active], shell=True,
              stdout=PIPE, stderr=PIPE)

    a = p.wait()

    yield gen.sleep(random.randint(1,5))

    time.sleep(random.randint(10,30))

    assert sub.state.current_state in (states.States.SUSPENDED,states.States.SUBSCRIBING)

    yield sub.ensure_safe([states.States.SUBSCRIBED])

    assert sub.state.current_state == states.States.SUBSCRIBED

    assert first_id == sub.framework["id"]
    assert first_mesos_id != sub.mesos_stream_id

    resp = yield sub.send({

        "type": "MESSAGE",
        "message": {
            "agent_id": {
                "value": "aa"
            },
            "executor_id": {
                "value": ""
            },
            "data": encode_data(b"s")
        }
    })

    assert resp.code == 202
    assert resp.effective_url == sub.connection.endpoint + sub.api_path


    sub.close()
    assert sub.closing == True



@pytest.mark.gen_test(run_sync=True, timeout=600)
def test_bad_subscription(io_loop, mocker):
    subm = {

    }

    handler = mocker.Mock()

    sub = subscription.Subscription(subm, "zk://localhost:2181", "/api/v1/scheduler", {subscription.Event.SUBSCRIBED: handler,
                                                                                       subscription.Event.HEARTBEAT: handler,
                                                                                       subscription.Event.OFFERS:handler,
                                                                                       subscription.Event.SHUTDOWN: handler},
                                    timeout=1, loop=io_loop)

    assert sub.state.current_state == states.States.CLOSED
    yield sub.start()
    yield gen.sleep(5)
    assert sub.state.current_state == states.States.CLOSED


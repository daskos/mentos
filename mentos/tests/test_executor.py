import os

import pytest
from mentos import ExecutorDriver
from mentos.exceptions import ExecutorException
from mentos.subscription import Subscription


def test_executor_event_handlers(mocker):
    executor = mocker.Mock()

    os.environ['MESOS_AGENT_ENDPOINT'] = 'localhost:5051'
    os.environ['MESOS_FRAMEWORK_ID'] = 'test'
    os.environ['MESOS_EXECUTOR_ID'] = 'test'
    driver = ExecutorDriver(executor)

    driver.on_subscribed({'agent_info': 'test',
                          'executor_info': {'executor_id': {'value': 'test'}},
                          'framework_info': {'id': {'value': 'test'}}})
    driver.on_close()
    driver.on_launch_group({'task': {'task_id': {'value': 'test'}}})
    with pytest.raises(ExecutorException):
        driver.on_launch({'task': {'task_id': {'value': 'test'}}})
    driver.on_launch({'task': {'task_id': {'value': 'test1'}}})
    driver.on_kill({'task_id': {'value': 'test'}})
    driver.on_acknowledged({'uuid': b'Pz3GkJaxS7S0TWdKNrvQIw==',
                            'task_id': {'value': 'test'}})
    driver.on_message({'data': b'a'})
    driver.on_shutdown()
    driver.on_error({'message': 'test'})

    executor.on_registered.assert_called_once()
    assert executor.on_shutdown.call_count == 2
    assert executor.on_launch.call_count == 2
    executor.on_kill.assert_called_once()
    executor.on_acknowledged.assert_called_once()
    executor.on_message.assert_called_once()


@pytest.mark.gen_test(timeout=60)
def test_executor_driver_callbacks(mocker):
    executor = mocker.Mock()
    os.environ['MESOS_AGENT_ENDPOINT'] = 'localhost:5051'
    os.environ['MESOS_FRAMEWORK_ID'] = 'test'
    os.environ['MESOS_EXECUTOR_ID'] = 'test'
    driver = ExecutorDriver(executor)

    # start = mocker.patch.object(ExecutorDriver, 'start')
    stop = mocker.patch.object(ExecutorDriver, 'stop')
    send = mocker.patch.object(Subscription, 'send')

    driver.start()
    driver.stop()

    driver.update({'task_id': 'test', 'state': 'TASK_RUNNING'})
    expected = {'type': 'UPDATE',
                'framework_id': {'value': 'test'},
                'executor_id': {'value': 'test'},
                'update': {'status': {'task_id': 'test',
                                      'state': 'TASK_RUNNING',
                                      'timestamp': 1485780553,
                                      'uuid': '8MNS2ohyRYCUtA04er89aw==',
                                      'source': 'SOURCE_EXECUTOR'}}}
    send.asert_called_once_with(expected)

    driver.message(b'message')
    expected = {'type': 'MESSAGE',
                'framework_id': {'value': 'test'},
                'executor_id': {'value': 'test'},
                'message': {'data': 'bWVzc2FnZQ=='}}
    stop.asert_called_once_with(expected)

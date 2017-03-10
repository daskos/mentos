from time import sleep

import pytest
from mentos import SchedulerDriver
from mentos.subscription import Subscription
from mock import call
from tornado import gen


def test_scheduler_event_handlers(mocker):
    sched = mocker.Mock()
    driver = SchedulerDriver(sched, name='test')

    driver.on_error({'message': 'test'})
    driver.on_heartbeat({'message': 'test'})
    driver.on_subscribed({'framework_id': 'test'})
    driver.on_offers({'offers': ['test']})
    driver.on_rescind_inverse({'offer_id': 'test'})
    driver.on_rescind({'offer_id': 'test'})
    driver.on_update({'status': 'test'})
    driver.on_message({'executor_id': 'test',
                       'agent_id': 'test',
                       'data': 'test',
                       'uuid': 'test'})
    driver.on_failure({'agent_id': 'test'})
    driver.on_failure({'agent_id': 'test',
                       'executor_id': 'test',
                       'status': 'test',
                       'uuid': 'test'})

    sched.on_executor_lost.assert_called_once()
    sched.on_message.assert_called_once()
    sched.on_rescinded.assert_called_once()
    sched.on_rescind_inverse.assert_called_once()
    sched.on_reregistered.assert_called_once()
    sched.on_offers.assert_called_once()
    sched.on_agent_lost.assert_called_once()
    sched.on_heartbeat.assert_called_once()
    sched.on_update.assert_called_once()
    sched.on_reregistered.assert_called_once()
    sched.on_error.assert_called_once()


@pytest.mark.gen_test(timeout=60)
def test_scheduler_driver_callbacks(mocker):
    sched = mocker.Mock()
    driver = SchedulerDriver(sched, name='test')
    send = mocker.patch.object(Subscription, 'send')
    # kill = mocker.patch.object(SchedulerDriver, 'kill')
    # reconcile = mocker.patch.object(SchedulerDriver, 'reconcile')
    # decline = mocker.patch.object(SchedulerDriver, 'decline')
    # launch = mocker.patch.object(SchedulerDriver, 'launch')
    # accept = mocker.patch.object(SchedulerDriver, 'accept')
    # revive = mocker.patch.object(SchedulerDriver, 'revive')
    # acknowledge = mocker.patch.object(SchedulerDriver, 'acknowledge')
    # message = mocker.patch.object(SchedulerDriver, 'message')
    # shutdown = mocker.patch.object(SchedulerDriver, 'shutdown')
    # teardown = mocker.patch.object(SchedulerDriver, 'teardown')

    driver.start()
    driver.stop()
    with driver:
        sleep(0.1)

    driver.start()
    sleep(5)

    driver.request(['test'])
    driver.kill('test', 'test')
    driver.reconcile('test', 'test')
    driver.reconcile('test', None)
    driver.decline(['test'])
    driver.decline(['test'], ['test'])
    driver.launch(['test'], ['test'])
    driver.launch(['test'], None)
    driver.accept(['test'], ['test'], ['test'])
    driver.accept(['test'], None)
    driver.revive()
    driver.acknowledge({'task_id': 'test',
                        'agent_id': 'test',
                        'executor_id': 'test',
                        'status': 'test',
                        'uuid': 'test'})
    driver.message('test', 'test', b'test')
    driver.shutdown('test', 'test')
    driver.teardown('test')

    yield gen.sleep(5)

    test_id = {'value': 'test'}
    call_list = [
        {'type': 'REQUEST',
         'requests': ['test']},
        {'type': 'KILL',
         'kill': {'task_id': test_id,
                  'agent_id': test_id}},
        {'type': 'RECONCILE',
         'reconcile': {'tasks': [{'task_id': test_id,
                                  'agent_id': test_id}]}},
        {'type': 'RECONCILE',
         'reconcile': {'tasks': []}},
        {'type': 'DECLINE',
         'decline': {'offer_ids': ['test']}},
        {'type': 'DECLINE',
         'decline': {'offer_ids': ['test'],
                     'filters': ['test']}},
        {'type': 'ACCEPT',
         'accept': {'offer_ids': ['test'],
                    'operations': [{'type': 'LAUNCH',
                                    'launch': {'task_infos': ['test']}}]}},
        {'type': 'DECLINE',
         'decline': {'offer_ids': ['test']}},
        {'type': 'ACCEPT',
         'accept': {'offer_ids': ['test'],
                    'operations': ['test'],
                    'filters': ['test']}},
        {'type': 'DECLINE',
         'decline': {'offer_ids': ['test']}},
        {'type': 'REVIVE'},
        {'type': 'ACKNOWLEDGE',
         'acknowledge': {'agent_id': 'test',
                         'task_id': 'test',
                         'uuid': 'test'}},
        {'type': 'MESSAGE',
         'message': {'agent_id': test_id,
                     'executor_id': test_id,
                     'data': 'dGVzdA=='}},
        {'type': 'SHUTDOWN',
         'kill': {'executor_id': test_id,
                  'agent_id': test_id}},
        {'type': 'TEARDOWN'}
    ]

    assert send.call_args_list == list(map(call, call_list))

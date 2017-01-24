#
# from mentos import ExecutorDriver
#
# def test_executor_event_handlers(mocker):
#     executor = mocker.Mock()
#
#     driver = ExecutorDriver(executor)
#
#     driver.update({})
#
#     driver.message( 'message')
#     driver.start()
#     driver.stop()
#
#     executor.on_registered.assert_called_once()
#     executor.on_reregistered.assert_called_once()
#     executor.on_disconnected.assert_called_once()
#     executor.on_launch.assert_called_once()
#     executor.on_kill.assert_called_once()
#     executor.on_message.assert_called_once()
#     executor.on_shutdown.assert_called_once()
#     executor.on_error.assert_called_once()
#
#
# def test_executor_driver_callbacks(mocker):
#     executor = mocker.Mock()
#     driver = ExecutorDriver(executor)
#
#
#
#     driver.start()
#     driver.stop()
#
#     driver.update({ 'task_id':'test', 'state':'TASK_RUNNING'})
#     driver.message('message')
#
#
#
#     driver.start.assert_called_once()
#     driver.stop.assert_called_once()
#
#     driver.sendStatusUpdate.assert_called_once()
#     driver.sendFrameworkMessage.assert_called_once()

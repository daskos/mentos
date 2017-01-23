import time

from mock import patch, Mock
from tornado import testing, concurrent

from mentos import retry
from mentos import exceptions as exc


class RetryTests(testing.AsyncTestCase):

    @testing.gen_test
    def test_clear_timings(self):
        policy = retry.RetryPolicy.forever()

        request1 = Mock()
        request2 = Mock()

        yield policy.enforce(request1)
        yield policy.enforce(request1)
        yield policy.enforce(request2)

        self.assertEqual(len(policy.timings[id(request1)]), 2)
        self.assertEqual(len(policy.timings[id(request2)]), 1)

        policy.clear(request1)

        self.assertTrue(id(request1) not in policy.timings)
        self.assertEqual(len(policy.timings[id(request2)]), 1)

    def test_once(self):
        policy = retry.RetryPolicy.once()

        self.assertEqual(policy.try_limit, 1)
        self.assertEqual(policy.sleep_func([1, 2, 3]), None)

    def test_n_times(self):
        policy = retry.RetryPolicy.n_times(3)

        self.assertEqual(policy.try_limit, 3)
        self.assertEqual(policy.sleep_func([1, 2, 3]), None)

    def test_forever(self):
        policy = retry.RetryPolicy.forever()

        self.assertEqual(policy.try_limit, None)
        self.assertEqual(policy.sleep_func([1, 2, 3]), None)

    def test_exponential_backoff(self):
        policy = retry.RetryPolicy.exponential_backoff()

        self.assertEqual(policy.try_limit, None)

        timings = []

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 1)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 2)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 4)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 8)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 16)

    def test_exponential_backoff_with_max_and_base(self):
        policy = retry.RetryPolicy.exponential_backoff(base=3, maximum=25)

        self.assertEqual(policy.try_limit, None)

        timings = []

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 1)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 3)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 9)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 25)

        wait = policy.sleep_func(timings)
        timings.append(wait)

        self.assertEqual(wait, 25)

    @patch.object(retry, "time")
    def test_until_elapsed(self, mock_time):
        state = {"now": time.time()}

        policy = retry.RetryPolicy.until_elapsed(timeout=4)

        def increment_time(*args):
            state["now"] += 1
            return state["now"]

        mock_time.time.side_effect = increment_time

        self.assertEqual(policy.try_limit, None)

        timings = []

        wait = policy.sleep_func(timings)
        timings.append(state["now"])

        self.assertEqual(wait, 3)
        self.assertEqual(policy.sleep_func(timings), 3)
        self.assertEqual(policy.sleep_func(timings), 2)
        self.assertEqual(policy.sleep_func(timings), 1)
        self.assertEqual(policy.sleep_func(timings), 0)

    @testing.gen_test
    def test_failed_enforcement(self):
        policy = retry.RetryPolicy.once()

        request = Mock()

        yield policy.enforce(request)

        with self.assertRaises(exc.FailedRetry):
            yield policy.enforce(request)

    @testing.gen_test
    def test_timeout_failure(self):

        def in_the_past(_):
            return -1

        request = Mock()

        policy = retry.RetryPolicy(try_limit=3, sleep_func=in_the_past)

        yield policy.enforce(request)

        with self.assertRaises(exc.FailedRetry):
            yield policy.enforce(request)

    @patch.object(retry.gen, "sleep")
    @testing.gen_test
    def test_enforcing_wait_time_sleeps(self, mock_gen_sleep):
        f = concurrent.Future()
        f.set_result(None)

        mock_gen_sleep.return_value = f

        def in_the_future(_):
            return 60

        request = Mock()

        policy = retry.RetryPolicy(try_limit=3, sleep_func=in_the_future)

        yield policy.enforce(request)

        self.assertFalse(mock_gen_sleep.called)

        yield policy.enforce(request)

        mock_gen_sleep.assert_called_once_with(60)

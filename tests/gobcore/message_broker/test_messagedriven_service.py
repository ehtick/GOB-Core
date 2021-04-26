import mock
import unittest
from unittest.mock import MagicMock, call

from tests.gobcore import fixtures

# from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker import messagedriven_service
from gobcore.message_broker.messagedriven_service import \
    MessagedrivenService, _on_message, STATUS_FAIL, RUNS_IN_OWN_THREAD
from gobcore.status.heartbeat import STATUS_OK, STATUS_START


def handler(msg):

    global return_message
    return return_message


def mock_get_on_message(service):

    global return_method
    return return_method


class TestMessageDrivenServiceFunctions(unittest.TestCase):

    @mock.patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    @mock.patch("gobcore.message_broker.messagedriven_service.contains_notification")
    @mock.patch("gobcore.message_broker.messagedriven_service.send_notification")
    @mock.patch("gobcore.message_broker.messagedriven_service.process_issues")
    def test_on_message(self, mock_process_issues, mock_send_notification, mock_contains_notification, mock_heartbeat):

        global return_message

        # mock_contains_notification = True

        # setup mocks and fixtures
        mock_handler = MagicMock()
        # setup expectations
        return_message = fixtures.random_string()
        mock_handler.return_value = return_message
        service = fixtures.get_service_fixture(mock_handler)
        single_service = [v for v in service.values()][0]

        message = {}
        connection = MagicMock()

        return_queue = single_service['report']

        # on_message = messagedriven_service._get_on_message(single_service)
        result = messagedriven_service._on_message(connection, single_service, message)

        # The result should be True
        self.assertTrue(result)

        mock_heartbeat.progress.assert_has_calls([
            call(connection, single_service, message, STATUS_START),
            call(connection, single_service, message, STATUS_OK),
        ])
        # The message handler should be called with the message
        mock_handler.assert_called_with(message)

        # The return message should be published on the return queue
        connection.publish.assert_called_with(return_queue['exchange'], return_queue['key'], return_message)

        mock_process_issues.assert_called_with(return_message)
        mock_send_notification.assert_called_with(return_message)

    @mock.patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    def test_on_message_fail(self, mock_heartbeat):
        connection = MagicMock()
        service = {
            'handler': MagicMock(side_effect=Exception('exception msg')),
        }

        with self.assertRaises(Exception):
            _on_message(connection, service, 'some message')

        mock_heartbeat.progress.assert_called_with(connection, service, 'some message', STATUS_FAIL, 'exception msg')

    @mock.patch("gobcore.message_broker.messagedriven_service.MessagedrivenService")
    def test_messagedriven_service_wrapper(self, mock_service_class):
        services = ['service 1', 'service 2']
        name = 'some name'
        params = {'param1': 'value1'}
        messagedriven_service.messagedriven_service(services, name, params)
        mock_service_class.assert_called_with(services, name, params)
        mock_service_class.return_value.start.assert_called_once()


@mock.patch("gobcore.message_broker.messagedriven_service.initialize_message_broker")
class TestMessageDrivenService(unittest.TestCase):

    def test_get_service(self, mock_init_broker):
        services = {
            "a": {
                "queue": "q1",
                "handler": "h1",
            },
            "b": {
                "queue": lambda: "q2",
                "handler": "h2",
            }
        }
        messagedriven_service = MessagedrivenService(services, 'name', {})
        self.assertTrue(messagedriven_service._get_service("q1") == services["a"])

        # Queue lambda function is replaced with its result
        self.assertTrue(messagedriven_service._get_service("q2") == {
            **services["b"],
            "queue": "q2"
        })

    # @mock.patch("gobcore.message_broker.messagedriven_service.AsyncConnection")
    # def test_messagedriven_service(self, mocked_connection, mock_init_broker):

    #     global return_method
    #     return_method = fixtures.random_string()

    #     service_definition = fixtures.get_service_fixture(handler)
    #     single_service = [v for v in service_definition.values()][0]

    #     expected_queue = single_service['queue']

    #     messagedriven_service = MessagedrivenService(service_definition, 'Any name')
    #     messagedriven_service._init = MagicMock()
    #     messagedriven_service._start_threads = MagicMock()
    #     messagedriven_service._heartbeat_loop = MagicMock()
    #     messagedriven_service.keep_running = False
    #     messagedriven_service.start()

    #     mock_init_broker.assert_called_with()
    #     mocked_connection.assert_called_with(CONNECTION_PARAMS, {})
    #     mocked_connection.return_value.__enter__.return_value.subscribe\
    #         .assert_called_with([expected_queue], mock.ANY)  # Inner function

    #     messagedriven_service._heartbeat_loop.assert_called_once()
    #     messagedriven_service._start_threads.assert_not_called()

    def test_messagedriven_service_multithreaded(self, mock_init_broker):
        service_definition = fixtures.get_service_fixture(handler)

        messagedriven_service = MessagedrivenService(service_definition, 'Some name', {'thread_per_service': True})
        messagedriven_service._init = MagicMock()
        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False
        messagedriven_service.start()

        mock_init_broker.assert_called_with()
        messagedriven_service._heartbeat_loop.assert_called_once()
        messagedriven_service._start_threads.assert_called_once()

    def test_messagedriven_service_start(self, mock_init_broker):
        services = {
            "s1": {
                'queue': "q1",
                'handler': MagicMock(),
                'report': {
                    'exchange': "e1",
                    'key': "k1"
                }
            },
            "s2": {
                'queue': "q2",
                'handler': MagicMock(),
                RUNS_IN_OWN_THREAD: True,
                'report': {
                    'exchange': "e2",
                    'key': "k2"
                }
            },
            "s3": {
                'queue': "q3",
                'handler': MagicMock(),
                RUNS_IN_OWN_THREAD: False,
                'report': {
                    'exchange': "e3",
                    'key': "k3"
                }
            },
        }
        messagedriven_service = MessagedrivenService(services, 'Some name', {'thread_per_service': True})

        messagedriven_service._init = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False

        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service.start()

        messagedriven_service._start_threads.assert_called_with(['q1', 'q2', 'q3'])
        messagedriven_service._start_thread.assert_not_called()

        messagedriven_service = MessagedrivenService(services, 'Some name')

        messagedriven_service._init = MagicMock()
        messagedriven_service._heartbeat_loop = MagicMock()
        messagedriven_service.keep_running = False

        messagedriven_service._start_threads = MagicMock()
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service.start()

        messagedriven_service._start_threads.assert_called_with(['q2'])
        messagedriven_service._start_thread.assert_called_with(['q1', 'q3'])

    def test_messagedriven_service_startthreads(self, mock_init_broker):
        messagedriven_service = MessagedrivenService({}, 'Some name', {'thread_per_service': True})
        queues = ['queue1', 'queue2']
        messagedriven_service._start_thread = MagicMock()
        messagedriven_service._start_threads(queues)

        messagedriven_service._start_thread.assert_has_calls([
            call(['queue1']),
            call(['queue2']),
        ])

    @mock.patch("gobcore.message_broker.messagedriven_service.StoppableThread")
    def test_start_thread(self, mock_stoppable_thread, mock_init_broker):
        messagedriven_service = MessagedrivenService({}, 'Some name', {'thread_per_service': True})
        queues_arg = ['some queue', 'some other queue']
        mock_stoppable_thread.return_value = MagicMock()
        result = messagedriven_service._start_thread(queues_arg)
        mock_stoppable_thread.assert_called_with(
            target=messagedriven_service._listen,
            args=(queues_arg,), name="QueueHandler")
        mock_stoppable_thread.return_value.start.assert_called_once()
        self.assertEqual(mock_stoppable_thread.return_value, result)

        self.assertEqual([{
            'queues': queues_arg,
            'thread': mock_stoppable_thread.return_value,
        }], messagedriven_service.threads)

    @mock.patch("gobcore.message_broker.messagedriven_service.sys.exit")
    def test_init_exception(self, mock_exit, mock_init_messagebroker):
        mock_init_messagebroker.side_effect = Exception

        MessagedrivenService({}, 'name', {})

        mock_exit.assert_called_with(1)

    @mock.patch("gobcore.message_broker.messagedriven_service._on_message")
    def test_on_message(self, mock_on_message, mock_init_messagebroker):
        messagedriven_service = MessagedrivenService({}, 'name', {})
        messagedriven_service._get_service = MagicMock()
        messagedriven_service._on_message('connection', 'exchange', 'queue', 'key', 'msg')
        messagedriven_service._get_service.assert_called_with('queue')
        mock_on_message.assert_called_with('connection', messagedriven_service._get_service.return_value, 'msg')

    @mock.patch("gobcore.message_broker.messagedriven_service.msg_broker")
    def test_listen(self, patched_msg_broker, mock_init_messagebroker):
        messagedriven_service = MessagedrivenService({}, 'name', {})

        class MockConnection:
            count = 0
            subscribe = MagicMock()

            def is_alive(self):
                self.count += 1
                return self.count != 2

        def stop_check():
            return False

        patched_msg_broker.async_connection.return_value.__enter__.return_value = MockConnection()
        messagedriven_service.check_connection = 0

        messagedriven_service._listen(stop_check, ['q1', 'q1'])

    @mock.patch("gobcore.message_broker.messagedriven_service.Heartbeat")
    @mock.patch("gobcore.message_broker.messagedriven_service.msg_broker")
    def test_heartbeat_loop(self, mock_msg_broker, mock_heartbeat, mock_init_messagebroker):
        messagedriven_service = MessagedrivenService({}, 'name', {})

        class MockConnection:
            subscribe = MagicMock()

            def is_alive(self):
                # Trick to get around infinite while loop, while still being able to run the loop once
                messagedriven_service.keep_running = False
                return True

        mock_msg_broker.async_connection.return_value.__enter__.return_value = MockConnection()
        messagedriven_service.check_connection = 0
        messagedriven_service.heartbeat_interval = 0

        healthy_thread = type('MockThread', (object,), {'is_alive': lambda: True, 'stop': lambda: True})
        unhealthy_thread = type('MockThread', (object,), {'is_alive': lambda: False, 'stop': lambda: True})

        messagedriven_service.threads = [
            {'thread': healthy_thread, 'queues': 'the queues'},
            {'thread': unhealthy_thread, 'queues': 'the queues 2'},
        ]
        messagedriven_service._start_thread = MagicMock(return_value=healthy_thread)

        messagedriven_service._heartbeat_loop()

        messagedriven_service._start_thread.assert_has_calls([call('the queues 2')])
        mock_heartbeat.return_value.send.assert_called_once()

        # Check keyboard int...
        messagedriven_service.keep_running = True

        class MockConnection:
            subscribe = MagicMock()

            def is_alive(self):
                raise KeyboardInterrupt()

        mock_msg_broker.async_connection.return_value.__enter__.return_value = MockConnection()

        messagedriven_service._heartbeat_loop()
        mock_heartbeat.return_value.terminate.assert_called_once()

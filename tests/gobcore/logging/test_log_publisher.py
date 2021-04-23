from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.logging.log_publisher import (
    LogPublisher,
    AuditLogPublisher,
    IssuePublisher,
    AUDIT_LOG_EXCHANGE,
    ISSUE_EXCHANGE,
)


class TestLogPublisher(TestCase):

    def test_constructor(self):
        # Test if a log publisher can be initialized
        publisher = LogPublisher()
        assert(publisher is not None)

    @patch('gobcore.logging.log_publisher.threading.Thread')
    @patch('gobcore.logging.log_publisher.LogPublisher._auto_disconnect')
    @patch('gobcore.logging.log_publisher.get_connection')
    def test_publish(self, patched_connect, patched_auto_disconnect, patched_thread):
        publisher = LogPublisher()
        _auto_disconnect_thread = MagicMock()
        publisher._auto_disconnect_thread = _auto_disconnect_thread
        connection = patched_connect.return_value
        publisher.publish("Level", "Message")
        connection.connect.assert_called_with()
        connection.publish.assert_called_with(publisher._exchange, "Level", "Message", )
        patched_thread.assert_called_with(target=publisher._auto_disconnect, name='AutoDisconnect')
        _auto_disconnect_thread.join.assert_called_with()

    def test_disconnect(self):
        publisher = LogPublisher()
        mock_connection = MagicMock()
        publisher._connection = mock_connection

        publisher._disconnect()
        mock_connection.disconnect.assert_called_once()
        self.assertIsNone(publisher._connection)

        mock_connection.disconnect.reset_mock()
        publisher._disconnect()
        mock_connection.disconnect.assert_not_called()

    def test_auto_disconnect(self):
        publisher = LogPublisher()
        publisher._connection_lock = MagicMock()
        publisher._disconnect = MagicMock()
        publisher._auto_disconnect_timeout = 1

        publisher._auto_disconnect()
        publisher._disconnect.assert_called_once()
        self.assertEqual(0, publisher._auto_disconnect_timeout)


class TestAuditLogPublisher(TestCase):

    def test_init(self):
        publisher = AuditLogPublisher()
        self.assertEqual(AUDIT_LOG_EXCHANGE, publisher._exchange)

    def test_publish_request(self):
        publisher = AuditLogPublisher()
        publisher.publish = MagicMock()

        msg = {'bogus': 'msg'}
        publisher.publish_request(msg)
        publisher.publish.assert_called_with('request', msg)

    def test_publish_response(self):
        publisher = AuditLogPublisher()
        publisher.publish = MagicMock()

        msg = {'bogus': 'msg'}
        publisher.publish_response(msg)
        publisher.publish.assert_called_with('response', msg)


class TestIssuePublisher(TestCase):

    def test_init(self):
        publisher = IssuePublisher()
        self.assertEqual(ISSUE_EXCHANGE, publisher._exchange)

    @patch("gobcore.logging.log_publisher.LogPublisher.publish")
    def test_publish(self, mock_publish):
        publisher = IssuePublisher()

        msg = {'bogus': 'msg'}
        publisher.publish(msg)
        mock_publish.assert_called_with('request', msg)

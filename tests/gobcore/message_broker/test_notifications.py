from unittest import TestCase
from mock import patch
from unittest.mock import MagicMock

from gobcore.message_broker.notifications import (
    listen_to_notifications,
    contains_notification,
    send_notification,
    add_notification,
    get_notification,
    EventNotification,
    DumpNotification,
    _send_notification,
    _listen_to_notifications,
    ExportTestNotification,
)


@patch("gobcore.message_broker.notifications.NOTIFY_EXCHANGE", 'notification exchange')
@patch("gobcore.message_broker.notifications.NOTIFY_BASE_QUEUE", 'base queue')
@patch("gobcore.message_broker.notifications.NOTIFICATION_KEY", 'notification key')
@patch("gobcore.message_broker.notifications.NOTIFICATION_HEADER_FIELDS", ['a', 'b'])
class TestNotifications(TestCase):

    @patch("gobcore.message_broker.notifications._listen_to_notifications")
    def test_listen_to_notifications(self, mock_listen_to_notifications):
        result = listen_to_notifications('any id')
        mock_listen_to_notifications.assert_called_with('notification exchange', 'base queue.any id', '')
        self.assertEqual(result, mock_listen_to_notifications.return_value)

        result = listen_to_notifications('any id', 'any type')
        mock_listen_to_notifications.assert_called_with('notification exchange', 'base queue.any id', 'any type')

    def test_contains_notifications(self):
        result = contains_notification(None)
        self.assertFalse(result, None)

        result = contains_notification({})
        self.assertFalse(result, {})

        result = contains_notification({'any key': 'any value'})
        self.assertFalse(result, None)

        result = contains_notification({'notification key': 'any notification'})
        self.assertTrue(result, 'any notification')

    @patch("gobcore.message_broker.notifications._send_notification")
    def test_send_notifications(self, mock_send_notification):
        send_notification({})
        mock_send_notification.assert_not_called()

        send_notification({'header': {'a': 1, 'c': 3}, 'notification key': {'type': "any type", 'x': 1, 'y': 2}})
        mock_send_notification.assert_called_with(
            'notification exchange',
            notification_type='any type',
            msg={
                'header': {'a': 1, 'b': None},
                'type': "any type",
                'x': 1,
                'y': 2
                })

    def test_add_notification(self):
        notification = EventNotification('any applied', 'any last_event')
        msg = {}
        add_notification(msg, notification)
        expect = {
            'notification key': {
                'type': 'events',
                'contents': {
                    'applied': 'any applied',
                    'last_event': 'any last_event'
                }
            }}
        self.assertEqual(msg, expect)

    def test_get_notification(self):
        result = get_notification({})
        self.assertIsNone(result)

        result = get_notification({
            'header': 'any header',
            'type': 'events',
            'contents': {'applied': 'any applied', 'last_event': 'any last_event'}
        })
        self.assertEqual(result.type, 'events')
        self.assertEqual(result.header, 'any header')
        self.assertEqual(result.contents, {'applied': 'any applied', 'last_event': 'any last_event'})

    @patch("gobcore.message_broker.notifications.get_async_connection")
    @patch("gobcore.message_broker.notifications.get_manager")
    def test__send_notification(self, mock_get_manager, mock_get_async_connection):
        mock_connection = MagicMock()
        mock_manager = MagicMock()
        mock_get_async_connection.return_value.__enter__.return_value = mock_connection
        mock_get_manager.return_value.__enter__.return_value = mock_manager
        _send_notification('any exchange', 'any type', {})
        mock_manager.create_exchange.assert_called_with('any exchange')
        mock_connection.publish.assert_called_with(
            msg={},
            exchange='any exchange',
            key='any type')

    @patch("gobcore.message_broker.notifications.get_manager")
    def test__listen_to_notificiations(self, mock_get_manager):
        mock_manager = MagicMock()
        mock_get_manager.return_value.__enter__.return_value = mock_manager
        result = _listen_to_notifications('any exchange', 'any queue', 'any notification')
        mock_manager.create_exchange.assert_called_with(exchange='any exchange', durable=True)
        mock_manager.create_queue_with_binding.assert_called_with(
            exchange='any exchange', queue='any queue', keys=['any notification'])
        self.assertEqual(result, 'any queue')


class TestDumpNotification(TestCase):

    def test_from_msg(self):
        msg = {
            'type': 'dump',
            'header': {'some': 'header'},
            'contents': {
                'catalog': 'SOME CAT',
                'collection': 'SOME COLL',
            }
        }
        notification = get_notification(msg)
        self.assertIsInstance(notification, DumpNotification)
        self.assertEqual({'some': 'header'}, notification.header)
        self.assertEqual('SOME CAT', notification.contents.get('catalog'))
        self.assertEqual('SOME COLL', notification.contents.get('collection'))


class TestExportTestNotification(TestCase):

    def test_from_msg(self):
        msg = {
            'type': 'export_test',
            'header': {'some': 'header'},
            'contents': {
                'catalogue': 'SOME CAT',
                'collection': 'SOME COLL',
                'product': 'SOME PRODUCT',
            }
        }
        notification = get_notification(msg)
        self.assertIsInstance(notification, ExportTestNotification)
        self.assertEqual({'some': 'header'}, notification.header)
        self.assertEqual('SOME CAT', notification.contents.get('catalogue'))
        self.assertEqual('SOME COLL', notification.contents.get('collection'))
        self.assertEqual('SOME PRODUCT', notification.contents.get('product'))

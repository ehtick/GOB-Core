from unittest import TestCase
from mock import patch
from unittest.mock import MagicMock, call

from gobcore.message_broker.notifications import listen_to_notifications,\
    contains_notifications,\
    send_notifications,\
    add_notification,\
    get_notification,\
    EventNotification,\
    _create_broadcast_exchange,\
    send_broadcast,\
    listen_to_broadcasts

@patch("gobcore.message_broker.notifications.NOTIFY_EXCHANGE", 'notification exchange')
@patch("gobcore.message_broker.notifications.NOTIFY_BASE_QUEUE", 'base queue')
@patch("gobcore.message_broker.notifications.NOTIFICATION_KEY", 'notification key')
@patch("gobcore.message_broker.notifications.NOTIFICATION_HEADER_FIELDS", ['a', 'b'])
class TestNotifications(TestCase):

    @patch("gobcore.message_broker.notifications.listen_to_broadcasts")
    def test_listen_to_notifications(self, mock_listen_to_broadcasts):
        result = listen_to_notifications('any id')
        mock_listen_to_broadcasts.assert_called_with('notification exchange', 'base queue.any id')
        self.assertEqual(result, mock_listen_to_broadcasts.return_value)

    def test_contains_notifications(self):
        result = contains_notifications(None)
        self.assertFalse(result, None)

        result = contains_notifications({})
        self.assertFalse(result, {})

        result = contains_notifications({'any key': 'any value'})
        self.assertFalse(result, None)

        result = contains_notifications({'notification key': 'any notification'})
        self.assertTrue(result, 'any notification')

    @patch("gobcore.message_broker.notifications.send_broadcast")
    def test_send_notifications(self, mock_send_broadcast):
        send_notifications({})
        mock_send_broadcast.assert_not_called()

        send_notifications({'header': {'a': 1, 'c': 3}, 'notification key': {'x': 1, 'y': 2}})
        mock_send_broadcast.assert_called_with('notification exchange',
                                               msg={
                                                   'header': {'a': 1, 'b': None},
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

    @patch("gobcore.message_broker.notifications._create_exchange")
    def test__create_broadcast_exchange(self, mock_create_exchange):
        _create_broadcast_exchange('any channel', 'any exchange')
        mock_create_exchange.assert_called_with('any channel',
                                                durable=True,
                                                exchange='any exchange',
                                                exchange_type='fanout')

    @patch("gobcore.message_broker.notifications.pika.BasicProperties")
    @patch("gobcore.message_broker.notifications.pika.BlockingConnection")
    @patch("gobcore.message_broker.notifications._create_broadcast_exchange")
    def test_send_broadcast(self, mock_create_broadcast_exchange, mock_blocking_connection, mock_basic_properties):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        send_broadcast('any exchange', {})
        mock_create_broadcast_exchange.assert_called_with(mock_channel, 'any exchange')
        mock_channel.basic_publish.assert_called_with(
            body='{}',
            exchange='any exchange',
            properties=mock_basic_properties(
                delivery_mode=2  # Make messages persistent
            ),
            routing_key='')

    @patch("gobcore.message_broker.notifications.pika.BlockingConnection")
    @patch("gobcore.message_broker.notifications._create_broadcast_exchange")
    @patch("gobcore.message_broker.notifications._create_queue")
    @patch("gobcore.message_broker.notifications._bind_queue")
    def test_listen_to_broadcasts(self, mock_bind_queue, mock_create_queue, mock_create_broadcast_exchange, mock_blocking_connection):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        result = listen_to_broadcasts('any exchange', 'any queue')
        mock_create_broadcast_exchange.assert_called_with(mock_channel, 'any exchange')
        mock_create_queue.assert_called_with(channel=mock_channel, queue='any queue', durable=True)
        mock_bind_queue.assert_called_with(channel=mock_channel, exchange='any exchange', queue='any queue', key='')
        self.assertEqual(result, 'any queue')


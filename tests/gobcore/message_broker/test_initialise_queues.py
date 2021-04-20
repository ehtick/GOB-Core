from unittest import TestCase
from unittest.mock import patch, Mock

from gobcore.message_broker.initialise_queues import \
    initialize_message_broker, create_queue_with_binding


class TestInitialiseQueues(TestCase):

    @patch("gobcore.message_broker.initialise_queues.get_manager")
    def test_create_queue_with_binding(self, mock_get_manager):
        exchange, queue, keys = 'some exchange', 'some queue', 'some key'
        manager = Mock()
        mock_get_manager.return_value.__enter__.return_value = manager
        create_queue_with_binding(exchange, queue, keys)
        mock_get_manager.assert_called_with()
        manager.create_queue_with_binding.assert_called_with(exchange, queue, keys)

    @patch("gobcore.message_broker.initialise_queues.get_manager")
    def test_initialize_message_broker(self, mock_get_manager):
        manager = Mock()
        mock_get_manager.return_value.__enter__.return_value = manager
        initialize_message_broker()
        mock_get_manager.assert_called_with()
        manager.create_all.assert_called_with()

from unittest import TestCase
from unittest.mock import patch

from gobcore.message_broker.initialise_queues import \
    initialize_message_broker, create_queue_with_binding


class TestInitialiseQueues(TestCase):

    @patch("gobcore.message_broker.initialise_queues.msg_broker")
    def test_create_queue_with_binding(self, patch_msg_broker):
        exchange, queue, keys = 'some exchange', 'some queue', 'some key'
        manager = patch_msg_broker.manager.return_value.__enter__.return_value
        print(f'Manager {manager}')
        create_queue_with_binding(exchange, queue, keys)
        manager.create_queue_with_binding.assert_called_with(exchange, queue, keys)

    @patch("gobcore.message_broker.initialise_queues.msg_broker")
    def test_initialize_message_broker(self, patch_msg_broker):
        manager = patch_msg_broker.manager.return_value.__enter__.return_value
        initialize_message_broker()
        manager.create_all.assert_called_with()

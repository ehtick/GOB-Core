from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.message_broker import publish


class TestInitFile(TestCase):

    @patch("gobcore.message_broker.msg_broker")
    def test_publish(self, mock_msg_broker):
        exchange = 'exchange'
        key = 'key'
        msg = 'msg'
        connection = MagicMock()
        mock_msg_broker.connection = connection
        publish(exchange, key, msg)
        connection.return_value.__enter__.return_value.publish.assert_called_with(exchange, key, msg)
        connection.assert_called_with()

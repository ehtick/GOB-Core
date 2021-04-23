from unittest import TestCase
from unittest.mock import patch

from gobcore.message_broker import publish


class TestInitFile(TestCase):

    @patch("gobcore.message_broker.get_connection")
    def test_publish(self, mock_connection):
        exchange = 'exchange'
        key = 'key'
        msg = 'msg'
        publish(exchange, key, msg)
        mock_connection.return_value.__enter__.return_value.publish.assert_called_with(exchange, key, msg)
        mock_connection.assert_called_with()

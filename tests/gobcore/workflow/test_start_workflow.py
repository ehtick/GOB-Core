from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.workflow.start_workflow import start_workflow

@patch("gobcore.workflow.start_workflow.WORKFLOW_EXCHANGE", 'workflow exchange')
@patch("gobcore.workflow.start_workflow.WORKFLOW_REQUEST_KEY", 'workflow request key')
class TestStartWorkflow(TestCase):

    @patch("gobcore.workflow.start_workflow.pika.BasicProperties")
    @patch("gobcore.workflow.start_workflow.pika.BlockingConnection")
    def test_start_workflow(self, mock_blocking_connection, mock_basic_properties):
        mock_connection = MagicMock()
        mock_channel = MagicMock()
        mock_blocking_connection.return_value.__enter__.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        start_workflow('any workflow', {'arguments': 'any arguments'})
        mock_channel.basic_publish.assert_called_with(
            body='{"header": {"arguments": "any arguments"}, "contents": {}, "workflow": "any workflow"}',
            exchange='workflow exchange',
            properties=mock_basic_properties(
                delivery_mode=2  # Make messages persistent
            ),
            routing_key='workflow request key')

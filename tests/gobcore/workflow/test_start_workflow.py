from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.workflow.start_workflow import start_workflow, retry_workflow, SINGLE_RETRY_TIME, RETRY_TIME_KEY


@patch("gobcore.workflow.start_workflow.WORKFLOW_EXCHANGE", 'workflow exchange')
@patch("gobcore.workflow.start_workflow.WORKFLOW_QUEUE", 'workflow queue')
@patch("gobcore.workflow.start_workflow.WORKFLOW_REQUEST_KEY", 'workflow request key')
class TestStartWorkflow(TestCase):

    @patch("gobcore.workflow.start_workflow.msg_broker")
    def test_start_workflow(self, mock_msg_broker):
        mock_connection = MagicMock()
        mock_msg_broker.connection.return_value.__enter__.return_value = mock_connection
        start_workflow('any workflow', {'arguments': 'any arguments'})
        mock_connection.publish.assert_called_with(
            exchange='workflow exchange',
            key='workflow request key',
            msg={"header": {"arguments": "any arguments"}, "contents": {}, "workflow": "any workflow"},
        )
        start_workflow('any workflow', {'arguments': 'any arguments', 'contents_ref': 'contents ref'})
        mock_connection.publish.assert_called_with(
            exchange='workflow exchange',
            key='workflow request key',
            msg={
                "header": {
                    "arguments": "any arguments", "contents_ref": "contents ref"},
                "contents": {}, "workflow": "any workflow", "contents_ref": "contents ref"
            }
        )

    @patch("gobcore.workflow.start_workflow.msg_broker")
    def test_retry_workflow(self, mock_msg_broker):
        mock_connection = MagicMock()
        mock_msg_broker.connection.return_value.__enter__.return_value = mock_connection
        msg = {
            'workflow': {
            }
        }

        # No retry when no retry is specified
        result = retry_workflow(msg)
        self.assertFalse(result)
        mock_connection.publish_delayed.assert_not_called()

        msg = {
            'workflow': {
                'retry_time': 0
            }
        }
        # No retry when out of retry time
        result = retry_workflow(msg)
        self.assertFalse(result)
        mock_connection.publish_delayed.assert_not_called()
        retry_time = 100
        msg = {
            'workflow': {
                RETRY_TIME_KEY: retry_time
            }
        }
        # Retry when retry is specified
        result = retry_workflow(msg)
        mock_connection.publish_delayed.assert_called_with(
            exchange='workflow exchange',
            key='workflow request key',
            queue='workflow queue',
            msg={"workflow": {RETRY_TIME_KEY: retry_time - SINGLE_RETRY_TIME}},
            delay_msec=SINGLE_RETRY_TIME * 1000
        )
        self.assertTrue(result)

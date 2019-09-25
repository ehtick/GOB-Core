from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.logging.audit_logger import AuditLogger


class TestAuditLogger(TestCase):

    def test_get_instance(self):
        self.assertIsInstance(AuditLogger.get_instance(), AuditLogger)

        mocked_instance = MagicMock()
        AuditLogger.logger = mocked_instance

        self.assertEqual(mocked_instance, AuditLogger.get_instance())

    @patch("gobcore.logging.audit_logger.AuditLogPublisher")
    def test_init(self, mock_audit_log_publisher):
        audit_logger = AuditLogger()
        self.assertEqual(mock_audit_log_publisher.return_value, audit_logger.publisher)

    @patch("gobcore.logging.audit_logger.AuditLogPublisher", MagicMock())
    @patch("gobcore.logging.audit_logger.datetime.datetime")
    def test_log_request(self, mock_datetime):
        audit_logger = AuditLogger()
        mock_datetime.now.return_value = 'timestamp now'

        audit_logger.log_request('the source', 'the destination', {'extra': 'data'})
        audit_logger.publisher.publish_request.assert_called_with({
            'type': 'request',
            'source': 'the source',
            'destination': 'the destination',
            'timestamp': 'timestamp now',
            'data': {'extra': 'data'},
        })
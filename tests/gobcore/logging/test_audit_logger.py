from unittest import TestCase
from unittest.mock import MagicMock, patch
from uuid import UUID

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

    @patch("gobcore.logging.audit_logger.uuid.uuid4")
    def test_uuid(self, mock_uuid4):
        mock_uuid4.return_value = "some uuid"
        
        audit_logger = AuditLogger()
        self.assertEqual("some uuid", audit_logger._uuid())

    @patch("gobcore.logging.audit_logger.AuditLogPublisher", MagicMock())
    @patch("gobcore.logging.audit_logger.datetime.datetime")
    def test_log_request(self, mock_datetime):
        audit_logger = AuditLogger()
        audit_logger._uuid = lambda: 'generated uuid'
        mock_datetime.now.return_value = 'timestamp now'

        audit_logger.log_request('the source', 'the destination', {'extra': 'data'})
        audit_logger.publisher.publish_request.assert_called_with({
            'type': 'request',
            'source': 'the source',
            'destination': 'the destination',
            'timestamp': 'timestamp now',
            'data': {'extra': 'data'},
            'request_uuid': 'generated uuid',
        })

        audit_logger.log_request('the source', 'the destination', {'extra': 'data'}, 'passed uuid')
        audit_logger.publisher.publish_request.assert_called_with({
            'type': 'request',
            'source': 'the source',
            'destination': 'the destination',
            'timestamp': 'timestamp now',
            'data': {'extra': 'data'},
            'request_uuid': 'passed uuid',
        })

    @patch("gobcore.logging.audit_logger.AuditLogPublisher", MagicMock())
    @patch("gobcore.logging.audit_logger.datetime.datetime")
    def test_log_response(self, mock_datetime):
        audit_logger = AuditLogger()
        audit_logger._uuid = lambda: 'generated uuid'
        mock_datetime.now.return_value = 'timestamp now'

        audit_logger.log_response('the source', 'the destination', {'extra': 'data'})
        audit_logger.publisher.publish_response.assert_called_with({
            'type': 'response',
            'source': 'the source',
            'destination': 'the destination',
            'timestamp': 'timestamp now',
            'data': {'extra': 'data'},
            'request_uuid': 'generated uuid',
        })

        audit_logger.log_response('the source', 'the destination', {'extra': 'data'}, 'passed uuid')
        audit_logger.publisher.publish_response.assert_called_with({
            'type': 'response',
            'source': 'the source',
            'destination': 'the destination',
            'timestamp': 'timestamp now',
            'data': {'extra': 'data'},
            'request_uuid': 'passed uuid',
        })

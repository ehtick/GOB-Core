import datetime
import uuid

from gobcore.logging.log_publisher import AuditLogPublisher


class AuditLogger:
    logger = None

    @classmethod
    def get_instance(cls):
        if cls.logger is None:
            cls.logger = AuditLogger()

        return cls.logger

    def __init__(self):
        self.publisher = AuditLogPublisher()

    def _uuid(self):
        return str(uuid.uuid4())

    def log_request(self, source: str, destination: str, extra_data: dict, request_uuid: str = None):
        msg = {
            'type': 'request',
            'source': source,
            'destination': destination,
            'timestamp': datetime.datetime.now(),
            'request_uuid': request_uuid if request_uuid else self._uuid(),
            'data': extra_data,
        }
        self.publisher.publish_request(msg)

    def log_response(self, source: str, destination: str, extra_data: dict, request_uuid: str = None):
        msg = {
            'type': 'response',
            'source': source,
            'destination': destination,
            'timestamp': datetime.datetime.now(),
            'request_uuid': request_uuid if request_uuid else self._uuid(),
            'data': extra_data,
        }
        self.publisher.publish_response(msg)

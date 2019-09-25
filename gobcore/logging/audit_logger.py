import datetime

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

    def log_request(self, source: str, destination: str, extra_data: dict):
        msg = {
            'type': 'request',
            'source': source,
            'destination': destination,
            'timestamp': datetime.datetime.now(),
            'data': extra_data,
        }
        self.publisher.publish_request(msg)

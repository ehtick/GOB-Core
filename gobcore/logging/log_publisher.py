"""LogPublisher

This module contains the LogPublisher class.

A LogPublisher publishes log message on the message broker.

"""
import threading
import time

from gobcore.message_broker.message_broker import Connection
from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.config import LOG_EXCHANGE, AUDIT_LOG_EXCHANGE, ISSUE_EXCHANGE, REQUEST


class LogPublisher():

    _connection = None
    _connection_lock = threading.Lock()
    _auto_disconnect_thread = None
    _auto_disconnect_timeout = 0

    def __init__(self, connection_params=CONNECTION_PARAMS, exchange=LOG_EXCHANGE):
        # Register the connection params and log queue
        self._connection_params = connection_params
        self._exchange = exchange

    def publish(self, key, msg):
        # Acquire a lock for the connection
        with self._connection_lock:
            # Connect to the message broker, auto disconnect after timeout seconds
            self._auto_connect(timeout=2)
            # Publish the message
            self._connection.publish(self._exchange, key, msg)

    def _auto_connect(self, timeout):
        self._auto_disconnect_timeout = timeout
        if self._connection is None:
            # Start a connection if no connection is active
            self._connection = Connection(self._connection_params)
            self._connection.connect()
            # Start auto disconnect thread
            if self._auto_disconnect_thread is not None:
                # Join any previously ended threads
                self._auto_disconnect_thread.join()
            self._auto_disconnect_thread = threading.Thread(target=self._auto_disconnect, name="AutoDisconnect")
            self._auto_disconnect_thread.start()

    def _disconnect(self):
        if self._connection is not None:
            self._connection.disconnect()
            self._connection = None

    def _auto_disconnect(self):
        while True:
            time.sleep(1)
            with self._connection_lock:
                if self._auto_disconnect_timeout > 0:
                    self._auto_disconnect_timeout -= 1
                else:
                    self._disconnect()
                    break


class AuditLogPublisher(LogPublisher):
    REQUEST_KEY = 'request'
    RESPONSE_KEY = 'response'

    def __init__(self, connection_params=None):
        connection_params = connection_params or CONNECTION_PARAMS
        super().__init__(connection_params, AUDIT_LOG_EXCHANGE)

    def publish_request(self, msg):
        self.publish(self.REQUEST_KEY, msg)

    def publish_response(self, msg):
        self.publish(self.RESPONSE_KEY, msg)


class IssuePublisher(LogPublisher):

    def __init__(self, connection_params=None):
        connection_params = connection_params or CONNECTION_PARAMS
        super().__init__(connection_params, ISSUE_EXCHANGE)

    def publish(self, msg):
        super().publish(REQUEST, msg)

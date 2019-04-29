import unittest
import mock

from gobcore.message_broker.config import HEARTBEAT_QUEUE, get_queue
from gobcore.status.heartbeat import Heartbeat


class MockHeartbeat:

    msg = None
    queue = None
    key = None

    def __init__(self):
        MockHeartbeat.msg = None
        MockHeartbeat.queue = None
        MockHeartbeat.key = None

    def publish(self, queue, key, msg):
        MockHeartbeat.msg = msg
        MockHeartbeat.queue = queue
        MockHeartbeat.key = key


class TestHeartbeat(unittest.TestCase):

    @mock.patch("atexit.register")
    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_constructor(self, mocked_publish, mocked_connect, mocked_atexit_register):
        heartbeat = Heartbeat(MockHeartbeat(), "Myname")
        mocked_connect.assert_not_called()
        self.assertIsNotNone(MockHeartbeat.msg)
        mocked_atexit_register.assert_called()

    @mock.patch("atexit.register")
    @mock.patch("gobcore.message_broker.message_broker.Connection.connect")
    @mock.patch("gobcore.message_broker.message_broker.Connection.publish")
    def test_send(self, mocked_publish, mocked_connect, mocked_atexit_register):
        heartbeat = Heartbeat(MockHeartbeat(), "Myname")
        mocked_publish.reset_mock()

        heartbeat.send()
        self.assertIsNotNone(MockHeartbeat.msg)
        self.assertEqual(MockHeartbeat.queue["name"], "gob.status.heartbeat")
        self.assertEqual(MockHeartbeat.key, "HEARTBEAT")
        self.assertEqual(MockHeartbeat.msg["name"], "Myname")

    def test_progress(self):
        connection = MockHeartbeat()
        connection.publish = mock.MagicMock()

        msg = {
            "header": {
                "jobid": "any job",
                "stepid": "any step"
            }
        }
        service = {
            "report": "any report"
        }

        Heartbeat.progress(None, {}, {}, None)
        connection.publish.assert_not_called()

        Heartbeat.progress(None, service, {}, None)
        connection.publish.assert_not_called()

        Heartbeat.progress(None, {}, msg, None)
        connection.publish.assert_not_called()

        Heartbeat.progress(connection, service, msg, "any status")
        connection.publish.assert_called_with({
            "exchange": "gob.status",
            "name": "gob.status.heartbeat",
            "key": "HEARTBEAT"
        },
        "PROGRESS", {
            "jobid": "any job",
            "stepid": "any step",
            "status": "any status"
        })

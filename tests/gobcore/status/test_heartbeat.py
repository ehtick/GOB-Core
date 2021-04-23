import unittest
import mock

from gobcore.status.heartbeat import Heartbeat
from gobcore.message_broker.config import STATUS_EXCHANGE, HEARTBEAT_KEY

class MockHeartbeatConnection:

    msg = None
    queue = None
    key = None

    def __init__(self):
        self.msg = None
        self.queue = None
        self.key = None

    def publish(self, queue, key, msg):
        self.msg = msg
        self.queue = queue
        self.key = key


class MockedThread:
    _is_alive = True

    def __init__(self, name):
        self.name = name

    def is_alive(self):
        return self._is_alive;


def generate_threads(*specs):
    def func():
        for spec in specs:
            yield MockedThread(*spec)
    return func


class TestHeartbeat(unittest.TestCase):

    @mock.patch("atexit.register")
    def test_constructor(self, mocked_atexit_register):
        connection = MockHeartbeatConnection()
        Heartbeat(connection, "Myname")
        mocked_atexit_register.assert_called()
        self.assertIsNotNone(connection.msg)

    def test_send(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")

        heartbeat.send()
        self.assertIsNotNone(connection.msg)
        self.assertEqual(connection.queue, STATUS_EXCHANGE)
        self.assertEqual(connection.key, HEARTBEAT_KEY)
        self.assertEqual(connection.msg["name"], "Myname")

    @mock.patch("threading.enumerate", new=generate_threads(["a"]))
    def test_heartbeat_threads(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        assert len(heartbeat.threads) == 1

    @mock.patch("threading.enumerate", new=generate_threads(["_a"]))
    def test_heartbeat_no_threads(self):
        connection = MockHeartbeatConnection()
        heartbeat = Heartbeat(connection, "Myname")
        assert len(heartbeat.threads) == 0

    def test_progress(self):
        connection = MockHeartbeatConnection()
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

        Heartbeat.progress(connection, service, msg, "any status", "any info")
        connection.publish.assert_called_with("gob.status", "status.progress", {
            'header': {'jobid': 'any job', 'stepid': 'any step'},
            "jobid": "any job",
            "stepid": "any step",
            "status": "any status",
            "info_msg": "any info"
        })

    def test_log_msg(self):
        queue = "any queue"
        status = "any status"
        header = {
            'catalogue': "any catalogue"
        }
        msg = Heartbeat._progress_log_msg("any queue", "any status", header)
        self.assertTrue(queue in msg)
        self.assertTrue(status in msg)
        self.assertTrue("address: " in msg)
        self.assertTrue("dns: " in msg)
        self.assertTrue(header['catalogue'] in msg)
        self.assertTrue("collection: None" in msg)

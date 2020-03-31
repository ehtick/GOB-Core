from unittest import TestCase
from unittest.mock import MagicMock

from gobcore.datastore.datastore import Datastore


class DatastoreImpl(Datastore):

    def __init__(self, connection_config, read_config=None):
        super().__init__(connection_config, read_config)

    def connect(self):
        pass

    def query(self, query):
        pass


class TestDatastore(TestCase):

    def test_init(self):
        connection_config = MagicMock()
        read_config = MagicMock()
        impl = DatastoreImpl(connection_config, read_config)

        self.assertEqual(connection_config, impl.connection_config)
        self.assertEqual(read_config, impl.read_config)

    def test_read(self):
        result = ['1', '2', '3']
        impl = DatastoreImpl({})
        impl.query = lambda x: iter(result)
        self.assertEqual(result, impl.read(None))

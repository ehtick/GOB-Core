from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.wfs import WfsDatastore


class TestWfsDatastore(TestCase):

    @patch("gobcore.datastore.wfs.requests")
    def test_connect(self, mock_requests):
        url = 'some url'
        returnval = type('Response', (object,), {'ok': 'OK'})
        mock_requests.get.return_value = returnval
        config = {'url': url}
        store = WfsDatastore(config)

        store.connect()
        self.assertEqual("", store.user)
        self.assertEqual(mock_requests.get.return_value, store.response)
        mock_requests.get.assert_called_with(url)

    @patch("gobcore.datastore.wfs.requests")
    def test_connect_to_wfs_invalid_response(self, mock_requests):
        url = 'some url'
        returnval = type('Response', (object,), {'ok': None})
        mock_requests.get.return_value = returnval
        config = {'url': url}
        store = WfsDatastore(config)

        with self.assertRaises(AssertionError):
            store.connect()

    def test_query_wfs(self):
        features = ['f1', 'f2', 'f3']
        store = WfsDatastore({})
        store.response = type('MockResponse', (object,), {'json': lambda: {'features': features}})

        self.assertEqual(features, list(store.query(None)))

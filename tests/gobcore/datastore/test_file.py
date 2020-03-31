from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.datastore.file import FileDatastore


@patch("gobcore.datastore.file.Datastore", MagicMock)
class TestFileDatastore(TestCase):

    @patch("gobcore.datastore.file.pandas.read_csv")
    def test_connect(self, mock_read_csv):
        connection_config = {
            'filename': 'filename.csv',
            'filepath': '/path/to/filename.csv',
        }
        read_config = {
            'filetype': 'CSV',
            'separator': 'sep',
            'encoding': 'enc',
        }

        store = FileDatastore(connection_config, read_config)
        store.connect()

        self.assertEqual(mock_read_csv.return_value, store.connection)

        mock_read_csv.assert_called_with(filepath_or_buffer=connection_config['filepath'],
                                         sep=read_config['separator'],
                                         encoding=read_config['encoding'],
                                         dtype=str)

    def test_connect_invalid_type(self):
        store = FileDatastore({}, {'filetype': 'invalid'})

        with self.assertRaises(NotImplementedError):
            store.connect()

    @patch("gobcore.datastore.file.pandas.isnull", lambda x: x == 'PANDASNULL')
    def test_query(self):
        store = FileDatastore({}, {})

        store.connection = type('MockConnection', (), {
            'iterrows': lambda: [
                (0, {'a': 'A', 'b': 'B', 'c': 'PANDASNULL', 'd': 3, 'e': 1.0}),
                (1, {'a': 'AA', 'b': 'PANDASNULL', 'c': '', 'd': 4, 'e': 3.0}),
            ]
        })

        result = list(store.query(None))
        self.assertEqual(
            [
                {'a': 'A', 'b': 'B', 'c': None, 'd': 3, 'e': 1.0},
                {'a': 'AA', 'b': None, 'c': '', 'd': 4, 'e': 3.0},
            ],
            result
        )

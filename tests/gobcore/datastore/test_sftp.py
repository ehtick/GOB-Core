from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.datastore.sftp import SFTPDatastore


@patch("gobcore.datastore.sftp.Datastore", MagicMock)
class TestSFTPDatastore(TestCase):

    @patch("gobcore.datastore.sftp.paramiko")
    def test_connect(self, mock_paramiko):
        connection_config = {
            'host': 'any host',
            'port': 1234,
            'username': 'username',
            'password': 'password'
        }

        mock_transport = mock_paramiko.Transport.return_value

        store = SFTPDatastore(connection_config)
        store.connect()

        mock_paramiko.Transport.assert_called_with((connection_config['host'], connection_config['port']))
        mock_transport.connect.assert_called_with(username=connection_config['username'], password=connection_config['password'])
        
        mock_paramiko.SFTPClient.from_transport.assert_called_with(mock_transport)

    def test_create_directories(self):
        store = SFTPDatastore({})
        store.connection = MagicMock()

        # Let throw OSError to mimick directory that already exists
        store.connection.mkdir.side_effect = OSError

        path = 'directory/to/create'
        store._create_directories(path)

        store.connection.mkdir.assert_has_calls([
            call('directory'),
            call('directory/to'),
            call('directory/to/create'),
        ])

    @patch("gobcore.datastore.sftp.paramiko")
    def test_put_file(self, mock_paramiko):
        connection_config = {
            'host': 'any host',
            'port': 1234,
            'username': 'username',
            'password': 'password'
        }

        mock_sftp_connection = mock_paramiko.SFTPClient.from_transport.return_value

        store = SFTPDatastore(connection_config)
        store._create_directories = MagicMock()

        store.connect()

        store.put_file('any file', 'some/dir/any dest')

        store._create_directories.assert_called_with('some/dir')
        mock_sftp_connection.put.assert_called_with('any file', 'some/dir/any dest')
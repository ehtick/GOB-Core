from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.database.reader.oracle import query_oracle, read_from_oracle
from gobcore.exceptions import GOBEmptyResultException

class TestOracleReader(TestCase):
    class MockConnection():

        def __init__(self, result):
            self.cursor_obj = MagicMock()
            self.cursor_obj.return_value = iter(result)

        def cursor(self):
            return self.cursor_obj

    def test_query_oracle(self):
        mock_connection = self.MockConnection([{"id": 1}, {"id": 2}])
        query = "SELECT this FROM that WHERE this=that"

        for row in query_oracle(mock_connection, [query]):
            pass

        mock_connection.cursor_obj.execute.assert_called_with(query)

    @patch('gobcore.database.reader.oracle.query_oracle')
    def test_read_from_oracle(self, mock_query):
        expected_result = [{"id": 1}, {"id": 2}]
        mock_query.return_value = iter(expected_result)
        mock_connection = self.MockConnection(expected_result)
        query = "SELECT this FROM that WHERE this=that"

        result = read_from_oracle(mock_connection, [query])
        self.assertEqual(expected_result, result)

    def test_read_from_oracle_empty_result(self):
        mock_connection = self.MockConnection([])
        query = "SELECT this FROM that WHERE this=that"

        with self.assertRaises(GOBEmptyResultException):
            read_from_oracle(mock_connection, [query])

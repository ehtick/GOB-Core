import types

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.database.reader.postgresql import list_tables_for_schema, query_postgresql


class MockConnection():
    class Cursor():
        def __init__(self, expected_result):
            self.expected_result = expected_result

        def execute(self, query):
            return

        def close(self):
            return

        def __iter__(self):
            return iter(self.expected_result)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    commit_obj = MagicMock()

    def __init__(self, expected_result):
        self.cursor_obj = self.Cursor(expected_result)

    def cursor(self, **kwargs):
        return self.cursor_obj

    def commit(self):
        return


class TestPostgresReader(TestCase):

    def test_query_postgresql(self):
        expected_result = [i for i in range(10)]
        connection = MockConnection(expected_result)
        connection.cursor_obj.execute = MagicMock(return_value=expected_result)
        connection.cursor_obj.close = MagicMock()
        query = "SELECT something FROM something WHERE something=true"

        result = query_postgresql(connection, query)
        self.assertTrue(isinstance(result, types.GeneratorType))
        self.assertEqual(expected_result, list(result))

        connection.cursor_obj.execute.assert_called_with(query)

    @patch('gobcore.database.reader.postgresql.query_postgresql')
    def list_tables_for_schema(self, mock_query_postgresql):
        mock_query_postgresql.return_value = [
            {'table_name': 'table1'},
            {'table_name': 'table2'}
        ]
        connection = MagicMock()

        result = list_tables_for_schema(connection, 'someschema')
        self.assertEquals(['table1', 'table2'], result)

        mock_query_postgresql.assert_called_with(
            connection,
            "SELECT table_name FROM information_schema.tables WHERE table_schema='someschema'"
        )

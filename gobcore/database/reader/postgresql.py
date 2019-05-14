from gobcore.exceptions import GOBException
from psycopg2 import Error
from psycopg2.extras import DictCursor
from typing import List


def query_postgresql(connection, query):
    """Query Postgres

    :param connection:
    :param query:
    :return:
    """
    if isinstance(query, list):
        query = "\n".join(query)
    try:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query)
            connection.commit()

            for row in cursor:
                yield row
    except Error as e:
        raise GOBException(f'Error executing query: {query[:80]}. Error: {e}')


def list_tables_for_schema(connection, schema: str) -> List[str]:
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'"
    result = query_postgresql(connection, query)
    return [row['table_name'] for row in result]

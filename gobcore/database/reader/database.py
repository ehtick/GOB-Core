from sqlalchemy.exc import DBAPIError

from gobcore.exceptions import GOBException, GOBEmptyResultException


def query_database(connection, query):
    """Reads from the database

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a list of data
    """
    try:
        result = connection.execute("\n".join(query)).fetchall()
    except DBAPIError as e:
        raise GOBException(f'Database connection failed. Error: {e}.')

    if len(result) == 0:
        raise GOBEmptyResultException('No results found for database query')

    for row in result:
        yield dict(row)


def read_from_database(connection, query):
    """Reads from the database

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a list of data
    """
    return [row for row in query_database(connection, query)]

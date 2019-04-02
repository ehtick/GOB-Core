def query_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """

    for _, row in connection.iterrows():
        yield row


def read_from_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """
    return [row for row in query_file(connection)]

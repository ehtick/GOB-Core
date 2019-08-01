def query_wfs(response):
    """Reads from the response

    The requests library is used to iterate through the items

    :return: a list of dicts
    """

    for feature in response.json()['features']:
        yield feature


def read_from_wfs(response):
    """Reads from the features in the url connection

    :return: a list of dicts
    """
    return [row for row in query_wfs(response)]

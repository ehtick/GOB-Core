"""Implementation of data input connectors

The following connectors are implemented in this module:
    WFS - Connects to a WFS server and imports the features

"""
import requests


def connect_to_wfs(url):
    """Connect to the datasource

    The requests library is used to connect to the data source

    :return:
    """
    user = ""  # No user identification for file reads
    response = requests.get(url)
    assert response.ok, f"API Response not OK for url {url}"
    return response, user

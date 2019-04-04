import io
import os
import pandas
import re


from objectstore.objectstore import get_full_container_list, get_object


def query_objectstore(connection, config):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    container_name = os.getenv("CONTAINER_BASE", "acceptatie")
    file_filter = config.get("file_filter", ".*")
    file_type = config.get("file_type")

    # Use the container base env variable
    result = get_full_container_list(connection, container_name)
    pattern = re.compile(file_filter)

    for row in result:
        if pattern.match(row["name"]):
            file_info = dict(row)    # File information
            if file_type == "XLS":
                # Include (non-empty) Excel rows
                obj = get_object(connection, row, container_name)
                for item in _read_xls(obj, file_info):
                    yield item
            else:
                # Include file attributes
                yield file_info


def read_from_objectstore(connection, config):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    return [row for row in query_objectstore(connection, config)]


def _read_xls(obj, file_info):
    """Read XLS(X) object

    Read Excel object and return the (non-empty) rows

    :param obj: An Objectstore object
    :param file_info: File information (name, last_modified, ...)
    :return: The list of non-empty rows
    """
    io_obj = io.BytesIO(obj)
    excel = pandas.read_excel(io=io_obj)

    data = []
    for _, row in excel.iterrows():
        empty = True
        for key, value in row.items():
            # Convert any NULL values to None values
            if pandas.isnull(value):
                row[key] = None
            else:
                # Not NULL => row is not empty
                empty = False
        if not empty:
            row["_file_info"] = file_info
            data.append(row)

    return data

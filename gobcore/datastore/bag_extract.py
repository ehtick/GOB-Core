import re
import io
import os
import xml.etree.ElementTree as ET
from collections import defaultdict

from osgeo import ogr
from typing import List
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from gobcore.datastore.datastore import Datastore
from gobcore.datastore.objectstore import ObjectDatastore
from gobcore.exceptions import GOBException
from objectstore.objectstore import get_object


def _extract_nested_zip(zip_file, nested_zip_files: List[str], destination_dir: str):
    """Extracts nested zip file from zip_file.

    Example:
    _extract_nested_zip('a.zip', ['b.zip', 'c.zip'], '/tmp_dstdir')

    with:
    a.zip
        somefile_in_a
        other_file_in_a
        b.zip
            some_file_in_b
            c.zip
                some_file_in_c
                some_other_file_in_c
            some_other_file_in_b

    results in:
    /tmp_dst_dir/some_file_in_c
    /tmp_dst_dir/some_other_file_in_c

    :param zip_file:
    :param nested_zip_files:
    :param destination_dir:
    :return:
    """
    with ZipFile(zip_file, 'r') as f:
        if len(nested_zip_files) == 0:
            f.extractall(destination_dir)
        else:
            with f.open(nested_zip_files[0], 'r') as nested_zip_file:
                nested_zip_file_data = io.BytesIO(nested_zip_file.read())
                _extract_nested_zip(nested_zip_file_data, nested_zip_files[1:], destination_dir)


class BagExtractDatastore(Datastore):
    ns_pattern = re.compile(r'{.*}')
    namespaces = {
        # We could extract namespaces from the file, but this way we're sure they won't change in the source.
        'DatatypenNEN3610': 'www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601',
        'Objecten': 'www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601',
        'gml': 'http://www.opengis.net/gml/3.2',
        'Historie': 'www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601',
        'Objecten-ref': 'www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601',
        'nen5825': 'www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601',
        'KenmerkInOnderzoek': 'www.kadaster.nl/schemas/lvbag/imbag/kenmerkinonderzoek/v20200601',
        'selecties-extract': 'http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601',
        'sl-bag-extract': 'http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601',
        'sl': 'http://www.kadaster.nl/schemas/standlevering-generiek/1.0',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xs': 'http://www.w3.org/2001/XMLSchema',
    }

    def __init__(self, connection_config: dict, read_config: dict = None):
        super().__init__(connection_config, read_config)

        self.tmp_dir = TemporaryDirectory()
        self.files = []

        self.files = [os.path.join('/tmp/test', f) for f in sorted(os.listdir('/tmp/test'))]
        self._check_config()

    def _check_config(self):
        if not self.read_config.get('object_type'):
            raise GOBException("Missing object_type in read_config")

    def connect(self):
        file_location = self._download_file()
        filename = file_location.split('/')[-1]
        match = re.match(r'^BAGGEM(\d{4})L-(\d{8}).zip$', filename)

        if not match:
            raise GOBException(f"Unexpected filename format: {filename}")

        gemeente = match.group(1)
        datestr = match.group(2)

        _extract_nested_zip(file_location, [
            f"{gemeente}GEM{datestr}.zip",
            f"{gemeente}{self.read_config['object_type']}{datestr}.zip",
        ], self.tmp_dir.name)

        self.files = [os.path.join(self.tmp_dir.name, f) for f in sorted(os.listdir(self.tmp_dir.name))]

    def _download_file(self):
        """Downloads source file and returns file location

        TODO: This file is now temporarily downloaded from a fixed location from the object store. This file will later
        come from a web source or something similar.

        :return:
        """
        file_location = 'tmp_weesp/BAGGEM0457L-15112020.zip'

        # ObjectDatastore is hardcoded for now, as Core does not have direct access to Config, this is the easy temp
        # solution.
        config = {
            'type': 'objectstore',
            "VERSION": '2.0',
            "AUTHURL": 'https://identity.stack.cloudvps.com/v2.0',
            "TENANT_NAME": os.getenv("GOB_OBJECTSTORE_TENANT_NAME"),
            "TENANT_ID": os.getenv("GOB_OBJECTSTORE_TENANT_ID"),
            "USER": os.getenv("GOB_OBJECTSTORE_USER"),
            "PASSWORD": os.getenv("GOB_OBJECTSTORE_PASSWORD"),
            "REGION_NAME": 'NL'
        }

        read_config = {
            "file_filter": file_location,
            "container": "acceptatie",
        }

        objectstore = ObjectDatastore(config, read_config)
        objectstore.connect()

        file = next(objectstore.query(None))
        obj = get_object(objectstore.connection, file, read_config['container'])

        fname = file_location.split('/')[-1]
        download_location = os.path.join(self.tmp_dir.name, fname)
        with open(download_location, 'wb') as f:
            f.write(obj)

        return download_location

    def _gml_to_wkt(self, elm):
        gml_str = ET.tostring(elm).decode('utf-8')
        gml = ogr.CreateGeometryFromGML(gml_str)
        gml.FlattenTo2D()
        return gml.ExportToWkt()

    def _flatten_nested_list(self, lst: list, key_prefix: str):
        result = {}
        for item in lst:
            if isinstance(item, dict):
                # We have something of the form { ..., "key": [{"subkey": "A"}, {"subkey": "B"}], ... }
                # We transform this to { ..., "key/subkey": ["A", "B"], ... }
                d_item = self._flatten_dict(item)

                for d_key, d_value in d_item.items():
                    sub_key = f"{key_prefix}/{d_key}"
                    if sub_key not in result:
                        result[sub_key] = []

                    result[sub_key].append(d_value)
            else:
                result[key_prefix] = lst
        return result

    def _flatten_dict(self, d: dict):

        def flatten(dct: dict):
            result = {}
            for key, value in dct.items():
                if isinstance(value, dict):
                    # Recursively traverse dictionaries
                    result.update({f"{key}/{k}": v for k, v in flatten(value).items()})
                elif isinstance(value, list):
                    result.update(self._flatten_nested_list(value, key))
                else:
                    result[key] = value
            return result

        return flatten(d)

    def _element_to_dict(self, element: ET.Element):
        childs = list(element)

        if len(childs) == 1 and self.namespaces['gml'] in childs[0].tag:
            return self._gml_to_wkt(childs[0])
        elif childs:
            child_dicts = defaultdict(list)

            for child in childs:
                child_dicts[re.sub(self.ns_pattern, '', child.tag)].append(self._element_to_dict(child))

            return {k: v[0] if len(v) == 1 else v for k, v in child_dicts.items()}

        else:
            return element.text.strip()

    def query(self, query):
        for file in self.files:
            tree = ET.parse(file)
            root = tree.getroot()
            elements = root.iterfind('./sl:standBestand/sl:stand/sl-bag-extract:bagObject/Objecten:Verblijfsobject',
                                     self.namespaces)

            for element in elements:
                yield self._flatten_dict(self._element_to_dict(element))

import xml.etree.ElementTree as ET
import io
import os

from tempfile import TemporaryDirectory

from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.datastore.bag_extract import BagExtractDatastore, GOBException, _extract_nested_zip


class TestModuleFunctions(TestCase):

    def test_extract_nested_zip(self):
        testfile = os.path.join(os.path.dirname(__file__), 'testzip_for_extraction.zip')
        tmpdir = TemporaryDirectory()

        _extract_nested_zip(testfile, ['zipfile.zip', 'some_nested_zip.zip'], tmpdir.name)

        self.assertEqual({
            'some_file1.txt',
            'some_file2.txt'
        }, set(os.listdir(tmpdir.name)))


class TestBagExtractDatastore(TestCase):

    def get_test_object(self):
        with patch("gobcore.datastore.bag_extract.TemporaryDirectory") as mock_tmp_dir:
            connection_config = {"connection": "config"}
            read_config = {"object_type": "OBJT"}
            ds = BagExtractDatastore(connection_config, read_config)
            ds.tmp_dir.name = "/tmp_dir_name"
            return ds

    def test_check_config(self):
        with self.assertRaisesRegexp(GOBException, "Missing object_type in read_config"):
            ds = BagExtractDatastore({}, {})

    @patch("gobcore.datastore.bag_extract.os.listdir")
    @patch("gobcore.datastore.bag_extract._extract_nested_zip")
    def test_connect(self, mock_extract_zip, mock_listdir):
        ds = self.get_test_object()
        ds._download_file = MagicMock(return_value="thepath/to/BAGGEM1234L-12345678.zip")
        mock_listdir.return_value = [
            "fileA0001.xml",
            "fileA0002.xml",
            "fileA0003.zip",
        ]

        ds.connect()

        mock_extract_zip.assert_called_with(
            ds._download_file.return_value, [
                '1234GEM12345678.zip',
                '1234OBJT12345678.zip',
            ],
            "/tmp_dir_name",
        )
        self.assertEqual([
            "/tmp_dir_name/fileA0001.xml",
            "/tmp_dir_name/fileA0002.xml",
        ], ds.files)

        # Invalid filename
        ds._download_file.return_value = "thepath/to/BAGGEM123L-148024.zip"
        with self.assertRaises(GOBException):
            ds.connect()

    @patch("builtins.open")
    @patch("gobcore.datastore.bag_extract.os.getenv", lambda x: x)
    @patch("gobcore.datastore.bag_extract.get_object")
    @patch("gobcore.datastore.bag_extract.ObjectDatastore")
    def test_download_file(self, mock_object_datastore, mock_get_object, mock_open):
        ds = self.get_test_object()

        mock_object_datastore.return_value.query.return_value = iter(['some file'])

        self.assertEqual("/tmp_dir_name/BAGGEM0457L-15112020.zip", ds._download_file())

        mock_object_datastore.assert_called_with({
            'type': 'objectstore',
            'VERSION': '2.0',
            'AUTHURL': 'https://identity.stack.cloudvps.com/v2.0',
            'TENANT_NAME': 'GOB_OBJECTSTORE_TENANT_NAME',
            'TENANT_ID': 'GOB_OBJECTSTORE_TENANT_ID',
            'USER': 'GOB_OBJECTSTORE_USER',
            'PASSWORD': 'GOB_OBJECTSTORE_PASSWORD',
            'REGION_NAME': 'NL'
        }, {
            "file_filter": "tmp_weesp/BAGGEM0457L-15112020.zip",
            "container": "acceptatie",
        })
        mock_object_datastore.return_value.assert_has_calls([
            call.connect(),
            call.query(None),
        ])

        mock_get_object.assert_called_with(mock_object_datastore.return_value.connection, 'some file', 'acceptatie')

        mock_open.assert_called_with('/tmp_dir_name/BAGGEM0457L-15112020.zip', 'wb')
        mock_open.return_value.__enter__.return_value.write.assert_called_with(mock_get_object.return_value)

    @patch("gobcore.datastore.bag_extract.ogr.CreateGeometryFromGML")
    @patch("gobcore.datastore.bag_extract.ET")
    def test_gml_to_wkt(self, mock_et, mock_create_geometry):
        ds = self.get_test_object()

        res = ds._gml_to_wkt('elm')

        mock_create_geometry.assert_called_with(mock_et.tostring().decode())
        mock_create_geometry.return_value.FlattenTo2D.assert_called_once()
        mock_create_geometry.return_value.ExportToWkt.assert_called_once()
        self.assertEqual(mock_create_geometry().ExportToWkt(), res)

    def test_query(self):
        """Tests queyr, _element_to_dict, _flatten_dict, _flatten_nested_list and _gml_to_wkt

        :return:
        """
        xml = \
            """<?xml version="1.0" encoding="utf-8"?>
            <sl-bag-extract:bagStand xmlns:DatatypenNEN3610="www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601"
            xmlns:Objecten="www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601" xmlns:gml="http://www.opengis.net/gml/3.2"
            xmlns:Historie="www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601"
            xmlns:Objecten-ref="www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601"
            xmlns:nen5825="www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601"
            xmlns:KenmerkInOnderzoek="www.kadaster.nl/schemas/lvbag/imbag/kenmerkinonderzoek/v20200601"
            xmlns:selecties-extract="http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601"
            xmlns:sl-bag-extract="http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601"
            xmlns:sl="http://www.kadaster.nl/schemas/standlevering-generiek/1.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xsi:schemaLocation="http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601 http://www.kadaster.nl/schemas/bag-verstrekkingen/extract-deelbestand-lvc/v20200601/BagvsExtractDeelbestandExtractLvc-2.1.0.xsd">
            
                <sl-bag-extract:bagInfo>
                    <selecties-extract:Gebied-Registratief>
                        <selecties-extract:Gebied-GEM>
                            <selecties-extract:GemeenteCollectie>
                                <selecties-extract:Gemeente>
                                    <selecties-extract:GemeenteIdentificatie>0457</selecties-extract:GemeenteIdentificatie>
                                </selecties-extract:Gemeente>
                            </selecties-extract:GemeenteCollectie>
                        </selecties-extract:Gebied-GEM>
                    </selecties-extract:Gebied-Registratief>
                    <selecties-extract:LVC-Extract>
                        <selecties-extract:StandTechnischeDatum>2020-11-15</selecties-extract:StandTechnischeDatum>
                    </selecties-extract:LVC-Extract>
                </sl-bag-extract:bagInfo>
                <sl:standBestand>
                    <sl:dataset>LVBAG</sl:dataset>
                    <sl:inhoud>
                        <sl:gebied>GEMEENTE</sl:gebied>
                        <sl:leveringsId>0000000001</sl:leveringsId>
                        <sl:objectTypen>
                            <sl:objectType>VBO</sl:objectType>
                        </sl:objectTypen>
                    </sl:inhoud>
                    <sl:stand>
                        <sl-bag-extract:bagObject>
                            <Objecten:Verblijfsobject>
                                <Objecten:heeftAlsHoofdadres>
                                    <Objecten-ref:NummeraanduidingRef domein="NL.IMBAG.Nummeraanduiding">hoofdA</Objecten-ref:NummeraanduidingRef>
                                </Objecten:heeftAlsHoofdadres>
                                <Objecten:voorkomen>
                                    <Historie:Voorkomen>
                                        <Historie:voorkomenidentificatie>1</Historie:voorkomenidentificatie>
                                        <Historie:beginGeldigheid>2010-08-31</Historie:beginGeldigheid>
                                        <Historie:tijdstipRegistratie>2010-11-15T13:22:03.000</Historie:tijdstipRegistratie>
                                        <Historie:BeschikbaarLV>
                                            <Historie:tijdstipRegistratieLV>
                                            2010-11-15T13:31:10.557</Historie:tijdstipRegistratieLV>
                                        </Historie:BeschikbaarLV>
                                    </Historie:Voorkomen>
                                </Objecten:voorkomen>
                                <Objecten:heeftAlsNevenadres>
                                    <Objecten-ref:NummeraanduidingRef domein="NL.IMBAG.Nummeraanduiding">
                                    nevenA</Objecten-ref:NummeraanduidingRef>
                                </Objecten:heeftAlsNevenadres>
                                <Objecten:heeftAlsNevenadres>
                                    <Objecten-ref:NummeraanduidingRef domein="NL.IMBAG.Nummeraanduiding">
                                    nevenB</Objecten-ref:NummeraanduidingRef>
                                </Objecten:heeftAlsNevenadres>
                                <Objecten:identificatie domein="NL.IMBAG.Verblijfsobject">votA</Objecten:identificatie>
                                <Objecten:geometrie>
                                    <Objecten:punt>
                                        <gml:Point srsName="urn:ogc:def:crs:EPSG::28992" srsDimension="3">
                                            <gml:pos>131419.0 482833.0 0.0</gml:pos>
                                        </gml:Point>
                                    </Objecten:punt>
                                </Objecten:geometrie>
                                <Objecten:gebruiksdoel>woonfunctie</Objecten:gebruiksdoel>
                                <Objecten:gebruiksdoel>industriefunctie</Objecten:gebruiksdoel>
                                <Objecten:gebruiksdoel>kantoorfunctie</Objecten:gebruiksdoel>
                                <Objecten:oppervlakte>209494</Objecten:oppervlakte>
                                <Objecten:status>Verblijfsobject in gebruik</Objecten:status>
                                <Objecten:geconstateerd>N</Objecten:geconstateerd>
                                <Objecten:documentdatum>1900-01-01</Objecten:documentdatum>
                                <Objecten:documentnummer>docnr</Objecten:documentnummer>
                                <Objecten:maaktDeelUitVan>
                                    <Objecten-ref:PandRef domein="NL.IMBAG.Pand">pndA</Objecten-ref:PandRef>
                                </Objecten:maaktDeelUitVan>
                                          <Objecten:maaktDeelUitVan>
                                    <Objecten-ref:PandRef domein="NL.IMBAG.Pand">pndB</Objecten-ref:PandRef>
                                </Objecten:maaktDeelUitVan>
                            </Objecten:Verblijfsobject>
                        </sl-bag-extract:bagObject>
                    </sl:stand>
                </sl:standBestand>
            </sl-bag-extract:bagStand>
            """

        f = io.StringIO(xml)
        mocked_tree = ET.parse(f)

        ds = self.get_test_object()
        ds.files = ['the file']

        with patch("gobcore.datastore.bag_extract.ET.parse") as mock_parse:
            mock_parse.return_value = mocked_tree
            res = list(ds.query(None))

        expected = [{
            'documentdatum': '1900-01-01',
            'documentnummer': 'docnr',
            'gebruiksdoel': ['woonfunctie', 'industriefunctie', 'kantoorfunctie'],
            'geconstateerd': 'N',
            'geometrie/punt': 'POINT (131419 482833)',  # GML to WKT, and 3D to 2D
            'heeftAlsHoofdadres/NummeraanduidingRef': 'hoofdA',  # returned as single value
            'heeftAlsNevenadres/NummeraanduidingRef': ['nevenA', 'nevenB'],  # returned as list
            'identificatie': 'votA',
            'maaktDeelUitVan/PandRef': ['pndA', 'pndB'],
            'oppervlakte': '209494',
            'status': 'Verblijfsobject in gebruik',
            'voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV': '2010-11-15T13:31:10.557',
            'voorkomen/Voorkomen/beginGeldigheid': '2010-08-31',
            'voorkomen/Voorkomen/tijdstipRegistratie': '2010-11-15T13:22:03.000',
            'voorkomen/Voorkomen/voorkomenidentificatie': '1'
        }]

        self.assertEqual(expected, res)
        mock_parse.assert_called_with('the file')

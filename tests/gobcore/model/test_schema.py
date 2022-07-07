import unittest
import mock

from ..amschema_fixtures import get_dataset, get_table
from gobcore.model import Schema
from gobcore.model.schema import load_schema


class TestAMSSchema(unittest.TestCase):

    def setUp(self):
        pass

    @mock.patch("gobcore.model.schema.AMSchemaRepository")
    def test_load_schema(self, mock_repository):
        schema = Schema(datasetId="dataset", tableId="tableId", version="1.0")
        dataset = get_dataset()
        table = get_table()

        mock_repository.return_value.get_schema.return_value = table, dataset

        expected = {
            'attributes': {
                'datum_actueel_tot': {
                    'description': 'Einddatum van de cyclus, eventueel in combinatie met het kenmerk Status',
                    'type': 'GOB.String'
                },
                'geometrie': {
                    'description': 'Geometrische ligging van de meetbout',
                    'srid': 28992,
                    'type': 'GOB.Geo.Point'},
                'hoogte_tov_nap': {
                    'description': 'Hoogte van het peilmerk t.o.v. NAP',
                    'type': 'GOB.Decimal'
                },
                'identificatie': {
                    'description': 'Het peilmerknummer van het peilmerk.',
                    'type': 'GOB.String'
                },
                'jaar': {
                    'description': 'Het jaar van waterpassing, behorende bij de hoogte.',
                    'type': 'GOB.Integer'
                },
                'ligt_in_gebieden_bouwblok': {
                    'description': 'Het bouwblok waarbinnen het peilmerk ligt',
                    'ref': 'gebieden:bouwblokken',
                    'type': 'GOB.Reference'
                },
                'merk': {
                    'type': 'GOB.JSON',
                    'attributes': {
                        'code': {
                            'type': 'GOB.String',
                        },
                        'omschrijving': {
                            'type': 'GOB.String',
                        }
                    }
                },
                'merk_code': {
                    'description': 'Merk van het referentiepunt code',
                    'type': 'GOB.String'
                },
                'merk_omschrijving': {
                    'description': 'Merk van het referentiepunt omschrijving',
                    'type': 'GOB.String'
                },
                'omschrijving': {
                    'description': 'Beschrijving van het object waarin het peilmerk zich bevindt.',
                    'type': 'GOB.String'
                },
                'publiceerbaar': {
                    'description': 'Publiceerbaar ja of nee',
                    'type': 'GOB.Boolean'
                },
                'rws_nummer': {
                    'description': 'Nummer dat Rijkswaterstaat hanteert.',
                    'type': 'GOB.String'
                },
                'status_code': {
                    'description': 'Status van het referentiepunt (1=actueel, 2=niet te meten, 3=vervallen) code',
                    'type': 'GOB.Integer'
                },
                'status_omschrijving': {
                    'description': 'Status van het referentiepunt (1=actueel, 2=niet te meten, 3=vervallen) omschrijving',
                    'type': 'GOB.String'}
                ,
                'vervaldatum': {
                    'description': 'Vervaldatum van het peilmerk.',
                    'type': 'GOB.String'
                },
                'windrichting': {
                    'description': 'Windrichting',
                    'type': 'GOB.String'
                },
                'x_coordinaat_muurvlak': {
                    'description': 'X-coördinaat muurvlak',
                    'type': 'GOB.Decimal'
                },
                'y_coordinaat_muurvlak': {
                    'description': 'Y-coördinaat muurvlak',
                    'type': 'GOB.Decimal'
                }
            },
            'entity_id': 'identificatie',
            'version': 'ams_2.0.0'
        }

        result = load_schema(schema)
        self.assertEqual(expected, result)

        mock_repository.return_value.get_schema.assert_called_with(schema)

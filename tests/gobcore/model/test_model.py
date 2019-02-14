import unittest

from gobcore.model import GOBModel


class TestModel(unittest.TestCase):

    def setUp(self):
        self.model = GOBModel()

    def test_get_catalog_names(self):
        self.assertIn('meetbouten', self.model.get_catalog_names())

    def test_get_catalogs(self):
        catalogs = self.model.get_catalogs()
        self.assertIsInstance(catalogs.items(), type({}.items()))
        self.assertIn('meetbouten', catalogs.keys())

    def test_get_collection_names(self):
        self.assertIn('meetbouten', self.model.get_collection_names('meetbouten'))

    def test_get_table_names(self):
        self.assertIn('meetbouten_meetbouten', self.model.get_table_names())

    def test_get_table_name(self):
        self.assertEqual('meetbouten_meetbouten', self.model.get_table_name('meetbouten', 'meetbouten'))

    def test_get_reference_by_abbreviations(self):
        # Test lower and upper case
        self.assertEqual('meetbouten:meetbouten', self.model.get_reference_by_abbreviations('mbn', 'mbt'))
        self.assertEqual('meetbouten:meetbouten', self.model.get_reference_by_abbreviations('mbn', 'MBT'))

        # Test non existing abbreviation
        self.assertEqual(None, self.model.get_reference_by_abbreviations('xxx', 'xxx'))

    def test_functional_key_fields(self):
        fields = self.model.get_functional_key_fields('meetbouten', 'meetbouten')
        self.assertEqual(fields, ['_source', 'identificatie'])

    def test_technical_key_fields(self):
        fields = self.model.get_technical_key_fields('meetbouten', 'meetbouten')
        self.assertEqual(fields, ['_source', '_source_id'])

    def test_source_id(self):
        entity = {
            'idfield': 'idvalue'
        }
        spec = {
            'catalogue': 'meetbouten',
            'entity': 'meetbouten',
            'source': {
                'entity_id': 'idfield'
            }
        }
        source_id = self.model.get_source_id(entity, spec)
        self.assertEqual(source_id, 'idvalue')

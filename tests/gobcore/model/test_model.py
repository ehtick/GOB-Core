import unittest
from unittest.mock import MagicMock

from gobcore.exceptions import GOBException
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

    def test_set_api(self):
        model = {}
        self.model._set_api('meetbouten', 'meetbouten', model)
        self.assertEqual(model, {'api': {'filters': []}})

    def test_get_table_name_from_ref(self):
        self.assertEqual("abc_def", self.model.get_table_name_from_ref("abc:def"))

    def test_get_table_name_from_ref_error(self):
        self.model.split_ref = MagicMock(side_effect=GOBException)

        with self.assertRaises(GOBException):
            self.model.get_table_name_from_ref("something_invalid")

    def test_split_ref(self):
        error_testcases = [
            "abc:",
            ":abc",
            "abc"
            "abc:def:ghi"
        ]

        for testcase in error_testcases:
            with self.assertRaises(GOBException):
                self.model.split_ref(testcase)

    def test_get_collection_from_ref(self):
        self.model.get_collection = MagicMock(return_value={"fake": "collection"})
        self.model.split_ref = MagicMock(return_value=("some", "reference"))

        result = self.model.get_collection_from_ref("some:reference")

        self.assertEqual({"fake": "collection"}, result)
        self.model.split_ref.assert_called_with("some:reference")
        self.model.get_collection.assert_called_with("some", "reference")

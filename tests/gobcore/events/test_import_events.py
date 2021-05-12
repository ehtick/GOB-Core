import unittest
from unittest.mock import MagicMock

from gobcore.events import import_events
from gobcore.exceptions import GOBException
from gobcore.model import GOBModel

from tests.gobcore import fixtures


class TestImportEvents(unittest.TestCase):

    def test_add_create_event(self):
        tid = fixtures.random_string()
        event = import_events.ADD.create_event(tid, {}, '0.9')
        self.assertEqual(event['event'], 'ADD')
        self.assertEqual(event['version'], '0.9')

    def test_modify_create_event(self):
        tid = fixtures.random_string()
        event = import_events.MODIFY.create_event(tid, {'modifications': [], '_hash': fixtures.random_string()}, '0.9')
        self.assertEqual(event['event'], 'MODIFY')
        self.assertEqual(event['version'], '0.9')

    def test_modify_create_event_without_modifications(self):
        tid = fixtures.random_string()

        with self.assertRaises(GOBException):
            event = import_events.MODIFY.create_event(tid, {'_hash': fixtures.random_string()}, '0.9')

    def test_delete_create_event(self):
        tid = fixtures.random_string()
        event = import_events.DELETE.create_event(tid, {}, '0.9')
        self.assertEqual(event['event'], 'DELETE')
        self.assertEqual(event['version'], '0.9')

    def test_confirm_create_event(self):
        tid = fixtures.random_string()
        event = import_events.CONFIRM.create_event(tid, {}, '0.9')
        self.assertEqual(event['event'], 'CONFIRM')
        self.assertEqual(event['version'], '0.9')

    def test_bulkconfirm_create_event(self):
        event = import_events.BULKCONFIRM.create_event([], '0.9')
        self.assertEqual(event['event'], 'BULKCONFIRM')
        self.assertEqual(event['version'], '0.9')

    def test_add_apply_to(self):
        tid = fixtures.random_string()
        event = import_events.ADD.create_event(tid, {'identificatie': tid}, '0.9')
        self.assertEqual(event['version'], '0.9')

        metadata = fixtures.get_metadata_fixture()
        entity = fixtures.get_entity_fixture(event['data'])

        gob_event = import_events.ADD("tid", event['data'], metadata)
        gob_event.apply_to(entity)

        # Assert entity has the attribute with the value
        self.assertEqual(entity.identificatie, tid)

    def test_modify_apply_to(self):
        tid = fixtures.random_string()
        data = {
            'identificatie': tid,
            '_hash': fixtures.random_string(),
            'modifications': [{
                'key': 'identificatie',
                'new_value': 'new identificatie',
                'old_value': 'old identificatie',
            }]
        }
        event = import_events.MODIFY.create_event(tid, data, '0.9')
        entity = fixtures.get_entity_fixture({
            'identificatie': 'old identificatie',
        })
        gob_event = import_events.MODIFY("tid", event['data'], fixtures.get_metadata_fixture())
        gob_event._extract_modifications = MagicMock(return_value={'identificatie': 'new identificatie'})
        gob_event.apply_to(entity)

        self.assertEqual(entity.identificatie, 'new identificatie')
        self.assertEqual(event['version'], '0.9')

    def test_modify_extract_modifications(self):
        modifications = [{
            'key': 'a',
            'new_value': 'new a',
            'old_value': 'old a',
        }, {
            'key': 'b',
            'new_value': 'new b',
            'old_value': 'old b',
        }]

        gob_event = import_events.MODIFY("tid", {}, fixtures.get_metadata_fixture())
        self.assertEqual({'a': 'new a', 'b': 'new b'}, gob_event._extract_modifications({}, modifications))

    def test_catalogue_entity_source(self):
        metadata = MagicMock()
        metadata.catalogue = 'cat'
        metadata.entity = 'coll'
        metadata.source = 'source'

        import_events.ADD.gob_model = MagicMock()

        event = import_events.ADD("tid", {}, metadata)
        self.assertEqual('cat', event.catalogue)
        self.assertEqual('coll', event.entity)
        self.assertEqual('source', event.source)

        # Put back
        import_events.ADD.gob_model = GOBModel()

    def test_actions_names(self):
        testcases = [
            (import_events.ADD, 'ADD', 'ADD'),
            (import_events.MODIFY, 'MODIFY', 'MODIFY'),
            (import_events.DELETE, 'DELETE', 'DELETE'),
            (import_events.CONFIRM, 'CONFIRM', 'CONFIRM'),
            (import_events.BULKCONFIRM, 'BULKCONFIRM', 'CONFIRM'),
        ]

        for event, name, action in testcases:
            event.gob_model = MagicMock()
            event_obj = event("tid", {}, MagicMock())

            self.assertEqual(name, event_obj.name)
            self.assertEqual(action, event_obj.action)
            self.assertEqual("tid", event_obj.tid)

            # Put back to avoid failing tests using this object
            event.gob_model = GOBModel()

from unittest import TestCase
from unittest.mock import MagicMock

from gobcore.quality.quality_update import QualityUpdate
from gobcore.typesystem.gob_types import Decimal

class TestQualityUpdate(TestCase):

    def test_get_contents(self):
        qa = QualityUpdate()

        mock_issue = MagicMock()
        for value in 1, True, 2.5, "any value", Decimal(1.5):
            mock_issue.value = value
            contents = qa.get_contents(mock_issue)
            self.assertEqual(contents['betwijfelde_waarde'], str(value))

        header = {
            'source': 'any source',
            'application': 'any application',
            'catalogue': 'any catalogue',
            'collection': 'any collection',
            'attribute': 'any attribute',
            'mode': 'any mode',
        }

        # Enrich bevinding with msg info
        for attribute in ['source', 'application', 'catalogue', 'collection', 'attribute']:
            setattr(qa, attribute, header.get(f"original_{attribute}", header.get(attribute)))

        self.assertEqual("any source_any application_any attribute", qa.get_source())

        qa_header = qa.get_header(header)
        self.assertEqual(qa_header['catalogue'], 'qa')
        self.assertEqual(qa_header['collection'], 'any catalogue_any collection')
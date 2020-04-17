from unittest import TestCase
from unittest.mock import MagicMock

from gobcore.quality.quality_update import QualityUpdate
from gobcore.typesystem.gob_types import Decimal

class TestQualityUpdate(TestCase):

    def test_get_contents(self):
        qa = QualityUpdate([])

        mock_issue = MagicMock()
        for value in 1, True, 2.5, "any value", Decimal(1.5):
            mock_issue.value = value
            contents = qa.get_contents(mock_issue)
            self.assertEqual(contents['betwijfelde_waarde'], str(value))

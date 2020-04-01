from unittest import TestCase

from gobcore.quality.config import QA_LEVEL, QA_CHECK


class TestConfig(TestCase):

    def test_qa_level(self):
        self.assertIsNotNone(QA_LEVEL.FATAL)
        self.assertIsNotNone(QA_LEVEL.WARNING)
        self.assertIsNotNone(QA_LEVEL.INFO)

    def test_qa_check(self):
        for prop in [prop for prop in dir(QA_CHECK) if not prop.startswith('_')]:
            check = getattr(QA_CHECK, prop)
            self.assertTrue(isinstance(check, dict))
            self.assertIsNotNone(check['msg'])
            self.assertEqual(check['id'], prop)

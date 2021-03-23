from unittest import TestCase

from gobcore.enum import ImportMode


class EnumTestCase(TestCase):

    def test_enums(self):
        self.assertEqual(ImportMode.FULL.value, 'full')

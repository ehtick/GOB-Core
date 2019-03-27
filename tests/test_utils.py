import unittest

from sys import getsizeof

from gobcore.utils import getapproxsizeof, gettotalsizeof


class TestLogPublisher(unittest.TestCase):

    def testApproxsizeof(self):
        size = getapproxsizeof("a")
        self.assertEqual(size, getsizeof("a"))
        size = getapproxsizeof({"a": 1})
        self.assertTrue(size > 1)
        size = getapproxsizeof(["a"])
        self.assertTrue(size > 1)

    def testTotalsizeof(self):
        size = gettotalsizeof("a")
        self.assertEqual(size, getsizeof("a"))
        size = gettotalsizeof({"a": 1})
        self.assertTrue(size > 1)
        size = gettotalsizeof(["a"])
        self.assertTrue(size > 1)

    def testApproxAndTotalsizeof(self):
        for item in True, False, 1, "a", 1.5:
            self.assertEqual(gettotalsizeof(item), getapproxsizeof(item))
        for item in {"a": 1}, ["a"]:
            self.assertTrue(gettotalsizeof(item) >= getapproxsizeof(item))

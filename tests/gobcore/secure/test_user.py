import unittest
import mock

from gobcore.secure.config import LEVELS, ROLES
from gobcore.secure.user import User


class TestUser(unittest.TestCase):

    def setup(self):
        pass


    def test_create(self):
        user = User(None)
        self.assertIsNotNone(user._roles)

    def test_has_access(self):
        user = User(None)
        user._roles = []
        value = {"l": ROLES["user"]}
        self.assertFalse(user.has_access_to(value))

        user._roles = ["user"]
        value = {"l": ROLES["user"]}
        self.assertTrue(user.has_access_to(value))

        user._roles = ["user"]
        value = {"l": ROLES["admin"]}
        self.assertFalse(user.has_access_to(value))

import unittest
import mock

from gobcore.typesystem.gob_secure_types import SecureString, SecureDecimal, SecureDateTime
from gobcore.secure.crypto import read_protect
from gobcore.secure.user import User
from gobcore.secure.config import ROLES


class TestSecure(unittest.TestCase):

    def setup(self):
        pass


    def test_create(self):
        level = ROLES["user"]
        user = User(None)
        user._roles = ["user"]

        sec_string = SecureString.from_value(read_protect("some string"), level=10)
        self.assertTrue(isinstance(sec_string, SecureString))
        self.assertEqual(sec_string.get_value(user), "some string")

        sec_decimal = SecureDecimal.from_value(read_protect(10), level=10)
        self.assertTrue(isinstance(sec_decimal, SecureDecimal))
        self.assertEqual(sec_decimal.get_value(user), 10)

        sec_datetime = SecureDateTime.from_value(read_protect("2000-01-25T12:25:45.0"), level=10)
        self.assertTrue(isinstance(sec_datetime, SecureDateTime))

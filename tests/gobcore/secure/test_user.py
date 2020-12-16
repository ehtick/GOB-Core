from unittest import TestCase, mock

from gobcore.secure.config import GOB_SECURE_ATTRS
from gobcore.secure.user import User


@mock.patch("gobcore.secure.user.extract_roles")
class TestUser(TestCase):

    def test_create(self, mock_extract):
        mock_extract.return_value = ['role1', 'role2']
        mock_request = mock.MagicMock()
        user = User(mock_request)
        self.assertEqual(['role1', 'role2'], user._roles)
        mock_extract.assert_called_with(mock_request.headers)

    def test_has_access(self, mock_extract):
        mock_request = mock.MagicMock()
        mock_request.headers = {}
        user = User(mock_request)
        value = "any encrypted value"

        user._roles = []
        self.assertFalse(user.has_access_to(value))

        user._roles = ["any user"]
        self.assertFalse(user.has_access_to(value))

        user._roles = [GOB_SECURE_ATTRS]
        self.assertTrue(user.has_access_to(value))

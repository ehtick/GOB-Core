import os

from gobcore.secure.config import ROLES
from gobcore.secure.crypto import confidence_level


class User:

    def __init__(self, request):
        """
        A user is instantiated by the parameters in the HTTPS request

        :param request:
        """
        # roles = request.headers.get("X-Auth-Roles", "")
        roles = os.getenv("ROLES", "")
        self._roles = roles.split(",")

    def has_access_to(self, encrypted_value):
        """
        Tells if the user has access to the given encrypted value

        :param encrypted_value:
        :return:
        """
        required_level = confidence_level(encrypted_value)
        return any(ROLES.get(user_level, -1) >= required_level for user_level in self._roles)

from gobcore.secure.config import GOB_SECURE_ATTRS
from gobcore.secure.request import extract_roles


class User:

    def __init__(self, request):
        """
        A user is instantiated by the parameters in the HTTPS request

        :param request:
        """
        self._roles = extract_roles(request.headers)

    def has_access_to(self, encrypted_value):
        """
        Tells if the user has access to the given encrypted value

        :param encrypted_value:
        :return:
        """

        # For now only one role that is needed to access any encrypted value
        return GOB_SECURE_ATTRS in self._roles

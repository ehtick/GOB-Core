from gobcore.secure.config import REQUEST_ROLES, GOB_SECURE_ATTRS


class User:

    def __init__(self, request):
        """
        A user is instantiated by the parameters in the HTTPS request

        :param request:
        """
        roles = request.headers.get(REQUEST_ROLES, "")
        self._roles = roles.split(",")

    def has_access_to(self, encrypted_value):
        """
        Tells if the user has access to the given encrypted value

        :param encrypted_value:
        :return:
        """

        # For now only one role that is needed to access any encrypted value
        return GOB_SECURE_ATTRS in self._roles

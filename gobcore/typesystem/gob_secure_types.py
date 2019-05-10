import json

from gobcore.typesystem.gob_types import JSON, String, Decimal, DateTime
from gobcore.secure.crypto import is_encrypted, encrypt, decrypt, read_unprotect


class Secure(JSON):
    """
    Secure types are derived from the JSON type
    """
    name = "Secure"
    is_secure = True

    def __init__(self, value, level=None):
        if not is_encrypted(value):
            assert level is not None, "Missing level to encrypt the given value"
            value = encrypt(value, confidence_level=level)
        super().__init__(json.dumps(value))

    @classmethod
    def from_value(cls, value, **kwargs):
        if "level" in kwargs:
            level = kwargs["level"]
            del kwargs["level"]
            value = read_unprotect(value)
        else:
            level = None

        print(f"{cls.BaseType.name} from value", value, type(value))
        if not is_encrypted(value):
            value = cls.BaseType.from_value(value, **kwargs)

        return cls(value, level)

    def get_value(self, user=None):
        print("Get secure value", self._string)
        if user is None:
            has_access = False
        else:
            value = json.loads(self._string)
            has_access = user.has_access_to(value)

        if has_access:
            value = decrypt(value)
            return None if value is None else self.get_typed_value(value)
        else:
            return "*" * 10


class SecureString(Secure):
    name = "SecureString"
    BaseType = String

    def get_typed_value(self, value):
        return value


class SecureDecimal(Secure):
    name = "SecureDecimal"
    BaseType = Decimal

    def get_typed_value(self, value):
        return float(value)


class SecureDateTime(Secure):
    name = "SecureDateTime"
    BaseType = DateTime

    def get_typed_value(self, value):
        # Behave like its BaseType...
        return DateTime.from_value(value).to_value

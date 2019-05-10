"""GOB cryptograhic functions

"""
import json
import base64


def is_encrypted(value):
    """
    Tells if a value is an encrypted value

    :param value: any value
    :return: True when the value is an encrypted value
    """
    keys = ["i", "l", "v"]
    return isinstance(value, dict) and \
        all([key in value for key in keys]) and \
        len(value.keys()) == len(keys)


def confidence_level(encrypted_value):
    """
    Tells the confidence level of an encrypted value

    :param encrypted_value: any encrypted value
    :return: the required confidence level to have access to the value
    """
    return encrypted_value["l"]


def encrypt(value, confidence_level):
    """
    Encrypt a value with the encryption for the given confidence level

    :param value:
    :param confidence_level:
    :return:
    """
    key_index, secret = _get_encrypt_secret(confidence_level)
    return {
        "i": key_index,               # Allows for key change
        "l": confidence_level,        # Some data is more confident that other data
        "v": _encrypt(value, secret)  # The encrypted data
    }


def decrypt(value):
    """
    Decrypt an encrypted value

    :param value:
    :return:
    """
    key_index = value["i"]
    confidence_level = value["l"]
    secret = _get_decrypt_secret(key_index, confidence_level)
    return _decrypt(value["v"], secret)


# Use a strong and fast asymetric encryption algorithm like RSA, ECDH, ...


def _get_encrypt_secret(confidence_level):
    # return id and secret
    key_index = 0
    secret = "Some very complex asymetric cryptograhic thing"
    return key_index, secret


def _get_decrypt_secret(key_index, confidence_level):
    return "Some very complex asymetric cryptograhic thing"


def _encrypt(value, _):
    # Use a strong and fast asymetric encryption mechanism
    return base64.b64encode(str(value).encode()).decode('UTF-8')


def _decrypt(value, _):
    # Use a strong and fast asymetric encryption mechanism
    value = base64.b64decode(value).decode('UTF-8')
    print("DECRYPT", value, type(value), value == "None")
    return None if value == "None" else value


# Use a safe symetric encryption algorithm within GOB


def read_protect(value):
    json_value = json.dumps(value)
    return base64.b64encode(json_value.encode()).decode('UTF-8')


def read_unprotect(value):
    json_value = base64.b64decode(value).decode('UTF-8')
    return json.loads(json_value)

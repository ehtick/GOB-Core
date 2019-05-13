"""GOB cryptograhic functions

"""
import random
import json
# import base64

from gobcore.secure.demo import demo


_safe_storage = {}


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
    """
    Get the secret and its index to encrypt a value with the given confidence level

    :param confidence_level: confidence level of the value to encrypt
    :return: secret
    """
    return demo["index"], demo["secret"]


def _get_decrypt_secret(key_index, confidence_level):
    """
    Get the secret to decrypt an encrypted value with the given confidence level

    :param key_index: index of the secret
    :param confidence_level: confidence level of the encrypted value
    :return:
    """
    return demo["secret"]


def _encrypt(value, secret):
    """
    Encrypt a value using the give secret

    :param value: value to encrypt
    :param secret: secret to use to encrypt the value
    :return: the encrypted value
    """
    return demo["encrypt"](value, secret)


def _decrypt(value, secret):
    """
    Decrypt a valye using the given secret

    :param value: value to decrypt
    :param secret: secret to use to decrypt the value
    :return: the decrypted value
    """
    # Use a strong and fast asymetric encryption mechanism
    return demo["decrypt"](value, secret)


# Use a safe symetric encryption algorithm within GOB
# Or store it locally when used within one process cycle


def read_protect(value, save_local=True):
    """
    Protect sensitive data by storing it locally or encrypt its value

    :param value: any sensitive data
    :param save_local: store it locally if true, else encrypt
    :return: the key to the sensitive data or the encrypted value
    """
    if save_local:
        key = random.random()
        _safe_storage[key] = value
        return key
    else:
        return demo["encrypt"](json.dumps(value), None)


def read_unprotect(value):
    """
    Unprotect a previously read protected data

    :param value: the key to the sensitive data or the encrypted value
    :return: the unprotected value
    """
    if value in _safe_storage:
        saved_value = _safe_storage[value]
        del _safe_storage[value]
        return saved_value
    else:
        return json.loads(demo["decrypt"](value, None))

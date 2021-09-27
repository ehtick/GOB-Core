import unittest
from unittest.mock import patch

from gobcore.secure.cryptos.aes import AESCrypto, DecryptionError


class TestAES(unittest.TestCase):

    @patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_create(self):
        self.assertIsNotNone(AESCrypto())

    @patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_encrypt_decrypt(self):
        _, enc = AESCrypto().encrypt("any value")
        self.assertEqual(AESCrypto().decrypt(enc), "any value")

    @patch('gobcore.secure.cryptos.config.os.getenv', lambda s, *args: s)
    def test_encrypt_decrypt_value_error(self):
        _, enc = AESCrypto().encrypt("any value")
        self.assertEqual(AESCrypto().decrypt(enc), "any value")

        with self.assertRaises(DecryptionError):
            AESCrypto().decrypt(f"_{enc}")

    @patch('gobcore.secure.cryptos.config.os.getenv')
    def test_encrypt_decrypt_key_error(self, mock_get_env):
        mock_get_env.side_effect = lambda s, *args: s
        _, enc = AESCrypto().encrypt("any value")
        self.assertEqual(AESCrypto().decrypt(enc), "any value")

        _, enc1 = AESCrypto().encrypt("any value")
        _, enc2 = AESCrypto().encrypt("any value")
        self.assertEqual(enc1, enc2)

        AESCrypto._ciphers = None
        mock_get_env.side_effect = lambda s, *args: f"_{s}"
        with self.assertRaises(DecryptionError):
            AESCrypto().decrypt(enc)

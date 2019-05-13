"""
Demo for crypto

Uses base64 encode/decode for demo purposes

"""
import base64


def _encrypt(value, _):
    return base64.b64encode(str(value).encode()).decode('UTF-8')


def _decrypt(value, _):
    value = base64.b64decode(value).decode('UTF-8')
    return None if value == "None" else value


demo = {
    "index": 0,
    "secret": "DEMO",
    "encrypt": _encrypt,
    "decrypt": _decrypt
}

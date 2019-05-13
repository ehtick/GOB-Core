"""
Demo for crypto

Uses base64 encode/decode for demo purposes

"""
import base64


demo = {
    "index": 0,
    "secret": "DEMO",
    "encrypt": lambda value, secret: base64.b64encode(str(value).encode()).decode('UTF-8'),
    "decrypt": lambda value, secret: base64.b64decode(value).decode('UTF-8')
}

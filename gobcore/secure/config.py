"""
User authorization levels and roles

"""


LEVELS = {
    10: "low",
    20: "moderate",
    30: "high"
}


ROLES = {
    "anonymous": 0,
    "user": 10,
    "poweruser": 20,
    "admin": 30
}

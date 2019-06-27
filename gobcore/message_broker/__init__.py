from gobcore.message_broker.async_message_broker import AsyncConnection
from gobcore.message_broker.config import CONNECTION_PARAMS


def publish(exchange, key, msg):
    with AsyncConnection(CONNECTION_PARAMS) as connection:
        connection.publish(exchange, key, msg)

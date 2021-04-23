from gobcore.message_broker.brokers.broker import get_connection


def publish(exchange, key, msg):
    with get_connection() as connection:
        connection.publish(exchange, key, msg)

from gobcore.message_broker.brokers.broker import msg_broker


def publish(exchange, key, msg):
    with msg_broker.connection() as connection:
        connection.publish(exchange, key, msg)

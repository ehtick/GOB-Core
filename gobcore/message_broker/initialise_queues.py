"""Message broker initialisation

The message broker serves a number of persistent queues holding persistent messages.

Creating persistent queues is an independent task.

The queues are required by the import and upload modules.
These modules are responsable for importing and uploading and not for creating the queues.

The initialisation of the queues is an integral part of the initialisation and startup of the message broker.

"""
from typing import List
from functools import wraps
from gobcore.message_broker.config import MESSAGE_BROKER
from gobcore.message_broker.brokers.broker import get_manager


def manager_ctx(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        with get_manager() as m:
            return f(*args, **kwargs, manager=m)

    return wrapper


@manager_ctx
def create_queue_with_binding(exchange: str, queue: str, keys: List[str], manager):
    print(f'Create queue with bindings for {MESSAGE_BROKER}')
    manager.create_queue_with_binding(exchange, queue, keys)


@manager_ctx
def initialize_message_broker(manager):

    """
    Initializes the RabbitMQ message broker.

    Creates a virtual host and persistent exxhanges and queues
    :return:
    """
    print(f"Initialize message broker {MESSAGE_BROKER}")
    manager.create_all()

from gobcore.exceptions import GOBException
from gobcore.message_broker.config import MESSAGE_BROKER_TYPE

from .config import MESSAGE_BROKER_TYPE_AZURE, MESSAGE_BROKER_TYPE_RABBITMQ
from .azure_service_bus import AsyncConnection as conn_azure
from .azure_service_bus import BrokerManager as AzureBrokerManager
from .rabbitmq import AsyncConnection as conn_rabbitmq
from .rabbitmq import BrokerManager as RabbitMQBrokerManager

BROKER_MAP = {
    MESSAGE_BROKER_TYPE_AZURE: (AzureBrokerManager, conn_azure),
    MESSAGE_BROKER_TYPE_RABBITMQ: (RabbitMQBrokerManager, conn_rabbitmq)
}


def _get(idx, **kwargs):
    try:
        return BROKER_MAP[MESSAGE_BROKER_TYPE][idx](**kwargs)
    except KeyError:
        message_broker_types = [MESSAGE_BROKER_TYPE_RABBITMQ, MESSAGE_BROKER_TYPE_AZURE]
        raise GOBException(
            f'Invalid MESSAGE_BROKER_TYPE={MESSAGE_BROKER_TYPE}, available={message_broker_types}')


def get_manager():
    return _get(0)


def get_async_connection(params=None):
    return _get(1, params=params)

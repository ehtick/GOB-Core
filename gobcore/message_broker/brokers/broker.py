'''
    Define all the available Message brokers her
'''
from gobcore.exceptions import GOBException
from gobcore.message_broker.config import MESSAGE_BROKER_TYPE

from .config import MESSAGE_BROKER_TYPE_AZURE, MESSAGE_BROKER_TYPE_RABBITMQ
from .azure_service_bus import azure_connectors
from .rabbitmq import rabbitmq_connectors

BROKER_MAP = {
    MESSAGE_BROKER_TYPE_AZURE: azure_connectors,
    MESSAGE_BROKER_TYPE_RABBITMQ: rabbitmq_connectors,
}


class MsgBroker:

    @classmethod
    def _get(cls, idx, **kwargs):
        try:
            return BROKER_MAP[MESSAGE_BROKER_TYPE][idx](**kwargs)
        except KeyError:
            message_broker_types = [MESSAGE_BROKER_TYPE_RABBITMQ, MESSAGE_BROKER_TYPE_AZURE]
            raise GOBException(
                f'Invalid MESSAGE_BROKER_TYPE={MESSAGE_BROKER_TYPE}, available={message_broker_types}')

    @classmethod
    def manager(cls):
        return cls._get(0)

    @classmethod
    def connection(cls, params=None):
        return cls._get(1)

    @classmethod
    def async_connection(cls, params=None):
        return cls._get(2, params=params)


msg_broker = MsgBroker()

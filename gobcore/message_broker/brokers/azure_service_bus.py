import json
import traceback
import os
import datetime


from azure.servicebus.management import ServiceBusAdministrationClient, SqlRuleFilter
from azure.servicebus._base_handler import ServiceBusSharedKeyCredential
from azure.core.exceptions import ResourceExistsError
from azure.servicebus import ServiceBusClient, ServiceBusMessage

from gobcore.message_broker.offline_contents import offload_message, end_message
from gobcore.message_broker.config import (
    MESSAGE_BROKER, MESSAGE_BROKER_USER, MESSAGE_BROKER_PASSWORD, QUEUE_CONFIGURATION,
    EVERYTHING_KEY
)
from gobcore.message_broker.utils import to_json, get_message_from_body, StoppableThread
from gobcore.message_broker.brokers.broker_class import BrokerManager, Connection, AsyncConnection


def progress(*args):
    """Utility function to facilitate debugging

    :param args: args that will be printed
    :return: None
    """
    # import threading
    # print(f"[{threading.get_ident()}][asb] Progress", *args)
    pass


def _continue_exists(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except ResourceExistsError:
        pass


class AzureBrokerManager(BrokerManager):

    def __init__(self):
        self._connection = None
        self._topic_subscriptions = {}

    def __enter__(self):
        credential = ServiceBusSharedKeyCredential(
            policy=MESSAGE_BROKER_USER, key=MESSAGE_BROKER_PASSWORD)
        self._address = MESSAGE_BROKER
        self._connection = ServiceBusAdministrationClient(self._address, credential)
        return self

    def __exit__(self, *args):
        if self._connection:
            self._connection.close()

    def _create_vhost(vhost):
        '''
          No mapping for vhost, namespace considered but is already in the url
        '''
        pass

    def _get_topics(self):
        return [t.name for t in self._connection.list_topics()]

    def _get_subscriptions(self, exchange: str):
        return [s.name for s in self._connection.list_subscriptions(exchange)]

    def create_exchange(self, exchange, _durable):
        try:
            self._connection.create_topic(exchange)
        except ResourceExistsError:
            pass

    @staticmethod
    def _get_filter(key):
        operator = '=' if key.find('*') == -1 else 'LIKE'
        value = key.replace('*', '%')
        return f"Label {operator} '{value}'"

    def get_filter_args(self, keys):
        rules = ' OR '.join([self._get_filter(key) for key in keys])
        return SqlRuleFilter(f"{rules} ")

    def create_queue_with_binding(self, exchange, queue, keys):
        _continue_exists(self._connection.create_subscription, exchange, queue)

        # Filters use SQL like syntax,
        # Filter names cannot have asterix or dashes in the name
        keys = [key for key in keys if key != EVERYTHING_KEY]
        if keys:
            _continue_exists(self._connection.delete_rule, exchange, queue, '$Default')
            filter_args = self.get_filter_args(keys)
            _continue_exists(self._connection.create_rule, exchange, queue, 'gob.filter', filter=filter_args)

    def _initialize_queues(self):
        # First create all exchanges (some exchanges may not be included in queue_configuration below)
        exchanges = self._get_topics()
        for exchange in QUEUE_CONFIGURATION.keys():
            if exchange not in exchanges:
                self.create_exchange(exchange, True)
            queues = self._get_subscriptions(exchange)
            for queue, keys in QUEUE_CONFIGURATION[exchange].items():
                if queue not in queues:
                    self.create_queue_with_binding(exchange=exchange, queue=queue, keys=keys)

    def _delete_queue(self, topic, subscription):
        self._connection.delete_subscription(topic, subscription)

    def _delete_topic(self, topic):
        self._connection.delete_topic(topic)

    def create_all(self):
        print('* Creating vhost')
        self._create_vhost()
        print('* Creating exchange keys and queues if necessary')
        self._initialize_queues()
        print('* Exchanges and queues initialized')

    def destroy_all(self):
        exchanges = self._get_topics()
        for exchange in set(exchanges).intersection(set(QUEUE_CONFIGURATION.keys())):
            self._delete_topic(exchange)
            print(f' * Deleted exchange {exchange}')
            # No need to cleanup suscriptions, are binded to tpoic and removed when topic is deleted


class AzureConnection(Connection):

    def __init__(self, params=None):
        """Create a new Connection

        :param address: The RabbitMQ address
        """
        self._client = None
        super().__init__(self)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def connect(self):
        credential = ServiceBusSharedKeyCredential(
            policy=MESSAGE_BROKER_USER, key=MESSAGE_BROKER_PASSWORD)
        address = MESSAGE_BROKER
        self._client = ServiceBusClient(address, credential)

    def publish(self, exchange: str, key: str, msg: dict, ttl_msec: int=0):
        with self._client:
            sender = self._client.get_topic_sender(topic_name=exchange)

            msg = offload_message(msg, to_json)
            message = ServiceBusMessage(json.dumps(msg))
            message.label = key
            kwargs = {}
            if ttl_msec:
                kwargs['time_to_live'] = datetime.timedelta(milliseconds=ttl_msec)
            sender.send_messages(message, **kwargs)

    def publish_delayed(self, exchange: str, key: str, queue: str, msg: dict, delay_msec: int=0):
        with self._client:
            sender = self._client.get_topic_sender(topic_name=exchange)

            msg = offload_message(msg, to_json)
            scheduled_time_utc = datetime.datetime.utcnow() + datetime.timedelta(milliseconds=delay_msec)
            message = ServiceBusMessage(json.dumps(msg))
            message.label = key
            sender.schedule_messages(message, scheduled_time_utc)

    def disconnect(self):
        """Disconnect from RabbitMQ

        Close the connection
        Stop any running eventloop

        :return: None
        """
        if self._client:
            self._client.close()
        self._client = None


class AzureAsyncConnection(AsyncConnection):
    """This is an asynchronous RabbitMQ connection.

    It handles unexpected conditions when interacting with RabbitMQ

    Threading and locks are used to provide for a synchronous connection setup and
    to allow both publishing and subscribing in parallel.

    The connection allows for an unlimited number of subscriptions.

    Extensive use of closures is made to handle the asynchronous communication with RabbitMQ

    No automatic reconnection with RabbitMQ is implemented.

    """

    def __init__(self, params=None):
        """Create a new AsyncConnection

        :param address: The RabbitMQ address
        """
        # The connection parameters for the RabbitMQ Message broker
        self._address = MESSAGE_BROKER

        # The Connection and Channel objects
        self._subscriber = None
        self._client = None
        self._subscriptions = []

        self._params = {
            "load_message": True,
            "stream_contents": False,
            "prefetch_count": 1
        }
        if params:
            self._params = {**self._params, **params}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        progress('Out of connection ctx', args)
        self.disconnect()

    def is_alive(self):
        # Check all threads, if None return True
        return all(t.is_alive() for t in self._subscriptions)

    def connect(self, on_connect_callback=None):
        progress('Connecting to Azure Service bus', self._address)
        self._credential = ServiceBusSharedKeyCredential(
            policy=MESSAGE_BROKER_USER, key=MESSAGE_BROKER_PASSWORD)
        self._client = ServiceBusClient(self._address, self._credential)
        progress('Connected to Azure Service bus', self._address)

    def publish(self, exchange: str, key: str, msg: dict):
        '''
           Publish a message to exchange = (topic) with Label = key
        '''
        with self._client:
            sender = self._client.get_topic_sender(topic_name=exchange)

            msg = offload_message(msg, to_json)
            message = ServiceBusMessage(json.dumps(msg))
            message.label = key
            sender.send_messages(message)

    @staticmethod
    def _get_exchange(queue: str):
        try:
            return [x for x, y in QUEUE_CONFIGURATION.items() if queue in y.keys()][0]
        except IndexError:
            raise Exception('Unkown queue specified')

    def subscribe(self, queues, message_handler):  # noqa: C901
        """Subscribe to the given queues, for each queue start a new thread

        :param queues: The queues to subscribe on
        :return: List of created threads
        """

        def subscription(stop_subscription, topic, subscription, handler):
            progress('Subsribed', topic, subscription)
            # Each thread must have its own connection....
            key = QUEUE_CONFIGURATION[topic][subscription][0]
            client = ServiceBusClient(self._address, self._credential)
            with client:
                receiver = client.get_subscription_receiver(topic, subscription, max_wait_time=1)
                with receiver:
                    while True:
                        for azure_message in receiver:
                            progress('Message received')
                            body = b''.join(azure_message.message.get_data())
                            progress('Message received 1')
                            # Immediately remove from the queue
                            progress('Message received 2')
                            try:
                                params = {
                                    **self._params,
                                    **self._params.get(queue, {})
                                }
                                progress('Message received 3')
                                # Take care of off loadied messages
                                msg, offload_id = get_message_from_body(body, params)
                                progress(f'Message received 4 {offload_id}')
                                handler(client, topic, subscription, key, msg)
                            except Exception as e:
                                # Print error message, the message that caused the error and a short stacktrace
                                stacktrace = traceback.format_exc(limit=-10)
                                print(f"Message handling has failed: {str(e)}, message: {str(body)}", stacktrace)
                                os._exit(os.EX_TEMPFAIL)
                            if msg is not None and offload_id:
                                end_message(msg, offload_id)
                            progress('Message received 5')
                            receiver.complete_message(azure_message)
                        if stop_subscription():
                            break

        for queue in queues:
            progress('Subscription', queue)
            exchange = self._get_exchange(queue)
            subs = StoppableThread(target=subscription, args=(exchange, queue, message_handler), name=queue)
            subs.start()
            self._subscriptions.append(subs)

        return self._subscriptions

    def receive_msgs(self, queue, max_wait_time=5, max_message_count=1000):
        '''
          Receive message blocking
        '''
        messages = []
        exchange = self._get_exchange(queue)

        with self._client:
            receiver = self._client.get_subscription_receiver(exchange, queue)
            with receiver:
                receiver_msg = receiver.receive_messages(
                    max_message_count=max_message_count, max_wait_time=max_wait_time)
                for msg in receiver_msg:
                    messages.append(b''.join(msg.message.get_data()))
                    receiver.complete_message(msg)
        return messages

    def disconnect(self):
        progress(f"Disconnect")

        for subs in self._subscriptions:
            subs.stop()
            if subs.is_alive():
                subs.join()
        progress(f"Disconnect Joined")


azure_connectors = (BrokerManager, AzureConnection, AzureAsyncConnection)

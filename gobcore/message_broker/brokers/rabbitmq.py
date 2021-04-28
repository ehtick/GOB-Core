import requests
import os
import threading
import traceback
import json

import pika

from gobcore.message_broker.offline_contents import offload_message, end_message
from gobcore.message_broker.utils import to_json, get_message_from_body

from gobcore.message_broker.config import (
    MESSAGE_BROKER, MESSAGE_BROKER_PORT, MESSAGE_BROKER_VHOST,
    MESSAGE_BROKER_USER, MESSAGE_BROKER_PASSWORD, QUEUE_CONFIGURATION,
    EXCHANGES
)

from gobcore.message_broker.brokers.broker_class import BrokerManager, Connection, AsyncConnection


def progress(*args):
    """Utility function to facilitate debugging

    :param args: args that will be printed
    :return: None
    """

    # print("Progress", threading.get_ident(), *args)
    pass


def _get_connection_params():
    return pika.ConnectionParameters(
        host=MESSAGE_BROKER,
        virtual_host=MESSAGE_BROKER_VHOST,
        credentials=pika.PlainCredentials(username=MESSAGE_BROKER_USER,
                                          password=MESSAGE_BROKER_PASSWORD),
        heartbeat=1200,
        blocked_connection_timeout=600
    )


class RabbitMQBrokerManager(BrokerManager):

    def __init__(self):
        self._conn_params = _get_connection_params()
        self._connection = None

    def __enter__(self):
        self._connection = pika.BlockingConnection(self._conn_params)
        return self

    def __exit__(self, *args):
        self._connection.close()

    def _delete_vhost(self, host, port, vhost_name):
        response = requests.delete(
            url=f"http://{host}:{port}/api/vhosts/{vhost_name}",
            headers={
                "content-type": "application/json"
            },
            auth=(MESSAGE_BROKER_USER, MESSAGE_BROKER_PASSWORD),
        )
        response.raise_for_status()

    def _create_vhost(self, host, port, vhost_name):
        """
        Create a virtual host using the RabbtiMQ management interface

        :param vhost: name of the virtual host
        :return:
        """
        response = requests.put(
            url=f"http://{host}:{port}/api/vhosts/{vhost_name}",
            headers={
                "content-type": "application/json"
            },
            auth=(MESSAGE_BROKER_USER, MESSAGE_BROKER_PASSWORD),
        )
        response.raise_for_status()

    def create_exchange(self, exchange):
        """
        Create a RabbitMQ exchange

        :param channel: the RabbitMQ connection channel
        :param exchange: the name of the exchange
        :param durable: specifies wether the exchange should be persistent
        :return:
        """
        with self._connection.channel() as channel:
            channel.exchange_declare(
                exchange=exchange,
                exchange_type="topic",
                durable=True)

    def _create_queue(self, queue, durable, **kwargs):
        """
        Create a RabbitMQ queue

        :param channel: the RabbitMQ connection channel
        :param queue: the name of the queue
        :param durable: specifies wether the queue should be persistent
        :return:
        """
        with self._connection.channel() as channel:
            channel.queue_declare(
                queue=queue,
                durable=durable
            )

    def _bind_queue(self, exchange, queue, key):
        """
        Binds queue to exchange with given key

        :param channel: the RabbitMQ connection channel
        :param exchange: the name of the exchange
        :param queue: the name of the queue
        :param key: the key
        :return:
        """
        with self._connection.channel() as channel:
            channel.queue_bind(
                exchange=exchange,
                queue=queue,
                routing_key=key
            )

    def create_queue_with_binding(self, exchange, queue, keys, **kwargs):
        self._create_queue(queue=queue, durable=True, **kwargs)
        for key in keys:
            self._bind_queue(exchange=exchange, queue=queue, key=key)

    def _initialize_queues(self, queue_configuration):
        # First create all exchanges (some exchanges may not be included in queue_configuration below)
        for exchange in EXCHANGES:
            self.create_exchange(exchange=exchange)

        for exchange, queues in queue_configuration.items():
            for queue, keys in queues.items():
                self.create_queue_with_binding(exchange, queue, keys)

    def create_all(self):
        print('* Creating vhost')
        self._create_vhost(MESSAGE_BROKER, MESSAGE_BROKER_PORT, MESSAGE_BROKER_VHOST)
        print('* Creating exchange keys and queues')
        self._initialize_queues(QUEUE_CONFIGURATION)

    def destroy_all(self):
        print('* Deleting vhost and all exchanges')
        self._delete_vhost(MESSAGE_BROKER, MESSAGE_BROKER_PORT, MESSAGE_BROKER_VHOST)


class RabbitMQConnection(Connection):
    """This is an synchronous RabbitMQ connection.

    No automatic reconnection with RabbitMQ is implemented.

    """

    def __init__(self, params=None):
        """Create a new Connection

        :param address: The RabbitMQ address
        """

        # The connection parameters for the RabbitMQ Message broker
        self._connection_params = _get_connection_params()

        # The Connection and Channel objects
        self._connection = None
        self._channel = None

        # Custom params
        self._params = {
            "load_message": True,
            "stream_contents": False,
            "prefetch_count": 1
        }
        self._params.update(params or {})

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def is_alive(self):
        if self._connection is None or self._channel is None:
            return False
        else:
            return self._connection.is_open and self._channel.is_open

    def connect(self):
        self._connection = pika.BlockingConnection(self._connection_params)
        self._channel = self._connection.channel()

    def publish(self, exchange: str, key: str, msg: dict, ttl_msec: int=0):

        msg = offload_message(msg, to_json)
        message = json.dumps(msg)
        properties = dict(delivery_mode=2)
        if ttl_msec:
            properties['expiration'] = str(ttl_msec)
        self._publish(self, exchange, key, message, properties)

    def publish_delayed(self, exchange: str, key: str, queue: str, msg: dict, delay_msec: int=0):
        '''
            Create a delay message intended for the queue
        '''
        # RabbitMQ does not support scheduled messages, use the
        # dead letter routing to simulate this.
        msg = offload_message(msg, to_json)
        message = json.dumps(msg)
        delay_queue = f"{queue}_delay"
        with RabbitMQBrokerManager() as mconn:
            mconn.create_queue_with_binding(
                exchange, delay_queue, [key],
                arguments={
                    'x-dead-letter-exchange': exchange,
                    'x-dead-letter-routing-key': key
                })
        # Publish a delayed workflow-request message using ttl
        properties = dict(expiration=str(delay_msec))
        self._publish(exchange, key, message, properties)

    def _publish(self, exchange, key, message: str, properties: dict):

        '''
          Publish a message to exchange with key having
        '''
        # Check whether a connection has been established
        if self._channel is None or not self._channel.is_open:
            raise Exception("Connection with message broker not available")

        # Convert the message to json
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=key,
            properties=pika.BasicProperties(**properties),
            body=message,
        )

    def receive_msgs(self, queue, max_wait_time=0, max_message_count=0):

        if max_message_count == 0:
            print('Press CTRL-C to quit...')
        kwargs = {}
        if max_wait_time:
            kwargs['inactivity_timeout'] = int(max_wait_time)
        for method, properties, body in self._channel.consume(queue, **kwargs):
            if method is None:
                break
            yield body
            self._channel.basic_ack(delivery_tag=method.delivery_tag)
            max_message_count -= 1
            if max_message_count == 0:
                break

    def receive_msg(self, queue):
        raise AssertionError('Not needed yet....')

    def disconnect(self):
        """Disconnect from RabbitMQ

        Close any open channels
        Close the connection
        Stop any running eventloop

        :return: None
        """
        # Close any open channel
        if self._channel is not None and self._channel.is_open:
            self._channel.close()
        self._channel = None

        # Close any open connection
        if self._connection is not None:
            self._connection.close()
        self._connection = None


class RabbitMQAsyncConnection(AsyncConnection):
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
        # Custom params
        self._params = {
            "load_message": True,
            "stream_contents": False,
            "prefetch_count": 1
        }
        if params:
            self._params = {**self._params, **params}

        # The Connection and Channel objects
        self._connection = None
        self._channel = None

        # The RabbitMQ eventloop thread
        self._eventloop = None
        self._eventloop_failed = False

        # Optional method, called on connection established
        self._on_connect_callback = None

        # Threaded message handler
        self._message_handler_thread = None

    def __enter__(self):
        progress('Connecting')
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def is_alive(self):
        return not self._eventloop_failed

    def _on_open_connection(self, connection):
        """Called on successful connection to RabbitMQ

        A channel will be created for the communication with RabbitMQ

        :param connection: The connection that has been establishe
        :return: None
        """

        # Register the connection
        self._connection = connection

        def on_close_channel(channel, code, text):
            """Called when a channel is closed

            :param channel: The channel that is closed
            :param code: The code for the channel closure
            :param text: The test for the channel closure
            :return: None
            """

            progress("Channel closed:", code, text)
            self.disconnect()

        def on_open_channel(channel):
            """Called when a channel has been successfully established

            :param channel: The Channel object
            :return: None
            """

            # Handle max 1 message at the same time
            # Do not prefetch next message, just wait for processing to finish and then get next message
            # This prevents messages to get queued after a long running earlier message and get delayed
            channel.basic_qos(prefetch_count=self._params['prefetch_count'], all_channels=True)

            # If a callback has been defined for connection success, call this function
            if self._on_connect_callback:
                self._on_connect_callback()
                # Call it only once
                self._on_connect_callback = None

            # Release the lock set in the connect() function so that this function can return the result
            self._lock.release()

        # Create a Channel, on_open_channel is called on success
        self._channel = connection.channel(on_open_callback=on_open_channel)

        # Register a function to handle the closure of the channel
        self._channel.add_on_close_callback(callback=on_close_channel)

    def connect(self, on_connect_callback=None):
        """Connect to RabbitMQ.

        :param on_connect_callback: This function will be called when a connection has been established
        :return bool: True when the connection has been established, False otherwise
        """

        def on_error_connection(connection, text):
            """This function is called on a connection error

            The lock that has been set on connection creation will be released.
            The connect function will be released and notice that no channel has been created.
            It will therefore return False to inform the caller that no connection could be established

            The disconnect method will be called to clean up the connection properties and stop the eventloop.

            :param connection: The connection object
            :param text: The error text
            :return: None
            """

            progress("Connection error:", text)
            self._lock.release()
            self.disconnect()

        def on_close_connection(connection, code, text):
            """Called when a connection is closed

            :param connection: The RabbitMQ connection object
            :param code: The error code
            :param text: The error text
            :return: None
            """

            progress("Connection closed:", code, text)
            self._connection = None

        def eventloop():
            """The RabbitMQ eventloop.

            An ioloop is started to listen for messages for the active subscriptions.

            :return: None
            """

            try:
                self._connection.ioloop.start()
            except Exception as e:
                traceback.print_exc(limit=-5)
                progress("Eventloop exception:", e)
            progress("Eventloop ended")
            self._eventloop_failed = True

        # A callback function can be specified that will be called when a connection is established
        self._on_connect_callback = on_connect_callback

        # Obtain a lock. The lock will be release on connection establishment or on failure
        self._lock = threading.Lock()
        self._lock.acquire()

        connection_params = _get_connection_params()

        # Create a connection object
        self._connection = pika.SelectConnection(
            parameters=connection_params,
            on_open_callback=self._on_open_connection,
            on_open_error_callback=on_error_connection,
            on_close_callback=on_close_connection)

        # Start the RabbitMQ eventloop
        self._eventloop = threading.Thread(target=eventloop, name="Eventloop")
        self._eventloop.start()

        # Wait for the lock to be released to be able to report success or failure
        self._lock.acquire()

        # Check whether a channel has been created, which means the connection has been established
        return self._channel is not None

    def publish(self, exchange, key, msg):
        """Publish a message on a queue

        The message will be converted to json before publishing it on the queue

        :param exchange: The exchange to publish to
        :param key: The routing key of the message
        :param msg: The message
        :return: None
        """
        # Check whether a connection has been established
        if self._channel is None:
            raise Exception("Connection with message broker not available")

        # Allow for offloaded contents
        msg = offload_message(msg, to_json)

        # Convert the message to json
        json_msg = to_json(msg)

        # Publish the message as a persistent message on the queue
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=key,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )

    def on_message(self, queue, message_handler):  # noqa: C901
        """This function is called for every message that is received

        The specified handler will be called to let the application handle the message.
        Default behaviour is to acknowledge the message after it has been successfully handled.
        If the handler returns False the message will not be acknowledged and stay on the queue

        :param queue: The queue that is consumed by this on_message
        :return: The handle message function
        """

        def handle_message(channel, basic_deliver, properties, body):
            """Handle the incoming message

            The message body is a json object which will be parsed on receipt

            Call the handler and acknowledge the message after it has been successfully handled

            :param channel: The channel that represents the connection with RabbitMQ
            :param basic_deliver: The deliver properties, e.g. is redelivery, routing_key, ...
            :param properties: general message properties
            :param body: The message body (json dump)
            :return: None
            """

            def run_message_handler():
                offload_id = None
                result = None
                msg = None
                try:
                    # Try to get the message, parse any json contents and retrieve any offloaded contents
                    # Include any queue specific parameters
                    params = {
                        **self._params,
                        **self._params.get(queue, {})
                    }
                    msg, offload_id = get_message_from_body(body, params)
                    # Try to handle the message
                    result = message_handler(self, basic_deliver.exchange, queue, basic_deliver.routing_key, msg)
                except Exception as e:
                    # Print error message, the message that caused the error and a short stacktrace
                    stacktrace = traceback.format_exc(limit=-10)
                    print(f"Message handling has failed: {str(e)}, message: {str(body)}", stacktrace)
                    if basic_deliver.redelivered:
                        # When a message is redelivered then remove the message from the queue
                        print("Message handling has failed on second try, removing message")
                    else:
                        # Fatal fail program on first try
                        print("Message handling has failed, terminating program")
                        os._exit(os.EX_TEMPFAIL)

                if msg is not None:
                    # run fail-safe method to end the message
                    end_message(msg, offload_id)

                if result is not False:
                    # Default is to acknowledge message
                    # Only on an explicit return value of False the message keeps unacked.
                    channel.basic_ack(basic_deliver.delivery_tag)

            if self._message_handler_thread is not None:
                # Wait for any not yet terminated thread
                self._message_handler_thread.join()
            # Start a new thread to handle the message
            self._message_handler_thread = threading.Thread(target=run_message_handler, name="MessageHandler")
            self._message_handler_thread.start()

        return handle_message

    def subscribe(self, queues, message_handler):
        """Subscribe to the given queues

        :param queues: The queues to subscribe on
        :return: []
        """

        for queue in queues:
            self._channel.basic_consume(
                consumer_callback=self.on_message(queue, message_handler),
                queue=queue
            )
        return []

    def disconnect(self):
        """Disconnect from RabbitMQ

        Close any open channels
        Close the connection
        Stop any running eventloop

        :return: None
        """

        progress("Disconnect")

        # Close any open channel
        if self._channel is not None and self._channel.is_open:
            progress("Close channel")
            # Cancel consumers
            for consumer_tag in self._channel.consumer_tags:
                self._channel.basic_cancel(
                    callback=lambda frame: None,
                    consumer_tag=consumer_tag)
            self._channel.close()
        self._channel = None

        # Close any open connection
        if self._connection is not None:
            progress("Close connection")
            self._connection.close()
            # Wait for eventloop to finish on closing of the connection
            if self._eventloop is not None and threading.get_ident() != self._eventloop.ident:
                self._eventloop.join()
                self._eventloop = None
        self._connection = None


rabbitmq_connectors = (RabbitMQBrokerManager, RabbitMQConnection, RabbitMQAsyncConnection)

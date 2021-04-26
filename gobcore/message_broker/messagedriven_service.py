import sys
import time

from gobcore.status.heartbeat import Heartbeat, HEARTBEAT_INTERVAL, STATUS_OK, STATUS_START, STATUS_FAIL
from gobcore.message_broker.initialise_queues import initialize_message_broker
from gobcore.message_broker.notifications import contains_notification, send_notification
from gobcore.quality.issue import process_issues
from gobcore.message_broker.utils import StoppableThread

from gobcore.message_broker.brokers.broker import msg_broker

CHECK_CONNECTION = 5                # Check connection every n seconds
RUNS_IN_OWN_THREAD = "own_thread"   # Service that runs in a separate thread

# Assure that heartbeats are sent at every HEARTBEAT_INTERVAL
assert(HEARTBEAT_INTERVAL % CHECK_CONNECTION == 0)


def progress(*args):
    # print(f'[{threading.get_ident()}]', *args)
    pass


def _on_message(connection, service, msg):
    """Called on every message receipt

    :param connection: the connection with the message broker
    :param service: the service definition for the message
    :param msg: the contents of the message

    :return:
    """
    handler = service['handler']
    result_msg = None
    try:
        Heartbeat.progress(connection, service, msg, STATUS_START)
        result_msg = handler(msg)
        Heartbeat.progress(connection, service, msg, STATUS_OK)
    except Exception as err:
        Heartbeat.progress(connection, service, msg, STATUS_FAIL, str(err))
        # re-raise the exception, further handling is done in the message broker
        raise err

    if result_msg:
        process_issues(result_msg)

        if contains_notification(result_msg):
            send_notification(result_msg)

        # If a report_queue is defined, report the result message
        if 'report' in service:
            report = service['report']
            connection.publish(report['exchange'], report['key'], result_msg)

    # Remove the message from the queue by returning true
    return True


class MessagedrivenService:
    """Start a connection with a the message broker and the given definition

    servicedefenition is a dict of dicts:

    ```
    SERVICEDEFINITION = {
        'unique_key': {
            'exchange': 'name_of_the_exchange_to_listen_to',
            'queue': 'name_of_the_queue_to_listen_to',
            'key': 'name_of_the_key_to_listen_to'
            'handler': 'method_to_invoke_on_message',
             # optional report functionality
            'report': {
                'exchange': 'name_of_the_exchange_to_report_to',
                'queue': 'name_of_the_queue_to_report_to',
                'key': 'name_of_the_key_to_report_to'
            }
        }
    }
    ```

    start the service with:

    ```
    from gobcore.message_broker.messagedriven_service import MessagedrivenService

    MessagedrivenService(SERVICEDEFINITION).start()

    """
    def __init__(self, services: dict, name: str, params=None):
        self.services = services
        self.name = name
        params = params or {}
        self._params = params
        self.thread_per_service = params.get('thread_per_service', False)
        self.threads = []
        self.keep_running = True  # This variable is used for testing only
        self.check_connection = CHECK_CONNECTION
        self.heartbeat_interval = HEARTBEAT_INTERVAL

        self._init()

    def _init(self):
        """Initializes the message broker

        This method is idempotent. If the message broker has already been initialised it will be noticed and
        the initialisation becomes a noop

        :return:
        """
        try:
            initialize_message_broker()
        except Exception as e:
            print(f"Error: Failed to initialize message broker, {str(e)}")
            sys.exit(1)

        print("Succesfully initialized message broker")

        # Call all queue functions that need setting up after the message broker is initialized
        # For example, in a SERVICEDEFINITION, you'll find 'queue': lambda: listen_to_notifications(....)
        for service_id, service in self.services.items():
            if callable(service['queue']):
                service['queue'] = service['queue']()

    def _start_threads(self, queues: list):
        for queue in queues:
            self._start_thread([queue])

    def _start_thread(self, queues):
        thread = StoppableThread(target=self._listen, args=(queues,), name="QueueHandler")
        thread.start()

        self.threads.append({
            'thread': thread,
            'queues': queues,
        })

        return thread

    def _on_message(self, connection, exchange, queue, key, msg):
        """Called on every message receipt

        :param connection: the connection with the message broker
        :param exchange: the message broker exchange
        :param queue: the message broker queue
        :param key: the identification of the message (e.g. fullimport.proposal)
        :param msg: the contents of the message

        :return:
        """
        print(f"{key} accepted from {queue}, start handling")
        service = self._get_service(queue)

        return _on_message(connection, service, msg)

    def _listen(self, stop_check, queues: list):
        with msg_broker.async_connection(self._params) as connection:
            # Subscribe to the queues, handle messages in the on_message function (runs in another thread)
            queue_threads = connection.subscribe(queues, self._on_message)

            # id = ', '.join([queue for queue in queues])

            # Repeat forever
            while connection.is_alive() and not stop_check():
                time.sleep(self.check_connection)

            [q.stop() for q in queue_threads]
            # import threading
            # print(f"[{threading.get_ident()}] Queue connection for {id} stopped.")

    def start(self):
        asynchronous_queues = [service['queue'] for service in self.services.values()
                               if self.thread_per_service or service.get(RUNS_IN_OWN_THREAD)]
        synchronous_queues = [service['queue'] for service in self.services.values()
                              if not service['queue'] in asynchronous_queues]

        if asynchronous_queues:
            self._start_threads(asynchronous_queues)

        if synchronous_queues:
            self._start_thread(synchronous_queues)

        progress('Starting heartbeat loop')

        self._heartbeat_loop()

        progress('Should nicelly close other threads')

    def _heartbeat_loop(self):  # noqa: C901
        with msg_broker.async_connection(self._params) as connection:
            heartbeat = Heartbeat(connection, self.name)

            n = 0
            try:
                while self.keep_running and connection.is_alive():
                    time.sleep(self.check_connection)

                    for thread in self.threads:
                        if not thread['thread'].is_alive():
                            print(f"ERROR: died thread found")
                            # Create new thread
                            thread['thread'] = self._start_thread(thread['queues'])

                    n += self.check_connection

                    if n >= self.heartbeat_interval and all([t['thread'].is_alive() for t in self.threads]):
                        heartbeat.send()
                        n = 0
            except KeyboardInterrupt:
                progress('Keyboard int....')
                # Now cleanup the threads if alive
                [t['thread'].stop() for t in self.threads if t['thread'].is_alive()]
                progress('Keyboard int cleanup done ....')
                heartbeat.terminate()

    def _get_service(self, queue):
        """Gets the service for the specified queue

        :param queue:
        :return:
        """
        return next(s for s in self.services.values() if s["queue"] == queue)


def messagedriven_service(services, name, params=None):
    # For backwards compatibility
    MessagedrivenService(services, name, params).start()

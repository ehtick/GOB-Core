from abc import ABC, abstractmethod


class BrokerManager(ABC):

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *args):
        pass

    @abstractmethod
    def create_exchange(self, exchange):
        pass

    @abstractmethod
    def create_queue_with_binding(self, exchange, queue, keys):
        pass

    @abstractmethod
    def create_all(self):
        '''
           Create all the queues defined in QUEUE_CONFIGURATION
        '''
        pass

    @abstractmethod
    def destroy_all(self):
        pass


class Connection(ABC):

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *args):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def publish(self, exchange: str, key: str, msg: str, ttl_msec: int):
        pass

    @abstractmethod
    def publish_delayed(self, exchange: str, key: str, queue: str, msg: dict, delay_msec: int=0):
        pass

    @abstractmethod
    def disconnect(self):
        pass


class AsyncConnection(ABC):

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, *args):
        pass

    @abstractmethod
    def is_alive(self):
        pass

    @abstractmethod
    def connect(self, on_connect_callback=None):
        pass

    @abstractmethod
    def publish(self, exchange: str, key: str, msg: dict):
        pass

    @abstractmethod
    def subscribe(self, queues, message_handler):
        '''
         This is really the only async method.
         It will either start a thread or add the message handler
         to the poll/select hanlder.
        '''
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def receive_msgs(self, queue, max_wait_time, max_message_count):
        pass

    @abstractmethod
    def receive_msg(self, queue):
        pass

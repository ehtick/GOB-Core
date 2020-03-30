import pika

from gobcore.message_broker.config import CONNECTION_PARAMS
from gobcore.message_broker.utils import to_json
from gobcore.message_broker.initialise_queues import _create_exchange, _create_queue, _bind_queue

# Use a dedicated exchange for notifications
NOTIFY_EXCHANGE = "gob.notify"
# Each queue in the notification exchange starts with 'gob.notify'
NOTIFY_BASE_QUEUE = 'gob.notify'
# Notifications can be included in a message with the 'notification' key
NOTIFICATION_KEY = 'notification'
# Notification messages copy a selection of the original header fields
NOTIFICATION_HEADER_FIELDS = [
    'process_id', 'jobid', 'stepid', 'source', 'catalogue', 'collection', 'entity', 'version']


def listen_to_notifications(id):
    """
    Listen to notification messages
    The id is used to create a queue on which notifications will be received for the id

    :param id:
    :return:
    """
    queue = f"{NOTIFY_BASE_QUEUE}.{id}"
    return listen_to_broadcasts(NOTIFY_EXCHANGE, queue)


def contains_notifications(result_msg):
    """
    Tells wether the message includes any notifications

    :param result_msg:
    :return:
    """
    return result_msg and result_msg.get(NOTIFICATION_KEY)


def send_notifications(result_msg):
    """
    Send any notifications that are contained in the result msg

    :param result_msg:
    :return:
    """
    # If a notification has been specified then broadcast the notification
    if contains_notifications(result_msg):
        header_fields = NOTIFICATION_HEADER_FIELDS
        notification = {
            'header': {key: result_msg['header'].get(key) for key in header_fields},
            **result_msg[NOTIFICATION_KEY]
        }
        # Broadcast notification
        send_broadcast(NOTIFY_EXCHANGE, msg=notification)
        # Delete when handled
        del result_msg[NOTIFICATION_KEY]


def add_notification(msg, notification):
    """
    Include a notification in the message

    :param msg:
    :param notification:
    :return:
    """
    msg[NOTIFICATION_KEY] = {
        'type': notification.type,
        'contents': notification.contents
    }


def get_notification(msg):
    """
    Returns an Event object for the given message

    :param msg:
    :return:
    """
    if msg.get('type') == EventNotification.type:
        return EventNotification.from_msg(msg)


class EventNotification():

    type = "events"

    def __init__(self, applied, last_event, header=None):
        """
        Initialize an Event Notification

        :param applied: What kind of events have been applied and how many
        :param last_event: The last eventid after the events have been applied
        :param header:
        """
        self.contents = {
            'applied': applied,
            'last_event': last_event
        }
        self.header = header

    @classmethod
    def from_msg(cls, msg):
        """
        Construct an Event Notification object from a message

        :param msg:
        :return:
        """
        return cls(**msg['contents'], header=msg['header'])


def _create_broadcast_exchange(channel, exchange):
    """
    Create the broadcast exchange if it does not yet exist

    :param channel:
    :param exchange:
    :return:
    """
    _create_exchange(channel, exchange=exchange, durable=True, exchange_type='fanout')


def send_broadcast(exchange, msg):
    """
    Send a broadcast message on the specified exchange

    :param exchange:
    :param msg:
    :return:
    """
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()

        _create_broadcast_exchange(channel, exchange)

        # Convert the message to json
        json_msg = to_json(msg)

        # Broadcast the message as a non-persistent message on the queue
        channel.basic_publish(
            exchange=exchange,
            routing_key='',
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )


def listen_to_broadcasts(exchange, queue):
    """
    Listen to broadcast messages on the specified exchange and queue

    :param exchange:
    :param queue:
    :return:
    """
    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()

        _create_broadcast_exchange(channel, exchange)
        _create_queue(channel=channel, queue=queue, durable=True)
        _bind_queue(channel=channel, exchange=exchange, queue=queue, key='')
    return queue

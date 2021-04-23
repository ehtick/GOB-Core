from gobcore.message_broker.brokers.broker import get_connection
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, WORKFLOW_QUEUE, WORKFLOW_REQUEST_KEY


def start_workflow(workflow, arguments):
    """
    Start a workflow for the given specs

    Puts a message on the bus to request execution of the give workflow

    :param workflow_spec:
    :return:
    """
    # Store all arguments in the header if no header specified
    header = arguments.get('header', arguments)
    contents = arguments.get('contents', {})
    contents_ref = arguments.get('contents_ref', None)

    msg = {
        'header': header,
        'contents': contents,
        'workflow': workflow
    }

    if contents_ref:
        msg['contents_ref'] = contents_ref

    with get_connection() as conn:
        # Publish a workflow-request message on the workflow-exchange for the given workflow
        conn.publish(
            exchange=WORKFLOW_EXCHANGE,
            key=WORKFLOW_REQUEST_KEY,
            msg=msg,
        )


SINGLE_RETRY_TIME = 1 * 60  # Retry every 1 minute
RETRY_TIME_KEY = 'retry_time'


def retry_workflow(msg):
    """
    Retry to start the given workflow msg

    The message is sent to a queue that delays the message for a specified interval
    The message is resent until the remaining retry time that is specified in the message is less or equal to zero

    :param msg: workflow message
    :return:
    """
    workflow = msg['workflow']

    # Check if any retry time is left
    remaining_retry_time = workflow.get(RETRY_TIME_KEY, 0)
    if remaining_retry_time <= 0:
        # No time left, do not send the workflow message
        return False

    # reduce remaining retry time
    remaining_retry_time -= SINGLE_RETRY_TIME
    workflow[RETRY_TIME_KEY] = remaining_retry_time

    # Log retry
    print(f"Retry workflow, {remaining_retry_time} time left", workflow)

    with get_connection() as conn:
        delay_ms = SINGLE_RETRY_TIME * 1000
        conn.publish_delayed(
            exchange=WORKFLOW_EXCHANGE,
            key=WORKFLOW_REQUEST_KEY,
            queue=WORKFLOW_QUEUE,
            msg=msg,
            delay_msec=delay_ms)
    return True

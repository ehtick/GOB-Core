import json
import pika

from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY


def start_workflow(workflow, arguments):
    """
    Start a workflow for the given specs

    Puts a message on the bus to request execution of the give workflow

    :param workflow_spec:
    :return:
    """

    # Simple version, store all arguments in the header
    msg = {
        'header': arguments,
        'workflow': workflow
    }

    with pika.BlockingConnection(CONNECTION_PARAMS) as connection:
        channel = connection.channel()

        # Convert the message to json
        json_msg = json.dumps(msg)

        # Publish a workflow-request message on the workflow-exchange for the given workflow
        channel.basic_publish(
            exchange=WORKFLOW_EXCHANGE,
            routing_key=WORKFLOW_REQUEST_KEY,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make messages persistent
            ),
            body=json_msg
        )

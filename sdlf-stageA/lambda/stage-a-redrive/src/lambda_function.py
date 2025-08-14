import os

from datalake_library import DataLakeClient
from datalake_library.commons import init_logger

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        client = DataLakeClient(team=os.environ["TEAM"], pipeline=os.environ["PIPELINE"], stage=os.environ["STAGE"])

        messages = client.sqs.receive_messages(1, client.sqs.stage_dlq_url)
        if not messages:
            logger.info("No messages found in DLQ")
            return

        logger.info("Received {} messages".format(len(messages)))
        for message in messages:
            client.sqs.send_message_to_fifo_queue(message["Body"], "redrive", client.sqs.stage_queue_url)
            logger.info("Redrive message succeeded")
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return

import json

from datalake_library import DataLakeClient
from datalake_library.commons import init_logger

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        if isinstance(event, str):
            event = json.loads(event)

        client = DataLakeClient(team=event["team"], pipeline=event["pipeline"], stage=event["pipeline_stage"])

        logger.info("Execution Failed. Sending original payload to DLQ")
        client.sqs.send_message_to_fifo_queue(json.dumps(event), "failed", client.sqs.stage_dlq_url)
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

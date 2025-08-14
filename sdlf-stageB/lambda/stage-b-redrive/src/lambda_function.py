import json
import os

from datalake_library import DataLakeClient
from datalake_library.commons import init_logger

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        team = os.environ["TEAM"]
        pipeline = os.environ["PIPELINE"]
        stage = os.environ["STAGE"]

        client = DataLakeClient(team=team, pipeline=pipeline, stage=stage)

        messages = client.sqs.receive_messages(1, client.sqs.stage_dlq_url)
        if not messages:
            logger.info("No messages found in DLQ")
            return

        logger.info("Received {} messages".format(len(messages)))
        for message in messages:
            logger.info("Starting State Machine Execution")
            if isinstance(message["Body"], str):
                response = json.loads(message["Body"])
            client.states.run_state_machine(client.states.state_machine_arn, response)
            logger.info("Redrive message succeeded")
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return

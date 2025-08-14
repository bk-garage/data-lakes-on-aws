import json
import os

from datalake_library import DataLakeClient
from datalake_library.commons import init_logger

logger = init_logger(__name__)


def fetch_messages(team, pipeline, stage):
    client = DataLakeClient(team=team, pipeline=pipeline, stage=stage)
    # Default values, change if required
    min_items_to_process = 1
    max_items_to_process = 100

    logger.info(f"Pipeline is {pipeline}, stage is {stage}")
    logger.info("Querying {}-{}-{} objects waiting for processing".format(team, pipeline, stage))

    keys_to_process = client.sqs.receive_min_max_messages(
        min_items_to_process, max_items_to_process, client.sqs.stage_queue_url
    )

    logger.info("{} Objects ready for processing".format(len(keys_to_process)))
    return list(set(keys_to_process))


def lambda_handler(event, context):
    """Checks if any items need processing and triggers state machine
    Arguments:
        event {dict} -- Dictionary with details on what needs processing
        context {dict} -- Dictionary with details on Lambda context
    """
    try:
        keys_to_process = []
        trigger_type = event.get("trigger_type")  # this is set by the schedule event rule
        if trigger_type:  # scheduled
            records = fetch_messages(event["team"], event["pipeline"], event["pipeline_stage"])
        else:
            records = event["Records"]
        logger.info("Received {} messages".format(len(records)))
        response = {}
        for record in records:
            if trigger_type:
                event_body = json.loads(json.loads(record)["output"])[0]
            else:
                event_body = json.loads(json.loads(record["body"])["output"])[0]

            team = event_body["team"]
            pipeline = event_body["pipeline"]
            stage = os.environ["PIPELINE_STAGE"]
            dataset = event_body["dataset"]
            org = event_body["org"]
            domain = event_body["domain"]
            env = event_body["env"]

            client = DataLakeClient(team=team, pipeline=pipeline, stage=stage)
            stage_bucket = client.s3.stage_bucket
            keys_to_process.extend(event_body["processedKeys"])

            logger.info("{} Objects ready for processing".format(len(keys_to_process)))
            keys_to_process = list(set(keys_to_process))

            response = {
                "statusCode": 200,
                "body": {
                    "bucket": stage_bucket,
                    "keysToProcess": keys_to_process,
                    "team": team,
                    "pipeline": pipeline,
                    "pipeline_stage": stage,
                    "dataset": dataset,
                    "org": org,
                    "domain": domain,
                    "env": env,
                },
            }
        if response:
            logger.info("Starting State Machine Execution")
            client.states.run_state_machine(client.states.state_machine_arn, response)
    except Exception as e:
        # If failure send to DLQ
        if keys_to_process:
            client = DataLakeClient(team=team, pipeline=pipeline, stage=stage)
            client.sqs.send_message_to_fifo_queue(json.dumps(response), "failed", client.sqs.stage_dlq_url)
        logger.error("Fatal error", exc_info=True)
        raise e

import json
from pathlib import PurePath

from datalake_library import DataLakeClient
from datalake_library.commons import init_logger

logger = init_logger(__name__)


def transform_object(bucket, key, team, dataset):
    # Initialize data lake client with team/dataset/stage parameters
    client = DataLakeClient(team=team, dataset=dataset, stage="a")

    # IMPORTANT: Stage bucket where transformed data must be uploaded
    stage_bucket = client.s3.stage_bucket
    # Download S3 object locally to /tmp directory
    local_path = client.s3.download_object(bucket, key)

    # Apply business logic:
    # Below example is opening a JSON file and
    # extracting fields, then saving the file
    # locally and re-uploading to Stage bucket
    def parse(json_data):
        l = []  # noqa: E741
        for d in json_data:
            o = d.copy()
            for k in d:
                if type(d[k]) in [dict, list]:
                    o.pop(k)
            l.append(o)
        return l

    # Reading file locally
    with open(local_path, "r") as raw_file:
        data = raw_file.read()

    json_data = json.loads(data)

    # Saving file locally to /tmp after parsing
    output_path = f"{PurePath(local_path).with_suffix('')}_parsed.json"
    with open(output_path, "w", encoding="utf-8") as write_file:
        json.dump(parse(json_data), write_file, ensure_ascii=False, indent=4)

    # Uploading file to Stage bucket at appropriate path
    # IMPORTANT: Build the output s3_path without the s3://stage-bucket/
    s3_path = f"pre-stage/{team}/{dataset}/{PurePath(output_path).name}"
    # IMPORTANT: Notice "stage_bucket" not "bucket"
    # you can select kms_key = client.kms.data_kms_key => to use the datalake domain data key
    # or use the particular team kms_key = client.kms.team_data_kms_key
    client.s3.upload_object(output_path, stage_bucket, s3_path, kms_key=client.kms.team_data_kms_key)
    # IMPORTANT S3 path(s) must be stored in a list
    processed_keys = [s3_path]

    #######################################################
    # IMPORTANT
    # This function must return a Python list
    # of transformed S3 paths. Example:
    # ['pre-stage/engineering/legislators/persons_parsed.json']
    #######################################################

    return processed_keys


def lambda_handler(event, context):
    """Calls custom transform developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket and Key(s)
    """
    try:
        logger.info("Fetching event data from previous step")
        bucket = event["bucket"]
        key = event["key"]
        team = event["team"]
        dataset = event["dataset"]

        logger.info("Calling user custom processing code")
        event["processedKeys"] = transform_object(bucket, key, team, dataset)
        logger.info("Successfully processed object")

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

    return event

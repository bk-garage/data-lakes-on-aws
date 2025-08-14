from datalake_library.commons import init_logger

logger = init_logger(__name__)


def get_glue_transform_details(bucket, team, dataset, pipeline, stage):
    # Default Glue job configuration
    job_name = f"sdlf-{team}-{dataset}-glue-job"  # Name of the Glue Job
    glue_capacity = {"WorkerType": "G.1X", "NumberOfWorkers": 10}
    wait_time = 60
    glue_arguments = {
        # Specify any arguments needed based on bucket and keys (e.g. input/output S3 locations)
        "--SOURCE_LOCATION": f"s3://{bucket}/pre-stage/{team}/{dataset}",
        "--OUTPUT_LOCATION": f"s3://{bucket}/post-stage/{team}/{dataset}",
        "--job-bookmark-option": "job-bookmark-enable",
    }

    logger.info(f"Pipeline is {pipeline}, stage is {stage}")
    logger.info(f"Using default Glue job configuration: {job_name}")

    return {"job_name": job_name, "wait_time": wait_time, "arguments": glue_arguments, **glue_capacity}


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
        bucket = event["body"]["bucket"]
        team = event["body"]["team"]
        pipeline = event["body"]["pipeline"]
        stage = event["body"]["pipeline_stage"]
        dataset = event["body"]["dataset"]

        # Call custom transform created by user and process the file
        logger.info("Calling user custom processing code")
        event["body"]["glue"] = get_glue_transform_details(bucket, team, dataset, pipeline, stage)
        event["body"]["glue"]["crawler_name"] = "-".join(["sdlf", team, dataset, "post-stage-crawler"])

        logger.info("Successfully prepared Glue job configuration")
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return event

import boto3

from .interfaces import DynamoInterface, KMSInterface, S3Interface, SQSInterface, StatesInterface


class DataLakeClient:
    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        """
        Unified client for all data lake operations with shared boto3 session
        """
        # Shared session across all interfaces
        self.session = session or boto3.Session()

        # Initialize all interfaces with shared session
        self.s3 = S3Interface(team, dataset, pipeline, stage, log_level, self.session)
        self.dynamo = DynamoInterface(team, dataset, pipeline, stage, log_level, self.session)
        self.states = StatesInterface(team, dataset, pipeline, stage, log_level, self.session)
        self.sqs = SQSInterface(team, dataset, pipeline, stage, log_level, self.session)
        self.kms = KMSInterface(team, dataset, pipeline, stage, log_level, self.session)

import datetime as dt

from boto3.dynamodb.types import TypeSerializer

from ..commons import serialize_dynamodb_item
from .base_interface import BaseInterface


class DynamoInterface(BaseInterface):
    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        super().__init__(team, dataset, pipeline, stage, log_level, session)

    def _initialize_client(self):
        # DynamoDB specific client
        self.dynamodb = self.session.client("dynamodb", config=self.session_config)

    def _load_config(self):
        """Load DynamoDB-specific configuration from SSM"""
        self.object_metadata_table = self._get_ssm_parameter("/SDLF2/Dynamo/ObjectCatalog")
        self.manifests_table = self._get_ssm_parameter("/SDLF2/Dynamo/Manifests")

    @staticmethod
    def build_id(bucket, key):
        return f"s3://{bucket}/{key}"

    def put_item(self, table, item):
        serializer = TypeSerializer()
        self.dynamodb.put_item(TableName=table, Item=serialize_dynamodb_item(item, serializer))

    def update_object_metadata_catalog(self, item):
        item["id"] = self.build_id(item["bucket"], item["key"])
        item["timestamp"] = int(round(dt.datetime.now(dt.UTC).timestamp() * 1000, 0))
        return self.put_item(self.object_metadata_table, item)

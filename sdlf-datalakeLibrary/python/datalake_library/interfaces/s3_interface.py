import os
import shutil
from urllib.parse import unquote_plus

from .base_interface import BaseInterface


class S3Interface(BaseInterface):
    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        super().__init__(team, dataset, pipeline, stage, log_level, session)

    def _initialize_client(self):
        # S3 specific client
        self.s3 = self.session.client("s3", config=self.session_config)

    def _load_config(self):
        """Load S3-specific configuration from SSM"""
        self.raw_bucket = self._get_ssm_parameter("/SDLF2/S3/RawBucket")
        self.stage_bucket = self._get_ssm_parameter("/SDLF2/S3/StageBucket")
        self.analytics_bucket = self._get_ssm_parameter("/SDLF2/S3/AnalyticsBucket").split(":")[-1]
        self.artifacts_bucket = self._get_ssm_parameter("/SDLF2/S3/ArtifactsBucket")

    def download_object(self, bucket, key):
        dir_path = f"/tmp/{bucket}/"
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path, ignore_errors=True)
        os.makedirs(dir_path)

        object_path = dir_path + key.split("/")[-1]
        key = unquote_plus(key)
        self.s3.download_file(bucket, key, object_path)
        return object_path

    def upload_object(self, object_path, bucket, key, kms_key=None):
        extra_kwargs = {}
        if kms_key:
            extra_kwargs = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key}
        self.s3.upload_file(object_path, bucket, key, ExtraArgs=extra_kwargs)

    def copy_object(self, source_bucket, source_key, dest_bucket, dest_key=None, kms_key=None):
        source_key = unquote_plus(source_key)
        dest_key = dest_key or source_key
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        extra_kwargs = {}
        if kms_key:
            extra_kwargs = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key}
        self.s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key, **extra_kwargs)

    def get_size_and_last_modified(self, bucket, key):
        object_metadata = self.s3.head_object(Bucket=bucket, Key=key)
        return (object_metadata["ContentLength"], object_metadata["LastModified"].isoformat())

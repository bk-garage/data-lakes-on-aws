import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from ..commons import init_logger


class BaseInterface:
    """Simplified base interface for AWS service interactions"""

    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        # Simple properties - no config wrapper needed
        self.team = team
        self.dataset = dataset
        self.pipeline = pipeline
        self.stage = stage
        self.log_level = log_level or os.getenv("LOG_LEVEL", "INFO")

        # Shared session and logger
        self.session = session or boto3.Session()
        self.logger = init_logger(__name__, self.log_level)

        # Common session config
        self.session_config = Config(user_agent="awssdlf/2.11.0")

        # SSM client for parameter reads (belongs in interface layer)
        self.ssm = self.session.client("ssm", config=self.session_config)

        # Initialize service-specific clients and config
        self._initialize_client()
        self._load_config()

    def _initialize_client(self):
        """Override in subclasses to initialize service-specific boto3 clients"""
        pass

    def _load_config(self):
        """Override in subclasses to load service-specific configuration from SSM"""
        pass

    def _get_ssm_parameter(self, parameter_name):
        """Get SSM parameter value - interface responsibility, not config"""
        try:
            response = self.ssm.get_parameter(Name=parameter_name)
            return response["Parameter"]["Value"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                self.logger.error("SSM RATE LIMIT REACHED")
            else:
                self.logger.error(f"Error getting SSM parameter {parameter_name}: {e}")
            raise

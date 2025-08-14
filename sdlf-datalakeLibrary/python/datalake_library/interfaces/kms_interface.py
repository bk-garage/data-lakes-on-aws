from .base_interface import BaseInterface


class KMSInterface(BaseInterface):
    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        super().__init__(team, dataset, pipeline, stage, log_level, session)

    def _initialize_client(self):
        """Initialize KMS client"""
        self.kms = self.session.client("kms", config=self.session_config)

    def _load_config(self):
        """Load KMS-specific configuration from SSM"""
        self.data_kms_key = self._get_ssm_parameter("/SDLF2/KMS/KeyArn")
        if self.team:
            self.team_data_kms_key = self._get_ssm_parameter(f"/SDLF/KMS/{self.team}/DataKeyId")
            self.team_infra_kms_key = self._get_ssm_parameter(f"/SDLF/KMS/{self.team}/InfraKeyId")

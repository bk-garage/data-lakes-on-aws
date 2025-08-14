import json
from datetime import date, datetime

from .base_interface import BaseInterface


class StatesInterface(BaseInterface):
    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        super().__init__(team, dataset, pipeline, stage, log_level, session)

    def _initialize_client(self):
        """Initialize Step Functions client"""
        self.stepfunctions = self.session.client("stepfunctions", config=self.session_config)

    def _load_config(self):
        """Load Step Functions-specific configuration from SSM"""
        if self.team and self.stage and self.pipeline:
            self.state_machine_arn = self._get_ssm_parameter(f"/SDLF/SM/{self.team}/{self.pipeline}{self.stage}SM")

    @staticmethod
    def json_serial(obj):
        """JSON serializer for objects not serializable by default"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    def run_state_machine(self, machine_arn, message):
        return self.stepfunctions.start_execution(
            stateMachineArn=machine_arn, input=json.dumps(message, default=self.json_serial)
        )
